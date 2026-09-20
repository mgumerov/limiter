package consensus

import (
	"context"
	"errors"
	"fmt"
	"limiter/server"
	"log/slog"
	"sync/atomic"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcd "go.etcd.io/etcd/client/v3"
)

type EtcdConsensusTracker struct {
	lease atomic.Pointer[Lease]
}
var _ server.ConsensusTracker = (*EtcdConsensusTracker)(nil) //fail-fast type guard

//Here is where I will compromise. Problem is, etcd is not built for sub-second TTLs - even if I set a very brief
// leader election timeout of under 600s, it will only mean TTL=1 seconds. Meaning, if our cluster gets a split-brain right
// after lease starts, this instance will grant tickets for 1 more seconds. In this limiter service it's no problem because 
// we don't need to replicate to other instances; but real problem is that for the same time other instances will serve nothing,
// even if clients will notice we are not responding (died or non-reachable) and try to go to new leader instead.
//Having 10-seconds delay in service is not what we expect or want from 6M rps rate limiter :)
//Also, low TTLs mean the standard KeepAlive loop provided by etcd client might not have enough time to do its 
// keepalive+prolongation work (in a hidden loop), leading to losing of lease. Normally it sends keepalives each 1/3 TTL, 
// but it has a strict low limit of each 500 ms, which is only 1/2 of 1 second TTL (but still should be enough).
//Even if in the end I had to avoid using standard loop, it has its limit for a reason, and, counting also election timeout,
// we should not try to to go below 1 second. 
//Bottom line, I will try to request a 1-second TTL and no less, and actually probably get at least 2 or 3 seconds instead.
const LEASE_TIME_REQUEST = 1

//TODO Configurable
//TODO time.Minute would be better in the long run, but it seems that first attempts are suffering context deadline because initial client handshake is rather slow,
// so they will have to wait and that's inconvenient, especially for tests/benchmarks; progressive backoff might be a good idea.
const ETCD_BACKOFF = 1 * time.Minute

//TODO Configurable
//Timeout for single etcd operation. Should not make it too short because 
// 1) operations that are proposed to consensus (like KV transactions) require several round-trips
// 2) and anyway this timeout is only necessary to avoid waiting forever, not to fail-fast and quickly retry
//    (even if we set it to more than Lease TTL, and attempt to create a key times out, we just lose that race for leadership,
//     and some other instance gets it instead).
const ETCD_TIMEOUT = time.Duration(300) * time.Millisecond

//TODO Configurable.
//Same considerations apply as for ETCD_TIMEOUT: it's not meant to "fail fast, retry quickly". No sense bombarding etcd
// with repeating requests if for some reason it is not responding, it's better to just wait. Worst case, we wait for so long
// we lose the leadership (but while we still have it - we continue serving requests). Normally, maybe we wait longer than we expected
// but still get out response in time. Also, setting it too low is bad because etcd client is smart enough to internally retry, 
// so why not give it the opportunity.
//By the way, regarding the mentioned retries: etcd client config affects how often KeepAliveOnce retries, by default it's about 50ms;
// we should consider that configuration when picking diration for this one if we want a specific behavior, 
// like "give it enough time to let it do at least one retry".
const KEEP_ALIVE_ATTEMPT_TIMEOUT = time.Duration(300) * time.Millisecond

func StartMasterLeaseLoop(ctx context.Context, failed chan struct{}, cfg *server.Config) server.ConsensusTracker {
	if cfg.ETCDkey == "" {
		slog.Info("No etcd key configured. Unable to start consensus tracker.")
		slog.Warn("Running without distributed consensus configured! To be used only as a single instance.")
		return &AlwaysMasterConsensusTracker{}
	}

	tracker := EtcdConsensusTracker{}

	go func() {
		//New() does not do any initial connection synchronously, it is documents that it connects in the background, 
		// - thus, whatever connection error (as opposed to configuration errors) might happen as a consequence of New(), 
		// it might reflect on results of further operations we do with this client - but this particular call to New() will succeed. 
		// Meaning, we don't need to worry about making that call part of our normal retry policy for other operation 
		// (like, initial connection was not successful, we should try again) - `we attempt the call just once.
		cli, err := etcd.New(cfg.ETCD)
		if err != nil {
			slog.Error("Unable to bootstrap etcd client", "error", err) 
			failed <- struct{}{}
			return 
		}
		defer cli.Close()

		//Eventually I came to this approach. See end of file for (1) with explanation of alternatives
		//Loop invariant: this instance does not hold an active lease supporting this instance's claim of ownership (lease-guarded key in etcd)
		for ctx.Err() == nil {
			backoff := false

			//TODO use WithRequireLeader context maybe?
			offendingKey, err := tracker.tryCaptureAndHold(ctx, cli, cfg.ETCDkey)
			if err != nil {
				slog.Error("Etcd key capture failed", "Error", err)
				backoff = true
			} else {
				if offendingKey != nil {
					slog.Info("Another instance holds the key", "Key", offendingKey)
					err := waitForKeyDeletion(ctx, cli, offendingKey)
					if err != nil {
						slog.Error("Failed to wait for etcd key removal", "Error", err)
						backoff = true
					}
				} else {
					//We held the key for some time but now we don't;
					// no need to do anything about it, just try grabbing it again.
				}
			}
			if backoff {
				skip(ctx, ETCD_BACKOFF)
			}
		}
	}()

	return &tracker
}

func sleep(d time.Duration) {
	t := time.NewTicker(d)
	defer t.Stop()
	<- t.C
}

type Lease struct {
	ID      	etcd.LeaseID
	Requested  	time.Time		//Must contain monotonic clock. For example, time.Now() does.
	TTL			time.Duration
}

//Note that Go's time maintains monotonic clock (*) and thus helps handle time leaps. 
// It even uses monotonic clock in time comparisons (of course only so far as Equals/Before/etc predicates are concerned).
// Otherwise we might time-travel 1 hour back (say because of taking a flight somewhere)
// and happily continue serving requests for 1 hour after our 1-second lease has long expired.
// (*) Or rather it does IF the Time instance contains it.
//Also note that this should not be regarded as precise moment of expiration, but rather a close approximation from below.
// We don't get to know what moment ETCD counts this lease's TTL from, and even if we did - our clock might drift from theirs,
// but we can pick some moment which is both close to true expiration and still is before it.
func (l *Lease) Expires() time.Time {
	return l.Requested.Add(l.TTL)
}

//Note: there is no guarantee that the returned lease did not yet run out! If this function worked slowly enough and the lease TTL is very
// brief, the lease might have run out already before the function returns.
func acquireLease(ctx context.Context, cli *etcd.Client) (Lease, error) {
	rqTime := time.Now()
	//For some unfathomable reason, clientv3 developers added field Error to response, which does not exist in etcd grpc API,
	// and never explained what it is. Namely, should I use it as an additional error flag or it's just mirroring something?
	// On the other hand, they provided an example. I will adhere to it, but of course an example might not be a production grade approach.
	resp, err := cli.Grant(ctx,  LEASE_TIME_REQUEST)
	if err != nil {
		return Lease{}, fmt.Errorf("Failed to get new lease: %w", err)
	}
	//Note how we use time-of-request (as seen from our side) as starting point for TTL.
	// We cannot be sure what the true starting point is, but this pick at least is guaranteed to be earlier in time;
	// at the same time we can be reasonably sure it's close to the true mark.
	return Lease { 
		ID: resp.ID, 
		Requested: rqTime, //rqTime is Now() so it contains monotonic clock, as the Lease contract requires
		//This looks awkward but said to be most idiomatic way, even though it creates a transient Duration of X nanos despite X being measured in seconds.
		// But on a side note, it's justifiable: 
		// X seconds =  X * 1 Sec = X * (1 Nano) * 1 Sec / (1 Nano) = (X Nanos) * (exactly one of time constants "how many nanos in *")
		TTL: time.Duration(resp.TTL) * time.Second,
	}, nil
}

//If a key has been created, returns nil.
//If a key already existed, returns its metadata at the version at the moment of attempt - and nil error.
//If otherwise errored or interrupted, return error != nil.
func createKey(ctx context.Context, cli *etcd.Client, keyname string, lease Lease) (*mvccpb.KeyValue, error) {
	if lease.ID == 0 {
		return nil, fmt.Errorf("lease ID = 0 on input")
	}
	txnResp, err := cli.KV.Txn(ctx).
		//Beware of "version" vs "revision" (as in CreateRevision/ModRevision), the latter two are not reset if a key is deleted,
		// they keep incrementing monotonically when it's created again. Versioning, on the other hand, will restart.
		If(etcd.Compare(etcd.Version(keyname), "=", 0)). //"if the key does not exist" (its version is 0)
		Then(etcd.OpPut(keyname, "test" /*maybe lease ID? to compare. But we have one in bound lease and maybe can read it from there?*/, etcd.WithLease(lease.ID))).
		Else(etcd.OpGet(keyname, etcd.WithKeysOnly())). //return metadata about offending key; do not return its value
		Commit()
	if err != nil {
		return nil, err
	}
	var key *mvccpb.KeyValue = nil
	if !txnResp.Succeeded { //in this case I asked to fetch version, it's the only items in Responses
		key = txnResp.Responses[0].GetResponseRange().Kvs[0] //we only asked for 1 key
	}
	return key, nil
}

//Yes, even simple atomic reading still introduces some overhead over very fast processing, 
// so it means less performance than without consensus tracking.
// - but we consider that a small price for High Availability; also, we are still more performant than an actual network allows;
// - also, there will be much less writes than reads, so the overhead will be very low but still nonzero (even uncontended atomic read 
//   needs to sync with other caches, even though it does not wait for writes to complete and does not suffer from cache line invalidation) 
func (p *EtcdConsensusTracker) IAmMaster() bool {
	lease := p.lease.Load()
	if lease == nil {
		return false
	}
	return time.Now().Before(lease.Expires());
}

type AlwaysMasterConsensusTracker struct {
}
var _ server.ConsensusTracker = (*AlwaysMasterConsensusTracker)(nil) //fail-fast type guard

func (p *AlwaysMasterConsensusTracker) IAmMaster() bool {
	return true
}

//Blocks; does not return until it encountered a DELETE event for the key or has to stop
//- If returns because of DELETE, returns nil error
//- If returns because parentCtx cancelled or timed-out, also returns nil!
//- If returns because of another error returns the error
func waitForKeyDeletion(parentCtx context.Context, cli *etcd.Client, offendingKey *mvccpb.KeyValue) error {
	//TODO use WithRequireLeader maybe?
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	err := error(nil)
	for batch := range cli.Watch(ctx, string(offendingKey.Key),
		etcd.WithFilterPut() /* skip all PUTs, because we only want DELETEs */,
		etcd.WithRev(offendingKey.CreateRevision)) {
			if batch.Canceled { //In case of an error (NOT of context cancellation, mind you!)
				err = fmt.Errorf("Etcd watch error: %w", batch.Err())
				continue //although the contact says it's the last message anyway
			}
			for _, event := range batch.Events {
				//We asked to skip all PUTs, but generally speaking it's not the same as asking only for DELETEs;
				// currently there are no third option, but API can change - so let's not depend on it and re-check
				if event.Type != mvccpb.Event_DELETE {
					continue
				}

				//OK we received a DELETE, that's all we needed to know. If that particular claim on the key has ended, we will retry putting our claim on it.
				// (doesn't matter if someone else already grabbed the key again, we will still try, it won't hurt)
				// Similarly, there might be even more DELETE events waiting ahead in the event stream, if a key has been
				// recreated again while we waited. Unlikely, of course. Again, it does not matter, we will still retry capturing the key.
				// It also means we should not expect this to definitely be the end of the stream.
				
				//So now we just need to stop listening and continue the outer loop, but we should not just break this loop:
				// the manual asks us to immediately read everying that appears from the channel. 
				// So, we ask ectd to stop notifications, then we keep reading in case some more buffered events will appear.
				// Ultimately the channel will be closed from the other side and the loop will stop.
				cancel() //Ask to stop notifications (does not cancel parentCtx of course)
			}
		}
	return err
}

//Attempts to create an ownership-guarding key; if successful, keep extending its lease as long as possible.
//- If fails to create a key specifically because of someone holding it, returns immediately, 
//  returning the non-nil metadata of offending key at the moment of attempt, and nil error
//- If fails it for some other reason, returns err != nil.
//- If succeeded at creating a key, does not return until 1) fails to keep the lease alive, 
//  or 2) fatal ectd failure happens, or 3) context is cancelled; then one of these happens, returns nil, and nil error.
//This function expects safe publication of receiver, although is supposed to be called on other gorounites
func (p *EtcdConsensusTracker) tryCaptureAndHold(pctx context.Context, cli *etcd.Client, keyname string) (*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(pctx, ETCD_TIMEOUT)
	defer cancel()
	//I'd prefer not to give it any timeout because at this level of logic we don't really care how much time we can spend trying
	// to acquire a lease; however if for example we were expecting response from server and it just disappears, TCP will never recognize that
	// and never attempt to connect to some other server instead. Maybe GRPC will detect the problem, I am not sure, but not all communication
	// to etcd server is grpc.
	lease, err := acquireLease(ctx, cli) 
	if err != nil {
		return nil, fmt.Errorf("Cannot acquire a lease: %w", err)
	}

	//Normally when the current function returns it's because the lease is expired anyway, but there are cases when it's not:
	// - panic
	// - persistent ectd error
	// - graceful shutdown
	// - error/timed out while creating key (doesn't mean key was not created!)
	//Therefore, not to make others wait until its expiration despite us NOT servicing, we make sure we release the lease
	// (in case key has been created, it will delete it)
	defer func() {
//TODO check if works correctly if keepalive is still running at the time.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := cli.Lease.Revoke(cleanupCtx, lease.ID) 
		if err != nil {
			if errors.Is(err, rpctypes.ErrLeaseNotFound) {
				//Lease has actually expired, do nothing
			} else {
				slog.Error("Could not renounce the lease, it might still be locking other instances out!", "Error", err, "Lease", lease.ID)
			}
		}
	}()

	ctx, cancel = context.WithTimeout(pctx, ETCD_TIMEOUT) //Ignore parent context because we set a specific short time limit
	defer cancel()

	offendingKey, err := createKey(ctx, cli, keyname, lease)
	if err != nil {
		return nil, fmt.Errorf("Key creation failed: %w", err)
	}
	if offendingKey != nil {
		return offendingKey, nil
	}

	slog.Info("Ownership acquired")
	
	//Initial lease is also published by this call
	updates := keepLeaseAlive(pctx, cli, lease)
	for newLease := range updates {
	 	slog.Info("Extended lease", "Lease", newLease)
	 	p.lease.Store(&newLease)
	}

	slog.Info("Ownership lost")
	return nil, nil
}

//Starts a goroutine that publishes all updates to channel; initial lease is also published. 
//The gorotine stops when fails to continue because of error (failed to extend a lease in time, or faced a permanent error from ETCD,
// or ctx timed out or was cancelled), or notably because the operation succeeded at ectd but we do not know it (for example,
// we got a network error instead of etcd response, or ctx timed out);
// in that latter case it does not try to remove the lease or do anything else to recover from that indeterminate state,
// that's caller's responsibity if desired.
//
//Unfortunately it seems we cannot afford to just use the recommended client's KeepAlive() loop.
// With the loop we know about each lease extension only postfactum, so it's not possible to say
// "I requested extension at XX -> the lease extends sometime before XX + new_TTL" like we do when first requesting a lease.
//
//TODO Propose PR to make standard KeepAlive loop know about starting time of each attempt, shouldn't be difficult.
func keepLeaseAlive(ctx context.Context, cli *etcd.Client, lease Lease) <-chan Lease {
	channel := make(chan Lease, 1) // Try to be nonblocking

	go func(c chan<- Lease) {
		defer close(channel)

		minStep := time.Duration(500) * time.Millisecond // Same as in recommended KeepAlive loop

		//Set up initial invariant
		var newLease Lease = lease
		var err error
		var nextAttempt time.Time
		var step time.Duration
		for { //Loop invariant: on entry to each iteration we have results of the last attempt of keepalive
			if err == nil {
				lease = newLease
				nextAttempt = lease.Requested
				step = max((lease.TTL - KEEP_ALIVE_ATTEMPT_TIMEOUT) / 3, minStep)
				c <- lease
			} else { 
				//Keep in mind that some errs (say, connection-dropped) do not mean we could not actually succeed without knowing it,
				// so we should be ready for that outcome.	

				//This decision is trickier than it sounds.
				//We'd like not just to tell err "interrupted" from "actual error" -
				// we want to know "failing because no response in T milliseconds" (in which case we continue)
				// from "failing because parent context contained some deadline and it expired" (in which case we stop).
				//To do that we could make the call in each iteration to tell us the difference 
				// (that will require it tracking an independent timer instead of just adding a timeout to a context).
				//Or we could simply look at "ctx" here to see if it's Done - to rule out the second possibility.
				// Let's do the latter, the easy way.
				if cerr := ctx.Err(); cerr != nil { // WE are interrupted
					break // then just break and never mind what was the last "err"
				}

				//Now we know that WE are not interrupted. Now let's check if keepAliveAttempt suffered an interruption nevertheless.
				if _, ok := errors.AsType[*KeepAliveError](err); !ok { //then it was an interruption
					//Treat it as transient error (even if it's not): do not update the cached lease but do not break the loop.
					//And don't log it.
					//Also the keepalive might have actually succeeded!
					//So, if we just proceed, we must be ready for both cases. 
				} else { //some real error (or, a success observed as an error)
					slog.Error("Lease keepalive stopped because of error", "Error", err)
					//Even if it was a success but we do not recognize it, stop. This is per our contract.
					break
				}
			}

			//Because of what stated above, at this point a lease might have or have not been extended by a prior iteration
			// and we cannot reliably distinguish. 
			//But that really is no problem in this approach, because even if it had been extended, so what,
			// we will just extend that same lease yet again - it's not like its ID has been changed or something.
			//We only need to care if we exit the loop instead: in that case, the lease must be deleted if it still lives;
			// but we will not do it here.

			nextAttempt = nextAttempt.Add(step)
			if nextAttempt.After(lease.Expires()) {
				break
			}
			if !skip(ctx, time.Until(nextAttempt)) {
				break // failed to wait because context was cancelled
			}

			//Note: if we are unlucky, the waiting above might take much longer than requested and get us in the position when the lease has ended.
			// But this call must be ready for that anyway, because for example the request it makes might run too slowly,
			// or might face transient errors and retry, and in both those cases it might also make a request when a lease already expired.
			func() {
				// Set up new time-limited context with some little timeout - large enough to perform 1 or maybe more attempts,
				//  still little enough compared to lease TTLs (because that limit will be effectively substracted from TTL of extended lease
				//  when guesssing its expiration time)
				tctx, cancel := context.WithTimeout(ctx, KEEP_ALIVE_ATTEMPT_TIMEOUT)
				defer cancel()

				newLease, err = keepAliveAttempt(tctx, cli, lease)
			} ()
		}
	}(channel)

	return channel
}

func skip(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <- ctx.Done():
		return false
	case <- timer.C:
		return true
	}
}

//Attempts to invoke keep-alive for given lease within a small limited time-window.
//If returns error != nil, returns the same "lease" value. The error can be used to tell real errors from being interrupted.
// - If interrupted, returns ctx.Err (i.e. the error has identity of Cancelled or DeadlineExceeded)
// - If otherwise errors, returns the error translated to KeepAliveError (hiding its initial identity)
//It's possible that the call actually succeeded, of course, and we just failed to receive ack, or whatever.
func keepAliveAttempt(ctx context.Context, cli *etcd.Client, lease Lease) (Lease, error) {
	now := time.Now()
	//Note: this func may actually make multiple attempts (on errors it considers transient)
	resp, err := cli.KeepAliveOnce(ctx, lease.ID)
	if err != nil {
		//We need to tell if KeepAliveOnce failed because of a timeout/cancelled or some "real" error.
		// This might seem the wrong way to do it, but apparently it's not. 
		// For more details about other ways to do it I considered, see Additional Notes.(2) in this file
		// Also, we only need to know "interrupted or errored", we don't need specifically "at least <timeout> seconds has expired",
		// so we are OK with our timeout possibly shortened by joining an existing context.
		if cerr := ctx.Err(); cerr != nil {
			return lease, cerr //ctx.Err can either be Canceled or timeout.
		}
		//To avoid accidentally returning a propagated error which unwraps to Cancelled/DeadlineExceeded, 
		// we wrote in our contract that instead of normal error-wrapping we will translate the error 
		// (preserving the cause but do not allowing to unwrap it).
		return lease, &KeepAliveError { Message: "Failed to do etcd keepalive: " + err.Error(), Cause: err }
	}
	return Lease { 
		ID: lease.ID,
		Requested: now,
		TTL: time.Duration(resp.TTL) * time.Second,
	}, nil
}

//Do not implement Unwrap because we don't want errors.Is() see this error as its cause (in case the root cause is some internal DeadlineExceeded)
type KeepAliveError struct {
	Message string
	Cause error
}

//TODO test it
func (e *KeepAliveError) Error() string {
	if e.Cause == nil {
		return e.Message
	}
	return e.Message + ": " + e.Cause.Error()
}

// ETCD_UNSUPPORTED_ARCH="arm64" /opt/homebrew/opt/etcd/bin/etcd

//Additional notes:
//
// (1) Just that you are aware of troubles with different approaches...
// 		First I came up with the following loop:
// 		1) check if lease cashe holds a lease that has not yet expired
// 		2) - if it does, sleep until that particular lease expires (while sleeping, some newer lease might be placed in a cache)
// 		3) - if it does not, request new lease and attempt to create a lease-guarded key;
// 			failing that, back off - but not with sleep, use etcd Watch instead and wait for channel.
// 			(if we just sleep, and we want to avoid long delays between someone losing claim and we getting it,
// 			we must not sleep for long; TTL at most, and even that is too much because the claim might expire
// 			right after we last checked; And since we use short TTL, that means our instances will send frequent requests to etcd
// 			all the time even if they don't do a lease keepalive; Watch helps avoiding that AND shorten that unavailablility window.
//
//.     While logical, it suffers from a race. Go client encourages lease prolongation via the loop it controls, let's call it a keepalive loop;
//.         its actions can succeed or fail, and they will race with this loop, because this loop will  try to put claims on keys
//          while that loop will try to keep existing claims. Also, both my loop and their loop behave differently depending if the call
//.         was before or after expiration. Worse that that, our definition of "expired" is based on our estimation of expiration time, 
// 			and it might be a bit earlier than ectd's perspective of it (the source of truth), increasing chances of that happening.
//			Also there is matter of retries, especially in cases where because of an error we do not know if etcd DID perform the action or not. 
//			At least 32 cases might be possible (whether keepalive is executed before expiration, whether re-creation is executed before expiration, which of it happens fist,
//				and which of them succeeds, and which of them actually realize that they succeeded), making reasoning here a real nightmare.
//				Here is just one of the cases:
				// - keepalive executed before it expired, 
				//   re-creation happens a bit later,
				//   re-creation also happens before expiration.
				//   -> Then re-creation fails. If keepalive succeeds, the old lease holds; this loop backs off, deducing that someone ELSE
				//      has the lock, but it's no problem, on next iteration it will see it's actually this instance (if that does not change)
				// 	 If keepalive fails (even despite silent retries), it means that either it genuinely failed or it succeeded but fails to receive
				// 	 a response. If it genuinely failed, the lease expires, but since re-creation also fails, we just back off in this loop,
				// 	 whereas keepalive stops. On next iteration after back-off, we will see the lease as expired and retry claiming ownership.
				// 	 But if it actually succeeded, the same happens, we try to re-claim the key we still already own. And we will not
				// 	 realize it until next successful keepalive attempt will update our cached TTL. But that is not a big problem,
				// 	 just some more backoffs maybe and some unserved time (which we probably cannot avoid anyway if network is unhealthy between
				// 	 us and etcd). But it WILL be a problem, if this instance thinks it LOST ownership and everyone else thinks it DID NOT.
				// 	 Everything stops until the client stops retrying. Well, maybe it's not that bad, if we can limit the retry window
				// 	 to something narrow.
//
//		Of course I also thought about manually calling lease keepalive, but AI kept saying how subtly tricky it was to implement correctly.
//.    
// 		Then I abandoned the above appoach and thought about total switch to Watch API, making claim-loop event-driven, only laying claims
//		when we know someone's claim is expired, rather than deducing that from comparing expiration time. That might have helped, but still
//.     there would be a race.
//
//		Finally AI proposed a solution which is likely widely used, which uses STOP of keepalive loop as a trigger for doing
//		claim loop, and vice versa :) Which eliminates most races. That is the one I ended up implementing.
//
// (2) Regarding the error classification problem...
//
//		Initially I was going to make specific identity of returned error a part of the contract (by saying it returns DeadlineExceeded iff 
// 		its internal timeout was exceeded). If taking that road, we cannot just blindly propagate/wrap
// 		something received from KeepAliveOnce, like we normally would. KeepAliveOnce could actually have suffered DeadlineExceeded 
//  	from some other its own internal WithTimeout context, failed because of that, wrapped it into some other error and returned it to us; 
//  	if we just propagate that, we violate our contract because we must only return DeadlineExceeded under specific conditions.
//		Also (if we did also receive parent context) timeout could have been dictated by THAT context and not ours - meaning,
//		OUR context did NOT time out yet. (Yes yes in fact our timeout would be limited to rightmost point of parent's timeout,
//		still nominally our timeout we talk about in our contract - would not yet expire).
//		I could not find a way to correctly distinguish between those cases looking at an error returned from KeepAliveOnce.
//
//		Then someone one told me (I am summing it up) that actually it's Go convention that
//		- If a function docs say it uses a passed context to control lifecycle/timeouts/etc, and a function fails because that context
//		  cancellation/timeout, it returns its Context.Err() - BUT not everyone follows that convention
//		- If some other error occured inside of some SDK, it will not cross SDK border without being translated to some other SDK-specific
//		  error, meaning that "some internal context timed out -> wrap as some other error -> returned from SDK -> 
// 		  recognized as DeadlineExceeded" is impossible - BUT not everyone follows that one either.
//		So the bottom line of the advice was to:
//		- Expect that behavior unless docs explicitly say otherwise
//		- Unless guaranteed by docs, check implementation to make sure
//		Of course, I prefer not to rely on something not publicly part of contract, lest implementation changes later,
//		 but I have been told it's considered quite an accepted practice in Go. I think it will be best complemented with
//		 writing unit tests checking those specific undocumented behavior we are going to rely on. Though I still see 
// 		 "source code is the ultimate truth" as means to exlain current behavior (say, in debugging) and not predict future behavior
//		 in newer versions. Even their famous "Go 1 Promise" has this disclaimer: "The promise applies to the exported APIs 
// 		 and their documented behaviors. It does not apply to internal implementation details, which may change[...]".
//		But of course that approach is not practical. I quickly discovered no one actually makes promises like translating everything
//		 on SDK borders, and BESIDES there remains a problem that the timed out context might be not the one created down the stack,
//		 but rather one created above (see my earlier talk about parent context timeout) - then for the SDK function we call
//		 that's the same thing, it will say "I stopped because of a timeout given to me" even if actually it was not OUR timeout
//		 that expired.
//
//		Another idea occured to me is this.
//		IF KeepAliveOnce has been interrupted by a timeout, it might still mean it would otherwise return some error and not
//		succeed anyway. The only difference between failing before timing out and timing out before failing is that in 1st case
//		the failure was observed by KeepAliveAttempt. So, it's rather justifiable to just check ctx for timing-out AFTER 
// 		getting error from KeepAliveOnce, and it it really timed-out - just PRETEND we did not see the error before the timout
// 		(like if KeepAliveOnce returned timeout rather than otherwise errored). This does not hurts correctness, but this
//		might be suboptimal or hurt observability to some degree, because by doing so we essentially hide an error we could 
// 		otherwise log or make adjustments for.
//		Actually, that results in completely the same approach people proposed me (and which I implemented),
// 		but their proposal lacked justification and simply conflated two things: seeing that context has timed out,
//      and knowning that this timeout is the reason of failure. Unexpectedly, there is a validation to that.
//
//		And ultimately it hit me that the idea just above is actually already widely used under the hood!
//		Take any foo(ctx), at lowest level there is not such thing as timeout (what adding two integers, for example),
//		so each action accepting timeout will most likely SIMULATE it rather that actually using some mechanism that combines
// 		execution and timeout on hardware level. Two ways are possible: 1) split work into steps and check for timeout between steps,
//		2) do work in one goroutine, await timeout in another, and react to whatever finishes first. Actually the 2nd approach
//		is a widely used pattern in go, expressed with select {}.
//		But that's nearly the same thing as checking ctx.Err() after completion of foo()! Well, checking is less precise but
//		conceptually both ways suffer the same problem of "not observing" of actually received error. And no one cares!
//		Then it's not just justifiable but completely valid to use that appoach:
//		- do not rely on foo()'s returned error for decicions
//		- use select{} or directly check for context timeout/cancellation instead
//		- and even that way we can only be sure that our CONTEXT has stopped because of timeout - 
// 		  but we cannot be sure that out timeout actually expired :) because the timeout we specified for context
//		  could be shortened under the hood to match parent's timeout in parent context. In that case, upon context timeout
//		  we DO need to interrupt, but it does not mean we need to act the same way as if OUR timeout run out,
//		  rather it depends on what we want. If such cases - where we want to reason about our timeout specifically - 
// 		  we well need to set some independent Timer and wait for it.
//		  Contexts are good for interrupting but they are bad for discovering exact reasons.
//		- the error returned from foo() when it's interrupted is only handy for debugging or if the calling function does
//		  not actually care WHY foo() did not do its work (error, or interruption - in both cases the caller is just going to also
//		  stop, returning the same value - probably wrapped). That's actually most of cases, mind you :) 
// 		  But if the caller really does need reasoning about whether that was a "real" error containing wrapped timeout,
// 		  or just wrapped timeout, and what context was it that timed out - that "error" is no source of information.
