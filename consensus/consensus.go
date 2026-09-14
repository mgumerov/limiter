package consensus

import (
	"context"
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

//TODO !!! Ok but now I am not using standard KeepAlive loop - update this
//
//Here is where I will compromise. Problem is, etcd is not built for sub-second TTLs - even if I set a very brief
// leader election timeout of under 600s, it will only mean TTL=1 seconds. Meaning, if our cluster gets a split-brain right
// after lease starts, this instance will grant tickets for 1 more seconds. In this limiter service it's no problem because 
// we don't need to replicate to other instances; but real problem is that for the same time other instances will serve nothing,
// even if clients will notice we are not responding (died or non-reachable) and try to go to new leader instead.
//Having 10-seconds delay in service is not what we expect or want from 6M rps rate limiter :)
//Also, low TTLs mean the high-level client might not have enough time to do its keepalive+prolongation work (in a hidden loop),
// leading to losing of lease. Normally it sends keepalives each 1/3 TTL, but it has a strict low limit of each 500 ms,
// which is only 1/2 of 1 second TTL (but still should be enough).
//
//To avoid overcomplication (I just want to explore how to work with leases and is-master tracking) I will refrain from inventing
// something outside normal approach, despite those concerns. I will simply try to request a 1-second TTL, even though I'll probably 
// end up granted with at least 2 or 3 seconds instead.
const LEASE_TIME_REQUEST = 1

//TODO Configurable
//TODO time.Minute would be better in the long run, but it seems that first attempts are suffering context deadline because initial client handshake is rather slow,
// so they will have to wait and that's bad; progressive backoff might be a good idea.
const ETCD_BACKOFF = 1 * time.Second 

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
		}
		defer cli.Close()

		//Eventually I came to this approach. See end of file for (1) with explanation of alternatives
		//Loop invariant: this instance does not hold an active lease supporting this instance's claim of ownership (lease-guarded key in etcd)
		for ctx.Err() == nil {			
			//TODO use WithRequireLeader context maybe?Etcd key capture failed
			offendingKey, err := tracker.tryCaptureAndHold(cli, cfg.ETCDkey, ctx)
			if err != nil {
				slog.Error("Etcd key capture failed", "Error", err)
				sleep(ETCD_BACKOFF)
				continue
			}
			if offendingKey != nil {
				slog.Info("Another instance holds the key", "Key", offendingKey)
				waitForKeyDeletion(cli, offendingKey, ctx)
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
func acquireLease(cli *etcd.Client) (Lease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond * 10)
	defer cancel()

	rqTime := time.Now()
	//For some unfathomable reason, clientv3 developers added field Error to response, which does not exist in etcd grpc API,
	// and never explained what it is. Namely, should I use it as an additional error flag or it's just mirroring something?
	// On the other hand, they provided an example. I will adhere to it, but of course an example might not be a production grade approach.
	resp, err := cli.Grant(ctx,  LEASE_TIME_REQUEST)
	if err != nil {
		return Lease{}, err
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

//If a key has been created, returns nil; if a key already existed, returns its metadata at the version at the moment of attempt
func createKey(cli *etcd.Client, keyname string, lease Lease) (*mvccpb.KeyValue, error) {
	if lease.ID == 0 {
		return nil, fmt.Errorf("lease ID = 0 on input")
	}
	//todo timeout and what it means for us
	txnResp, err := cli.KV.Txn(context.Background()).
		//Beware of "version" vs "revision" (as in CreateRevision/ModRevision), the latter two are not reset if a key is deleted,
		// they keep incrementing monotonically when it's created again. Versioning, on the other hand, will restart.
		If(etcd.Compare(etcd.Version(keyname), "=", 0)). //"if the key does not exist" (its version is 0)
		Then(etcd.OpPut(keyname, "test" /*maybe lease ID? to compare. But we have one in bound lease and maybe can read it from there?*/, etcd.WithLease(lease.ID))).
		Else(etcd.OpGet(keyname, etcd.WithKeysOnly())). //return metadata about offending key; do not return its value
		Commit()
	if err != nil {
		return nil, err
	}
	var key *mvccpb.KeyValue
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

func (p *AlwaysMasterConsensusTracker) Close() {
	//Do nothing
}

func (p *AlwaysMasterConsensusTracker) IAmMaster() bool {
	return true
}

func waitForKeyDeletion(cli *etcd.Client, offendingKey *mvccpb.KeyValue, parentCtx context.Context) {
	//TODO use WithRequireLeader maybe?
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	for batch := range cli.Watch(ctx, string(offendingKey.Key),
		etcd.WithFilterPut() /* skip all PUTs, because we only want DELETEs */,
		etcd.WithRev(offendingKey.CreateRevision)) {
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
}

//Attempts to create an ownership-guarding key; if successful, keep extending its lease as long as possible.
//- If fails to create a key specifically because of someone holding it, returns immediately, 
//  returning the non-nil metadata of offending key at the moment of attempt, and nil error
//- If succeeded, does not return until 1) fails to keep the lease alive, or 2) fatal ectd failure happens, or 3) context is cancelled.
//  then that happens, returns nil.
//- And as usual, if fails for some other reason, returns err != nil
//This function expects safe publication of receiver, although is supposed to be called on other gorounites
func (p *EtcdConsensusTracker) tryCaptureAndHold(cli *etcd.Client, keyname string, ctx context.Context) (*mvccpb.KeyValue, error) {
	lease, err := acquireLease(cli)
	if err != nil {
		return nil, fmt.Errorf("Cannot acquire a lease: %w", err)
	}
	//Normally when the current function returns it's because the lease is expired anyway, but there are cases when it's not:
	// - panic
	// - persistent ectd error
	// - graceful shutdown
	//Therefore, not to make others wait until its expiration despite us NOT servicing, we make sure we release the lease
	defer func() {
		_, err := cli.Lease.Revoke(context.Background(), lease.ID)
		if err != nil {
			if err == rpctypes.ErrLeaseNotFound {
				//Lease has actually expired, do nothing
			} else {
				slog.Error("Could not renounce the lease, it might still be locking other instances out!", "Error", err, "Lease", lease.ID)
			}
		}
	}()

	offendingKey, err := createKey(cli, keyname, lease)
	if err != nil {
		return nil, fmt.Errorf("Key creation failed: %w", err)
	}
	if offendingKey != nil {
		return offendingKey, nil
	}

	slog.Info("Ownership acquired")
	
	//Initial lease is also published by this call
	updates := keepLeaseAlive(ctx, cli, lease)
	for newLease := range updates {
		slog.Info("Extended lease", "Lease", newLease)
		p.lease.Store(&newLease)
	}

	slog.Info("Ownership lost")
	return nil, nil
}

//Publish all updates to channel; initial lease is also published.
//
//Unfortunately it seems we cannot afford to just use the recommended client's KeepAlive() loop.
// With the loop we know about each lease extension only postfactum, so it's not possible to say
// "I requested extension at XX -> the lease extends sometime before XX + new_TTL" like we do when first requesting a lease.
//
//TODO Кстати может быть стоит предложить PR который добавит в стандартный loop возможность отследить время начала запроса?
//     Это ведь несложно.
func keepLeaseAlive(ctx context.Context, cli *etcd.Client, lease Lease) <-chan Lease {
	channel := make(chan Lease, 1) // Try to be nonblocking

	go func(c chan<- Lease) {
		defer close(channel)

		//TODO Configurable
		attemptTimeout := time.Duration(100) * time.Millisecond
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
				step = max((lease.TTL - attemptTimeout) / 3, minStep)
				c <- lease
			} else {
				if err == context.Canceled {
					//treat it as transient error (even if it's not): do not update the cached lease but do not break the loop
				} else {
					break
				}
			}

			nextAttempt = nextAttempt.Add(step)
			if nextAttempt.After(lease.Expires()) {
				break
			}
			if !waitUntil(ctx, nextAttempt) {
				break // failed to wait because context was cancelled
			}
			newLease, err = keepAliveAttempt(cli, lease)
		}
	}(channel)

	return channel
}

func waitUntil(ctx context.Context, then time.Time) bool {
	timer := time.NewTimer(time.Until(then))
	defer timer.Stop()
	select {
	case <- ctx.Done():
		return false
	case <- timer.C:
		return true
	}
}

//Attempts to invoke keep-alive for given lease within a small limited time-window.
//If err != nil, returns the same lease
//Returns context.Canceled as error if failed to succeed within permitted time window.
// It does not accept context as input, therefore that's the only case when it returns that error.
func keepAliveAttempt(cli *etcd.Client, lease Lease) (Lease, error) {
	// Set up new time-limited context with some little timeout - large enough to perform 1 or maybe more attempts,
	//  still little enough compared to lease TTLs (because that limit will be effectively substracted from TTL of extended lease
	//  when guesssing its expiration time)
	//Note: I opted for not respecting any cancellation attempts that may be coming from higher level context,
	// because I mean this timeout to be relatively small (like 100ms), no hurt in spending 100 more ms before cancelling
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(100) * time.Millisecond)
	defer cancel()
	
	now := time.Now()
	//Note: this func may actually make multiple attempts (on transient errors)
	resp, err := cli.KeepAliveOnce(ctx, lease.ID)
	if err != nil {
		return lease, err
	}
	return Lease { 
		ID: lease.ID,
		Requested: now,
		TTL: time.Duration(resp.TTL) * time.Second,
	}, nil
}

//TODO Конвенция говорит что если метод завершается из-за отмены контекста, он возвращает context.Canceled (если вообще возвращает error)
// - в моих методах следует это поддержать

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
