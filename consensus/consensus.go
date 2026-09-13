package consensus

import (
	"context"
	"fmt"
	"limiter/server"
	"log/slog"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcd "go.etcd.io/etcd/client/v3"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

type EtcdConsensusTracker struct {
	IsMaster atomic.Bool
}
var _ server.ConsensusTracker = (*EtcdConsensusTracker)(nil) //fail-fast type guard

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
			//TODO use WithRequireLeader context maybe?
			offendingKey, err := captureKeyAndKeepSupporting(cli, cfg.ETCDkey, ctx)
			if err != nil {
				//???
			}
			if offendingKey != nil {
				waitForKeyDeletion(cli, offendingKey, ctx)
			}
		}
	}()

	return &tracker
}

type Lease struct {
	ID      etcd.LeaseID

	//Note that Go's time maintains monotonic clock (*) and thus helps handle time leaps. 
	// It even uses monotonic clock in time comparisons (of course only so far as Equals/Before/etc predicates are concerned).
	// Otherwise we might time-travel 1 hour back (say because of taking a flight somewhere)
	// and happily continue serving requests for 1 hour after our 1-second lease has long expired.
	// (*) Or rather it does IF the Time instance contains it. This one does, and so does time.Now().
	//Also note that this should not be regarded as precise moment of expiration, but rather a close approximation from below.
	// We don't get to know what moment ETCD counts this lease's TTL from, and even if we did - our clock might drift from theirs,
	// but we can pick some moment which is both close to true expiration and still is before it.
	Expires time.Time //This instance's local time, not ETCD's
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
		//This looks awkward but said to be most idiomatic way, even though it creates a transient Duration of X nanos despite X being measured in seconds.
		// But on a side note, it's justifiable: 
		// X seconds =  X * 1 Sec = X * (1 Nano) * 1 Sec / (1 Nano) = (X Nanos) * (exactly one of time constants "how many nanos in *")
		Expires: rqTime.Add(time.Duration(resp.TTL) * time.Second),
	}, nil
}

//If a key has been created, returns nil; if a key already existed, returns its metadata at the version at the moment of attempt
func createKey(cli *etcd.Client, keyname string, lease Lease) (*mvccpb.KeyValue, error) {
	//todo timeout and what it means for us
	txnResp, err := cli.KV.Txn(context.Background()).
		//Beware of "version" vs "revision" (as in CreateRevision/ModRevision), the latter two are not reset if a key is deleted,
		// they keep incrementing monotonically when it's created again. Versioning, on the other hand, will restart.
		If(etcd.Compare(etcd.Version(keyname), "=", 0)). //"if the key does not exist" (its version is 0)
		Then(etcd.OpPut(keyname, "test" /*maybe lease ID? to compare. But we have one in bound lease and maybe can read it from there?*/, etcd.WithLease(lease.ID))).
		Else(etcd.OpGet(keyname, etcd.WithKeysOnly())). //return metadata about offending key; do not return its value
		Commit()
	var key *mvccpb.KeyValue
	if !txnResp.Succeeded { //in this case I asked to fetch version, it's the only items in Responses
		key = txnResp.Responses[0].GetResponseRange().Kvs[0] //we only asked for 1 key
	}
	return key, err
}


func (p *EtcdConsensusTracker) IAmMaster() bool {
	return p.IsMaster.Load()
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
//  returning the non-nil metadata of offending key at the moment of attempt.
//- If succeeded, does not return until 1) fails to keep the lease alive, or 2) fatal ectd failure happens, or 3) context is cancelled.
//  then that happens, returns nil.
//- And as usual, if fails for some other reason, returns err != nil
func captureKeyAndKeepSupporting(cli *etcd.Client, keyname string, ctx context.Context) (*mvccpb.KeyValue, error) {
	lease, err := acquireLease(cli)
	if err == nil {
		slog.Info("Lease", "response", fmt.Sprintf("%#v", lease))
	} else {
		//????
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
		//TODO ???
		return nil, err //or maybe wrap it
	}
	if offendingKey != nil {
		return offendingKey, nil
	}
	
	updates, err := cli.Lease.KeepAlive(ctx, lease.ID)
	if err != nil {
		//????
		return nil, err //or maybe wrap it
	}

	//TODO А потом вообще переделать и в main на то чтобы не вызывать всем Shutdown а отменять контекст
	// - но это подходит только тем кто умеет его слушать (надо проверять)
	for upd := range updates {
		//TODO update cached lease
		slog.Info("TTL updated", "TTL", upd.TTL)
	}

	return nil, nil
}