package casproc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"limiter/server"
)

//This approach tries to make a compare-and-set-style update on bucket's count instead of putting mutex on it,
// betting on the fact that all our operations are super fast (however, GC pauses are worrying me).
//Unfortunately, with current algorithm, the bucket contains not 1 but 2 fields, and there is no way to atomically
// update two int64 fields together (we could for int32 by combining them). Even if we add 3rd field as version control (*),
// we cannot just "update two fields then update version" because those are several updates and anyone can see partial changes mid-flight.
//   (*) which itself is tricky; atomic access to it will not guarantee observing changes of other fields, happens-before guarantees
//       are need to be thoroughly reviewed to make sure of that;
//       for example goroutine1 { count := x; v.Store(y) } goroutinte2 { v.Load(); if (count) } is probably OK, because count := is
//       sequenced-before v.Store, v.Store is serialized-before v.Load, and v.Load() is sequenced-before "if (count)".
//
//Good thing is, changes need not to be atomic! At least I think so (TODO add some validity tests to make sure no one ever over-consumes).
// I came up with this algorithm. We divide each processing in 4 stages:
// 1. Calculate C = how much tokens need to be added. This is read-only stage. If C = 0, skip stages 2-3.
// 2. Bucket.Issued += C, atomically, but only if someone else did not change Issued yet (so, this stage is classic CAS).
//    If the operation detects concurrent changes, we just skip stages 2-3 and proceed to stage 4: we will not add tokens and maybe will reject
//    a query we could have processed, but we just made sure someone is already on the way to refill the bucket, so it's just one unlucky query,
//    and even so, there may remain still enough tickets in the bucket to grant it.
//    Note then we add to Issued first, because if we add to count instead, and then try to add to issue and see it has been changed,
//    that would mean we should not add to issue (to avoid producing tokens for same time interval twice), but we would have already added to count
//    and someone even might have used those added tokens.
// 3. Bucket.Count += C, atomically, unconditional. That's what makes my approach work: adding to actual count can be delayed, and it's OK
//    if someone else adds more tokens in between, if we know that they produced by another time interval.
//    Note: actually we CANNOT do it just with one atomic increment, if we want to avoid bursts:
//    two concurrent threads might together insert too much and cause Count to go above Limit temporarily,
//    and we cannot just do that and then remove excess count, because someone might have already observed the new Count.
//    That's why we'll have to use some form of retries and backoffs instead. (Alternatively, each read from Count must limit it by Limit)
// 4. Bucket.Count -= request, atomically, unconditional. Might be combined with stage 3 maybe, but I'll refrain from it for now, for readability;
//    also it would make it harded to skip stage 3 and still apply stage 4, when that is necessary.
//    Note: actually here we face the same problem as in stage 3. We cannot just decrement, we need to make sure it does not go below
//    0 in the process!
type CasProcessor struct {
	//The described algorithm explicitly allows changing fields one by one in parallel, and Go's map explicilty forbids direct writes to its
	// entries, forcing us to replace the whole entry (=read struct and write changed struct), effectively invalidating the approach.
	// So in this processor we store pointers in the map. It's still not that bad, because those extra allocations are only made once.
	buckets map[string]*Bucket
	m sync.Mutex
	cfg *server.Config
	retries int
	lostAdding *metrics.Counter
	lostGranting *metrics.Counter
}
var _ server.Processor = (*CasProcessor)(nil) //fail-fast type guard

//Unfortunately we also need to have our own struct. Even if we can manage without modern atomic API (Int64.Add etc.),
// if we want to be cross platform (target 32bit platforms as well), then "legacy" atomic APIs might cause kernel panic
// on misaligned fields! And Issued, being not a first field and int64, risks just that. We either need to declare it Int64,
// or make it first (or use as an array), or manually insert padding (which is virtually impossible to do because field sizes 
// and padding that Go itself injects to satisfy field alignments is platform dependent)
//I'll go with "place it first" way then. It's less like modern Go, but it allows me to manually decide if I want atomic access or not,
// hopefully to better show/test grasp of happens-before concepts in Go.
type Bucket struct {
	Issued int64 // Must go first, then, to ensure good alignment to use it with legacy atomic APIs
	Count int32
	Limit int32				//immutable
	StartedAt time.Time		//immutable
}

//"retries" is number of attempts to do compare-and-set updates. See the implementation for details, but the general idea is that
// it should be kept low (say 3) because it only matters when there are many request to some bucket and high limit on it, 
// and under that circumstances rejecting a small extra fraction of requests to that bucket is probably OK. If more precision is necessary, 
// you might want to adjust that to 10 or even 100, but high values will be of little help because if request contention is that high, 
// spin-locks themselves will start draining more CPU than actual processing.
func StartCasProcessor(processorFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set, retries int) *CasProcessor {
	buckets := make(map[string]*Bucket)
	startedAt := time.Now()
	for key, limit := range cfg.APIs {
		//We could start with startedAt=0, but then one of two things happen
		// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
		// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
		//Also we start with empty buckets because of that same problem with "full bucket" approach
		buckets[key] = &Bucket { Limit: limit, StartedAt: startedAt }
	}

	//only stores count of occurences, not number of tokens lost
	lostAdding := myMetrics.NewCounter("lost_adding")
	//only stores count of occurences, not number of tokens lost
	lostGranting := myMetrics.NewCounter("lost_granting")
	
	//Extremely tricky! Since in CAS processor I don't use mutexes or channels for synchronization, 
	// this struct's own member functions, called from different goroutines, are risking not observing field values!
	// Especially given how Request() is called on a goroutine not created here by us, but created by Fiber possibly
	// sometime earlier.
	return &CasProcessor { buckets: buckets, cfg: cfg, retries: retries, lostAdding: lostAdding, lostGranting: lostGranting }
}

//Like Processor's contract says, this function is thread-safe but it relies on instance's safe publication.
// That way we can access immutable fields directly.
// Now the funny part: IF we couldn't rely on it, we would need to either atomically access all our fields (even immutables),
//  or use mutexes/channels/WaitGroups to wait for completion of construction (no actual waiting should actually happen);
//  and mutex/channel alone will be a bit constly even on a fast path (because this is very fast function) - WaitGroup would be better
//  or we could combine a boolean atomic flag with mutex/channel to act as fast path.
func (p *CasProcessor) Request(key string, amount int32) server.Response {
	//Make own copy
	pBucket, ok := p.buckets[key]
	if (!ok) {
		return server.Response { Granted: 0 } //for real use, we should maybe distinguish this error reason
	}

	//The algorithm is conceptually identical to the one used in other Processors, so most actions are explained there and I don't repeat it here.
	now := time.Now()

	//Unless atomic-guarded, first read of this in each goroutine is not guaranteed to happen-after last write to it, even if for subsequent
	// calls to this function from same goroutine we are sure to observe some write (if at some point HB is established for some other pair of operations);
	// and anyway we need not just "some" written value, but the most current one.
	//Besides, direct reading of int64 even under HB guarantees might cause machine word tearing (partial read).
	//So we use sync apis to access it.
	//Also, have to use old style APIs because the field is not declared atomic
	issuedWas := atomic.LoadInt64(&pBucket.Issued) //Copy it before calculations, because results of calculations will need to be applied only if value is not changed since

	//Stage 1
	var limit int32 = pBucket.Limit
	elapsed := now.Sub(pBucket.StartedAt).Nanoseconds()
	expectation := float64(elapsed) / float64(time.Second.Nanoseconds()) * float64(limit)

	delta := int64(expectation) - issuedWas
	if delta != 0 {
		//Stage 2
		if (!atomic.CompareAndSwapInt64(&pBucket.Issued, issuedWas, issuedWas + delta)) {
			delta = 0
		}
	}

	if delta != 0 {
		
		//Stage 3
		if delta > int64(limit) {
			delta = int64(limit)
		}

		//Could simply increment atomically, but we want to prevent going above limit even when average RPS remains OK
		//Limited number of retries here. It's not mutex which we test with CAS and it might be in "closed" state for long time,
		// so we will only fail if right at the same time someone changes it - which means incredible contention. Such contention
		// means high number of requests and high limit; then not adding 1-2 tokens to bucket because of contention seems like
		// no great problem, and neither does rejecting 1% more requests.
		//But there can be circumstances where that breaks:
		// - Lots of requests despite low limit. (So, one missed adding to bucket might mean a lot; though actually the algorithm 
		//   mitigales that effect by non contending if it has nothing to add, i.e. those requests that actually are accompanied by
		//.  adding to bucket will mostly go uncontended). But this case means that in any case the caller gets most of its requests rejected
		//   and probably cannot function properly anyway.
		// - Brief bursts, like, most of the second nothing happens, then some request comes, it wants to add many tokens, but some 
		//   other request wins (adding just 1 maybe). That's also unlikely: for the 2nd request to add less tokens, it must first observe
		//.  that the 1st request already issued its tokens, so, first request wins - but 1st request might win adding to Issied 
		//   and still lose adding to Count (say 2nd request observed adding to Issued, then very quickly deduced that it only needs to issue 1,
		//   and actually issued it and adds to count, and its adding to Count wins over the 1st request happening right that same moment).
		//If we are seeing lots of those (unexpected) cases, we might want to use higher number of retries, or use Mutex-based processor
		// in place of this one.
		cLostAdd := 0
		for range p.retries {
			increment := int32(delta)
			//look at the current value of count to determine how much we can add
			//Like with Issued, it seems to me that whenever any goroutine FIRST makes that read from Count,
			// no HB relations bind some write to happen BEFORE that read, therefore we need to insert atomic access.
			// Particularly, docs say that every atomic write to a variable HB every atomic read to it, and that includes CAS
			// ("The compare-and-swap operations... act as both atomic reads and atomic writes for the purposes of the global total order.")
			old := atomic.LoadInt32(&pBucket.Count)
			new := old + increment
			if new > limit {
				new = limit
				if old == new { //meaning new=old=limit
					break //Of course we don't wait until we can add something - who knows maybe no one consumes a ticket for next hours
				}
			}
			if atomic.CompareAndSwapInt32(&pBucket.Count, old, new) {
				break
			} else {
				cLostAdd++
			}
		}
		if (cLostAdd == p.retries) {
			p.lostAdding.Add(1) //under extreme LOSS rate this can itself become source of contention but it would require truly massive RPS
		}
	}

	//Stage 4
	if (amount > p.cfg.MaxRequest) {
		amount = p.cfg.MaxRequest
	}
	//Cannot just decrement atomically, because we must not go below 0
	//Note: since retries are limited, we must make sure that if they are spent, we are not granting anything 
	// (couldn't substract from the bucket => couldn't grant to client) that's why I set it to 0 explicitly at the start
	var granted int32 = 0
	cLostGrant := 0
	for range p.retries {
		decrement := amount
		//This time, it might appear like we could skip atomic access, because due by standing "after" the last atomic read of same var in same goroutine
		// this read will at least see that-recent value. But we need the most recent value! So again we need atomic read.
		old := atomic.LoadInt32(&pBucket.Count)
		new := old - decrement
		if new < 0 {
			new = 0
			if old == new { //meaning old=new=0 => the bucket is empty
				break //we don't wait for eventual refilling
			}
		}
		if atomic.CompareAndSwapInt32(&pBucket.Count, old, new) {
			granted = old - new
			break
		} else {
			cLostGrant++
		}
		if (cLostGrant == p.retries) {
			p.lostGranting.Add(1)
		}
	}

	return server.Response { Granted: granted }
}

func (p *CasProcessor) Close() {
	//do nothing
}
