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
	Issued int64 // Must go first, then
	Count int32
	Limit int32
	StartedAt time.Time
}

func StartCasProcessor(processorFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) *CasProcessor {
	buckets := make(map[string]*Bucket)
	startedAt := time.Now()
	for key, limit := range cfg.APIs {
		//We could start with startedAt=0, but then one of two things happen
		// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
		// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
		//Also we start with empty buckets because of that same problem with "full bucket" approach
		buckets[key] = &Bucket { Limit: limit, StartedAt: startedAt }
	}
	
	return &CasProcessor { buckets: buckets, cfg: cfg }
}

// Интересно
// Даже с таким простым телом все равно не получается более 5М в секунду обслужить
// Интересно почему - тут же даже нет contention. Неужели тут уже запись в handler_time сказывается? С блокировками. Надо бы проверить!
// И для CAS это тоже может влиять

func (p *CasProcessor) Request2(key string, amount int32) server.Response {
	return server.Response { Granted: 0 } 
}

func (p *CasProcessor) Request(key string, amount int32) server.Response {
	//Make own copy
	pBucket, ok := p.buckets[key]
	if (!ok) {
		return server.Response { Granted: 0 } //for real use, we should maybe distinguish this error reason
	}

	//The algorithm is conceptually identical to the one used in other Processors, so most actions are explained there and I don't repeat it here.
	now := time.Now()

	//Когда вот это чтение из issued происходит в какой-то горутине ВПЕРВЫЕ, то с учетом того что горутина могла быть создана еще до CasProcessor,
	// нет никаких HB гарантий того, что она увидит последние изменения в Issued. При последующих чтениях может быть гарантии и есть.
	//???
	//And anyway, direct reading of int64 even under HB guarantees might cause machine word tearing (partial read).
	//So we use sync apis.
	//Also, have to use old style APIs because the field is not declared atomic
	issuedWas := atomic.LoadInt64(&pBucket.Issued) //Copy it before calculations, because results of calculations will need to be applied only if value is not changed since

	//Stage 1
	var limit int32 = pBucket.Limit //!!! all writing to Limit is make before creating instance of CasProcessor. Todo check if it's enough to ensure they are observed here - in its "member function" 
	elapsed := now.Sub(pBucket.StartedAt).Nanoseconds()
	expectation := float64(elapsed) / float64(time.Second.Nanoseconds()) * float64(limit)

	delta := int64(expectation) - issuedWas
// if (delta > 0) {
// 	slog.Info("Producing", "delta", delta)
// }
//slog.Info("Tracing", "issuedWas", issuedWas)
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
		for {
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
				if old == new {
					break //It already has that value, writing to it now will not change anything
				}
			}
			ack := atomic.CompareAndSwapInt32(&pBucket.Count, old, new)
			if ack {
				break
			}

			//TODO do something about potentially infinite spinning, consuming CPU
		}
	}

	//Stage 4
	if (amount > p.cfg.MaxRequest) {
		amount = p.cfg.MaxRequest
	}
	var granted int32
	//Cannot just decrement atomically, because we must not go below 0
	for {
		decrement := amount
		//This time, it might appear like we could skip atomic access, because due by standing "after" the last atomic read of same var in same goroutine
		// this read will at least see that-recent value. But we need the most recent value! So again we need atomic read.
		old := atomic.LoadInt32(&pBucket.Count)
		new := old - decrement
		if new < 0 {
			new = 0
			if old == new {
				granted = 0
				break //It already has that value, no need to write anything
			}
		}
		ack := atomic.CompareAndSwapInt32(&pBucket.Count, old, new)
		if ack {
			granted = old - new
			break
		}

		//TODO do something about potentially infinite spinning, consuming CPU
	}

	return server.Response { Granted: granted }
}

func (p *CasProcessor) Close() {
	//do nothing
}
