package mutexproc

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"limiter/server"
)

type MutexProcessor struct {
	buckets map[string]server.Bucket
	m sync.Mutex
	cfg *server.Config
}
var _ server.Processor = (*MutexProcessor)(nil) //fail-fast type guard

//Like Processor interface says, the created instance is thread safe to use but still has to be published safely.
// Just guarding Request method with a mutex does not mean it happens-after initialization of all fields of MutexProcessor
// in absence of any other guarantees; of course we could guard its construction here with the very same mutex 
// and that would solve the problem, but thankfully the clause in the interface lets us just rely on safe publication instead.
func StartMutexProcessor(processorFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) *MutexProcessor {
	buckets := make(map[string]server.Bucket)
	startedAt := time.Now()
	for key, limit := range cfg.APIs {
		//We could start with startedAt=0, but then one of two things happen
		// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
		// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
		//Also we start with empty buckets because of that same problem with "full bucket" approach
		buckets[key] = server.Bucket { Limit: limit, StartedAt: startedAt }
	}
	
	return &MutexProcessor { buckets: buckets, cfg: cfg }
}

func (p *MutexProcessor) Request(key string, amount int32) server.Response {
	p.m.Lock() //Thanks to Go RTL, their Mutex even has fast-path by trying to spin-lock a little, before committing to acquiring lock on a contended Mutex
	defer p.m.Unlock() //TODO sub-optimal because finalizing actions like reporting need not be done within Mutex guard, it stretches time spent within mutex

	bucket, ok := p.buckets[key]
	if (!ok) {
		return server.Response { Granted: 0 } //for real use, we should maybe distinguish this error reason
	}

	server.Refill(&bucket, time.Now())

	if (amount > p.cfg.MaxRequest) {
		amount = p.cfg.MaxRequest
	}
	granted := min(amount, bucket.Count)
	bucket.Count -= granted
	p.buckets[key] = bucket

	return server.Response { Granted: granted }
}

func (p *MutexProcessor) Close() {
	//do nothing
}
