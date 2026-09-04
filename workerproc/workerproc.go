package workerproc

import (
	"log/slog"
	"time"
	"github.com/VictoriaMetrics/metrics"

	"limiter/server"
)

type WorkerProcessor struct {
	rqChan chan<- Request
}
var _ server.Processor = (*WorkerProcessor)(nil) //fail-fast type guard

type Request struct {
	key string
	amount int32
	reply chan<- server.Response
}

// In worker-pool approach, worker goroutines never complete, but they are reused and that saves some allocations etc.
// In semaphore-protected approach, answering routines are completed and cleanly recreated, that's cleaner
//  (no need to guarantee goroutine will not die out of panic) but more expensive.
// Also, since I don't want more than 1 request processed at each moment, I can use mutex-protected approach. 
//  It does not use allocations and does not need goroutine creation at all, and appears like most efficient solution.
// What do I pick? I'll allow selecting between worker-pool and mutex, because I have some reservation about mutex drawbacks 
//  and need to do measurements.
func StartWorkerProcessor(processorFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) *WorkerProcessor {
	reqChan := make(chan Request) //unbuffered, because what good such buffering is - under heavy load?
	ptime := myMetrics.NewSummary("processing_time")
	
	//Process all requests sequentially by a single goroutine
	go func() {
		defer func() {
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
				slog.Error("Processing Worker panicked", "error", r)
				processorFailed <- struct{}{}
			}
		}()
		
		buckets := make(map[string]*server.Bucket)
		startedAt := time.Now()
		for key, limit := range cfg.APIs {
			//We could start with startedAt=0, but then one of two things happen
			// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
			// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
			//Also we start with empty buckets because of that same problem with "full bucket" approach
			buckets[key] = &server.Bucket { Limit: limit, StartedAt: startedAt }
		}

		//Note, range loop, just like 2-argument reading form, checks for closing the channel,
		// we'll use it to propagate graceful shutdown to our goroutine
		for request := range reqChan {
			//TODO move this whole thing to "server" or some "algo", it's independent of specific server
			//On the other hand, maybe some implementations will need specific optimized variety of this algorithm

			start := time.Now()
			slog.Debug("Processing request", "key", request.key, "amount", request.amount)

			//Compared to my initial implementation, keeping buckets in heap (presumably) and retrieving potentially
			// new bucket each time - hurts locality. It might make sense to associate dedicated worker with each
			// bucket and make that bucket a local variable, that way maybe compiler will even use registers and not
			// real RAM. On the other hand, it's not like we use DIFFERENT workers to process same bucket, it's 
			// the other way around, so it should not hurt that much.
			// I did some *load tests* and they don't show significant differences. Actually, they weren't stable enough
			// to prove there is no difference, but in both cases pure processing time (not counting receiving from channel)
			// for 500k requests with pauses makes up for 140-150ms. So, further measurements would be necessary, but 
			// there is no immediately noticeable difference. More importantly, since I am aiming at 500k RPS
			// and currently only seem to serve 100K, clearly 140ms or say 120 or 160ms do not really mean any difference,
			// it's not the slowest thing on request path :(
			bucket, ok := buckets[request.key]
			if (!ok) {
				request.reply <- server.Response { Granted: 0 } //for real use, we should maybe distinguish this error reason
				continue
			}

			server.Refill(bucket, time.Now())

			if (request.amount > cfg.MaxRequest) {
				request.amount = cfg.MaxRequest
			}
			granted := min(request.amount, bucket.Count)
			bucket.Count -= granted
			request.reply <- server.Response { Granted: granted }

			ptime.UpdateDuration(start)
		}
	}()

	return &WorkerProcessor { rqChan: reqChan }
}

func (p *WorkerProcessor) Request(key string, amount int32) server.Response {
	//buffered, because it makes no sense to block when responding, however little are chances;
	// also, this avoids depending on the receiving side _still being alive_ (might have panicked or whatever)
	//TODO potentially a performace hindrance, say 1M allocations/second, need to somehow measure and try another approach
	reply := make(chan server.Response, 1)
	p.rqChan <- Request { key: key, amount: amount, reply: reply }
	return <- reply
}

func (p *WorkerProcessor) Close() {
	close(p.rqChan)
}
