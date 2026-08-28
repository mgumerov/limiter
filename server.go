package main

import (
	"context"
	"log"
	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"
)

type Request struct {
	amount int
	reply chan<- Response
}

type Response struct {
	granted int
}

type Processor struct {
	rqChan chan<- Request
}

const MAX_REQUEST int32 = 1000000
const LIMIT int32 = 1 //1 RPS

func main() {	
	processorFailed := make(chan struct{}, 1)
	var processor *Processor = startProcessor(processorFailed);

	//The remainder of this function could be just fiber.Listen(), but Fiber v2 does not listen context for graceful
	// shutdown, neither does it return control after inialization. So, decided to move its startup into a goroutine
	// completely and provide means for watching for subsequent failure of this or other subsystems.
	//But, fiber V3 actually knows how to listen for graceful shutdown; meaning, if we accept 
	// it as our only "blind-launch" but critical service, we could skip it all and just fiber.Listen()
	// right here, since we won't be needing to wait for some other events in parallel.
	//In the end, I decided to take the longer route because I want to be able to use other engines than Fiber,
	// so I don't want it to transform my main routine into Fiber's event loop. Besides, this requires me to
	// learn how to cope with related problems.

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM, //sadly no platform-neutral os.* constant for this; even though actually Go translates to SIGTERM on Windows
	)
	defer stop() //we won't need this context after this function completes

	//Idiomatic approach says passing contexts along (here, or/and to fiber.Listen) because this clearly involves some 
	// i/o and long activity. But actually it depends on how the called func will use the context: maybe I am passing 
	// request-scoped context to some async processing, for example. In this case, passing globally-scoped notify-context 
	// actually makes sense, but only if I want Fiber to use it for graceful shutdowns, and I stated above that I prefer it not to.
	fiberFailed := make(chan struct{}, 1)
	fiber := createHTTP(processor, fiberFailed)
	startHTTP(fiber, fiberFailed);
	
	select {
	case <- ctx.Done():
		//do nothing
	case <- fiberFailed:
		log.Print("HTTP server terminated unexpectedly") //since we did not tell it to shut down yet
		fiber = nil
	case <- processorFailed:
		log.Print("Processing Worker terminated unexpectedly") //since we did not tell it to shut down yet
		fiber = nil
	
	}
	
	if fiber != nil {
		if err := fiber.Shutdown(); err != nil {
			log.Print(err)
		}
	}
	processor.close()
}


type Bucket struct {
	count int32
	limit int32
	startedAt time.Time
	issued int64
}

// In worker-pool approach, worker goroutines never complete, but they are reused and that saves some allocations etc.
// In semaphore-protected approach, answering routines are completed and cleanly recreated, that's cleaner
//  (no need to guarantee goroutine will not die out of panic) but more expensive.
// Also, since I don't want more than 1 request processed at each moment, I can use mutex-protected approach. 
//  It does not use allocations and does not need goroutine creation at all, and appears like most efficient solution.
// What do I pick? I'll allow selecting between worker-pool and mutex, because I have some reservation about mutex drawbacks 
//  and need to do measurements.
func startProcessor(processorFailed chan<- struct{}) *Processor {
	reqChan := make(chan Request) //unbuffered, because what good such buffering is - under heavy load?
	
	//Process all requests sequentially by a single goroutine
	go func() {
		defer func() {
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
				log.Printf("Processing Worker panicked: %v", r)
				processorFailed <- struct{}{}
			}
		}()

		//We could start with startedAt=0, but then one of two things happen
		// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
		// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
		bucket := Bucket { limit: LIMIT, startedAt: time.Now() }

		//Note, range loop, just like 2-argument reading form, checks for closing the channel,
		// we'll use it to propagate graceful shutdown to our goroutine
		for request := range reqChan {
			refill(&bucket, time.Now())

			if (request.amount > int(MAX_REQUEST)) { //int32 fits in int
				request.amount = int(MAX_REQUEST)
			}
			amount := int32(request.amount)
			
			if bucket.count < amount {
				request.reply <- Response { granted: 0 }
			} else {
				bucket.count -= amount
				request.reply <- Response { granted: request.amount }
			}
		}
	}()

	return &Processor { rqChan: reqChan }
}

//TODO: how critical can possible time leap be? Like, in "leap second" or "switch to daylight time"
func refill (bucket *Bucket, now time.Time) {
	//Have to re-establish some type boundaries to avoid precision loss (= increment in stairs) 
	// by accidentally casting float64(int64) when I wanted to cast float64(int32); or to avoid
	// messing up integer conversion.
	var limit int32 = bucket.limit

	//Actually we don't need utmost precision here, if we pour less buckets this microsecond, we'll just pour more the next one;
	// we only want it to be more or less smooth, so millis would not work good. At the same time, why lose precision by using micros
	// when we can just as well use nanos? Even 100 years as Nanos still fits int64
	elapsed := now.Sub(bucket.startedAt).Nanoseconds()
	expectation := float64(elapsed) / float64(time.Second.Nanoseconds()) * float64(limit)
	delta := int64(expectation) - bucket.issued
	bucket.issued += delta

	//Now put them to bucket. But, we don't necessarily put all of them - to avoid bursts after delays
	//This also helps us avoid >int32 deltas
	if delta > int64(limit) {
		delta = int64(limit)
	}
	bucket.count += int32(delta)
	if (bucket.count > limit) {
		bucket.count = limit
	}
}

//Although idiomatic (responding via channel), implementation is still better be hidden
func (p *Processor) request(amount int) Response {
	//buffered, because it makes no sense to block when responding, however little are chances;
	// also, this avoids depending on the receiving side _still being alive_ (might have panicked or whatever)
	//TODO potentially a performace hindrance, say 1M allocations/second, need to somehow measure and try another approach
	reply := make(chan Response, 1)
	p.rqChan <- Request { amount: amount, reply: reply }
	return <- reply
}

//Relies on no concurrent/subsequent calls to request()
//(if that is idiomatic expectation then we don't need that comment)
func (p *Processor) close() { //close is the idiomatic name for stopping lifecycle and releasing resources
	close(p.rqChan)
}

func createHTTP(processor *Processor, fiberFailed chan<- struct{}) *fiber.App {
	app := fiber.New()

	app.Use(recoverer.New(recoverer.Config {
		PanicHandler: 
			func(c fiber.Ctx, r any) error {
				fiberFailed <- struct{}{}
				//Since panics are worse situation in Go than say Java exceptions, we don't look at the error and deduce what
				// happened and what details to give our client; we don't give any
				return fiber.ErrInternalServerError
			}}));

	//Actually we should be looking up a bucket which a specific API call uses (say, calls to one service uses one bucket, calls to other service use another);
	// and futher development of that idea would be sharding instances by API key.
	//But let's leave all that out of scope for the sake of being concise: multiple buckets is simple to implement and does not require any learning,
	// and sharding management is somewhat trickier but makes the project bloat without showing any extra Go mastery.
	app.Get("/", func(c fiber.Ctx) error {
		if processor.request(1).granted == 1 {
			return c.SendStatus(fiber.StatusOK)
		} else {
			return c.SendStatus(fiber.StatusTooManyRequests)
		}
	})

	return app
}

func startHTTP(fiber *fiber.App, fiberFailed chan<- struct{}) {
	go func() {
		defer func() { //always report termination
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
        	    log.Printf("HTTP server panicked: %v", r)
				fiberFailed <- struct{}{}
        	}
		} ()
			
		//startup errors return non-nil, graceful shutdown returns nil, shutdown errors are only returned via shutdown() - not here
		if err := fiber.Listen(":3000"); err != nil {
			log.Printf("HTTP server startup failed: %v", err)
			fiberFailed <- struct{}{}
		}
	}()
}

//TODO: real bucket algo
//TODO: mutex + fast path (or maybe CAS)
//TODO: config for acceptance limit Q (max sum quota) and acceptance multiplier K; load shedding over capacity = L * K
//      (google's default is K=2.0, provided that rejection path is much shorter than acceptance path, but I'll need to measure
//       whether that's my case or not, and maybe set it to some LOWER value - after all, by serving over Q, we only want
//.      to propagate feedback to clients, we don't necessarily need to serve those extra requests out of pure principle;
//.      thus, if that's too costly, we are going to just ignore them).*
//.      * Setting the capacity closer to Q, however, means load-shedding is more likely to happen, and it can hurt well-behaving
//.        clients; that's bad, but if it's good enough compromise for Google, I'll take it. 
//         Thus, picking good value for K must be tuned for each specific system, minding how compliant the clients are expected to be. 