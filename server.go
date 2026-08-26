package main

import (
	"log"
	"context"
	
	"os/signal"
	"os"
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

		limit := 2

		//Note, range loop, just like 2-argument reading form, checks for closing the channel,
		// we'll use it to propagate graceful shutdown to our goroutine
		for request := range reqChan {
			if limit < request.amount {
				request.reply <- Response { granted: 0 }
			} else {
				limit -= request.amount
				request.reply <- Response { granted: request.amount }
			}
		}
	}()

	return &Processor { rqChan: reqChan }
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
			return c.SendString("c.SendStatus(fiber.StatusOK)")
		} else {
			return c.SendString("c.SendStatus(fiber.StatusTooManyRequests)")
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

//TODO: fill bucket; how?
//TODO: real bucket algo
//TODO: mutex + fast path