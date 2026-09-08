package fiberserver

import (
	"fmt"
	"log/slog"
	"strconv"

	"time"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"github.com/VictoriaMetrics/metrics"

	"limiter/server"
)

var _ server.Server = (*fiber.App)(nil) //fail-fast type guard

func CreateFiberServer(processor server.Processor, fiberFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) server.Server {
	fiber := createHTTP(processor, fiberFailed, myMetrics)
	//This creates (or eventually creates) Fiber's goroutines that will then execute our functions which in turn access Config
	// and Processor's state. Because starting a goroutine is sequenced-after any code leading to it, and serialized-before the goroutine's
	// code, the goroutine will observe everything this method currently observes (in terms of concurrency). 
	// Meaning, processor is safely published to that goroutine as its contract requires.
	startHTTP(fiber, fiberFailed, cfg)
	return fiber
}

func createHTTP(processor server.Processor, fiberFailed chan<- struct{}, myMetrics *metrics.Set) *fiber.App {
	handlerTime := myMetrics.NewHistogram("handler_time")

	app := fiber.New()

	app.Use(recoverer.New(recoverer.Config {
		PanicHandler: 
			func(c fiber.Ctx, r any) error {
				fiberFailed <- struct{}{}
				//Since panics are worse situation in Go than say Java exceptions, we don't look at the error and deduce what
				// happened and what details to give our client; we don't give any
				return fiber.ErrInternalServerError
			}}));

	//Actually, different buckets could be served by different dedicated workers, especially since there are no causality requirements
	// between requesting from them. Instead, we'll keep serving them all from same thread, to avoid overcomplicating this
	// PoC work, because otherwise we need to consider it carefully (also we could serve different APIs from different shards).
	handler := func (key string, amount int32, c fiber.Ctx) error {
		start := time.Now()
		slog.Debug("Serving request", "key", key, "amount", amount)
		result := processor.Request(key, amount)
		handlerTime.UpdateDuration(start)
	
		if result.Granted == amount {
			return c.SendStatus(fiber.StatusOK)
		} else {
			//TODO Use some response payload type here, if it's not against HTTP RFC for 429
			// We shouldn't use classic headers like x-ratelimit-* here because it makes more sense for proxy, where that
			// information has to somehow complement normal response from server. For limiter, it's explicit part of API
			// so we better make it a response.
			// Maybe we shouldn't even return 429 actually. But it makes more sense for tests.
			c.Set("X-RateLimit-Granted", strconv.Itoa(int(result.Granted)))
			return c.SendStatus(fiber.StatusTooManyRequests)
		}
	}
	
	//Use POST, not GET, because of read-only and idempotency requirements of HTTP, and because request body allows to send more data and in a safer way.
	//TODO better move parameter to POST body
	app.Post("/:key", func(c fiber.Ctx) error {
		q, err := strconv.ParseInt(c.Query("q", "1"), 10, 32)
		if err != nil {
			return fmt.Errorf("Amount parse error: %w", err)
		}
		return handler(c.Params("key"), int32(q), c)
	})
	//However, for debugging purposes GET is sometimes more convenient, while under heavy development
	app.Get("/:key", func(c fiber.Ctx) error {
		q, err := strconv.ParseInt(c.Query("q", "1"), 10, 32)
		if err != nil {
			return fmt.Errorf("Amount parse error: %w", err)
		}
		return handler(c.Params("key"), int32(q), c)
	})

	return app
}

func startHTTP(http *fiber.App, fiberFailed chan<- struct{}, cfg *server.Config) {
	go func() {
		defer func() { //always report termination
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
        	    slog.Error("HTTP server panicked", "error", r)
				fiberFailed <- struct{}{}
        	}
		} ()
			
		//startup errors return non-nil, graceful shutdown returns nil, shutdown errors are only returned via shutdown() - not here
		if err := http.Listen(fmt.Sprintf(":%d", cfg.Port)); err != nil {
			slog.Error("HTTP server startup failed", "error", err)
			fiberFailed <- struct{}{}
		}
	}()
}

/*
type ComplexRequest struct {
    UserID int32  `uri:"id"`        // Extracted from path: /users/:id
    Search string `query:"search"`  // Extracted from query string: ?search=abc
    Role   string `header:"X-Role"` // Extracted from HTTP headers
}
*/