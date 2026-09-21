package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/VictoriaMetrics/metrics"
	"gopkg.in/yaml.v3"

	"limiter/casproc"
	"limiter/consensus"
	"limiter/fiberserver"
	"limiter/mutexproc"
	"limiter/server"
	"limiter/testserver"
	"limiter/workerproc"
)

func main() {
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		var logLevel slog.Level
		if err := logLevel.UnmarshalText([]byte(level)); err != nil {
			slog.Warn("Invalid log config specified, ignoring")
		} else {
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{ Level: logLevel })))
		}
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		slog.Error("failed to load configuration", "path", configPath, "error", err)
		os.Exit(1) //TODO
	}

	var myMetrics = metrics.NewSet()

	nctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM, //sadly no platform-neutral os.* constant for this; even though actually Go translates to SIGTERM on Windows
	)
	defer stop() //we won't need this context after this function completes

	//Create nested context so that we could cancel it even when shutdown is not initiated by a signal
	ctx, cancel  := context.WithCancel(nctx)
	defer cancel()
		
	trackerFailed := make(chan struct{}, 1)
	tracker := consensus.StartMasterLeaseLoop(ctx, trackerFailed, cfg)

	processorFailed := make(chan struct{}, 1)
	var processor server.Processor
	switch v := os.Getenv("P"); v {
	case "W": processor = workerproc.StartWorkerProcessor(processorFailed, cfg, myMetrics)
	case "M": processor = mutexproc.StartMutexProcessor(processorFailed, cfg, myMetrics)
	case "C": {
		r := os.Getenv("R")
		retries, err := strconv.Atoi(r)
		if err != nil {
			slog.Error("Invalid retries count specified in env", "R", r)
			processor = &NoOpProcessor{} //still create a stub to be safely passed as dependency
			processorFailed <- struct{}{}
		}
		processor = casproc.StartCasProcessor(processorFailed, cfg, myMetrics, retries)
	}
	case "N": processor = &NoOpProcessor{}
	default:
		slog.Error("Invalid request processor specified in env", "P", v)
		processor = &NoOpProcessor{} //still create a stub to be safely passed as dependency
		processorFailed <- struct{}{}
	}

	//Rather than make each Processor verify holding master's mantle, we decorate whatever Processor has been constucted 
	// with orthogonal middleware (cross-cutting concern) for verification, leaving Processor and consensus-tracker uncoupled.
	processor = &GuardedProcessor { next: processor, tracker: tracker }
	
	//Idiomatic approach says passing contexts along (here, or/and to fiber.Listen) because this clearly involves some 
	// i/o and long activity. But actually it depends on how the called func will use the context: maybe I am passing 
	// request-scoped context to some async processing, for example. In this case, passing globally-scoped notify-context 
	// actually makes sense, but only if I want Fiber to use it for graceful shutdowns, and I stated above that I prefer it not to.
	serverFailed := make(chan struct{}, 1)
	var server server.Server
	switch v := os.Getenv("S"); v {
	case "F": server = fiberserver.CreateFiberServer(processor, serverFailed, cfg, myMetrics)
	case "T": server = testserver.CreateTestServer(processor, serverFailed, cfg, myMetrics)
	default:
		slog.Error("Invalid request server specified in env", "S", v)
		server = &NoOpServer{} //still create a stub to be safely passed as dependency
		serverFailed <- struct{}{}
	}

	select {
	case <- ctx.Done():
		//do nothing
	case <- serverFailed:
		slog.Error("HTTP server terminated unexpectedly") //since we did not tell it to shut down yet
		server = nil
	case <- processorFailed:
		slog.Error("Request processor terminated unexpectedly") //since we did not tell it to shut down yet
		processor = nil
	case <- trackerFailed:
		slog.Error("Consensus-tracking engine failed")
		tracker = nil
	}

	var buf bytes.Buffer
	myMetrics.WritePrometheus(&buf)
	slog.Info("Stopping", "metrics", buf.String())

	// If the outer (notifying-context) had been cancelled, this will do nothing but won't hurt; 
	//  if however it had not been cancelled (i.e. we got here because of serverFailed etc.), cancel the nested context.
	cancel()
	
	// The cancellation of context will stop all our subsystems that are watching that context,
	//  but others we have to stop explicitly.
	if server != nil {
		if err := server.Shutdown(); err != nil {
			slog.Error("Error while shutting down request server", "error", err)
		}
	}
	if processor != nil {
		processor.Close()
	}

}

func LoadConfig(path string) (*server.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg server.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	//This is better be decoupled from reading because we could load config from different sources. For now, we don't.
	if cfg.MaxRequest == 0 {
		return nil, fmt.Errorf("Maximum request size is not defined")
	}
	if cfg.Port == 0 {
		return nil, fmt.Errorf("Port is not defined")
	}

	return &cfg, nil
}

type NoOpProcessor struct {
}

func (p *NoOpProcessor) Request(key string, amount int32) server.Response {
	return server.Response { Granted: 0 }
}

func (p *NoOpProcessor) Close() {
	//Do nothing
}

type NoOpServer struct {
}

func (p *NoOpServer) Shutdown() error {
	return nil
}

type GuardedProcessor struct {
	next server.Processor
	tracker server.ConsensusTracker
}

func (p *GuardedProcessor) Request(key string, amount int32) server.Response {
	if !p.tracker.IAmMaster() {
		//TODO think of some way of signaling that the client is using the wrong instance of server and it's not normal 429
		// maybe some HTTP response codes are already well suited for that
		return server.Response { Granted: 0 }
	}
	return p.next.Request(key, amount)
}

func (p *GuardedProcessor) Close() {
	p.next.Close()
}