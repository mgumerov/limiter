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

	//TODO we might want to test if thread-safety measures taken by victoriametrics are causing too much overhead because
	// otherwise we process each request very fast and that overhead might be noticeable. In that case we'll need to maybe
	// collect only each Nth request into histograms (but that would make it blind to most peaks); as for totals, we can 
	// accumulate time via some cheaper means and periodically dump it to metrics.
	var myMetrics = metrics.NewSet()

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
		server = nil
	}
	
	var buf bytes.Buffer
	myMetrics.WritePrometheus(&buf)
	slog.Info("Stopping", "metrics", buf.String())
	
	if server != nil {
		if err := server.Shutdown(); err != nil {
			slog.Error("Error while shutting down request server", "error", err)
		}
	}
	processor.Close()
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
	//Do nothing
	return nil
}
