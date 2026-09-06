package testserver

import (
	"log/slog"
	"os"
	"time"
	
	"github.com/VictoriaMetrics/metrics"
	"github.com/form3tech-oss/f1/v2/pkg/f1"
	"github.com/form3tech-oss/f1/v2/pkg/f1/testing"

	"limiter/server"
)

type TestServer struct {
	processor server.Processor
	handlerTime *metrics.Summary //maybe histogram? also, might introduce extra delays and contention
}
var _ server.Server = (*TestServer)(nil) //fail-fast type guard

func CreateTestServer(processor server.Processor, serverFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) server.Server {

	var handlerTime = myMetrics.NewSummary("handler_time")

	setupTest := func (t *testing.T) testing.RunFn {
		return func(t *testing.T) {
			var start = time.Now()
			//TODO and don't forget to measure time spent in handler (possibly waiting when calling processor.Request)
			processor.Request("api1", 1)
			handlerTime.UpdateDuration(start)
		}
	}
	
	go func() {
		if err := f1.New().Add("tests", setupTest).ExecuteWithArgs(os.Args[1:]); err != nil {
			slog.Error("Unable to start F1 tests", "error", err)
			serverFailed <- struct{}{}
		}
	}()

	slog.Info("Started TEST request server")
	return &TestServer{}
}

func (s *TestServer) Shutdown() error {
	//I don't insert test interruption here to avoid overcomplicating, but actually F1 listens for normal termination signals,
	// so, doing anything here is only bad if Shutdown is called due to some other reason, like some part of app failed to start.
	// Anyway, this "server" is just for benchmarking.
	return nil
}

