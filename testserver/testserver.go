package testserver

import (
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/form3tech-oss/f1/v2/pkg/f1"
	"github.com/form3tech-oss/f1/v2/pkg/f1/testing"

	"limiter/server"
)

type TestServer struct {
	processor   server.Processor
	handlerTime *metrics.Summary //maybe histogram? also, might introduce extra delays and contention
}

var _ server.Server = (*TestServer)(nil) //fail-fast type guard

type TestResult struct {
	stretch int64
}

func CreateTestServer(processor server.Processor, serverFailed chan<- struct{}, cfg *server.Config, myMetrics *metrics.Set) server.Server {
	var handlerTime = myMetrics.NewSummary("handler_time")
	var granted = myMetrics.NewCounter("granted")
	
	//I tried to update metrics directly, but looks like it involves locking whole Summary to add each measurement, and
	// it becomes a massive source of contention.
	//So instead we accumulate metrics in different buckets and only combine on-demand:
	// - Grant-counter is combined few times a second, to make it useful to monitor via Grafana
	// - Handler-time (being a histogram) is only built when tests complete!
	var timecounts [1 + 0x1f]chan TestResult
	for i := range timecounts {
		timecounts[i] = make(chan TestResult, 10*1000*1000)
	}
	//TODO make them live in different cachelines maybe. But even as it is, tests do not show significant degradation after adding those atomics
	//TODO only necessary when supporting prometheus metrics exposition
	var grantCounts [1 + 0x1f]atomic.Int64

	
	//Because this test-setup function gets called from a goroutine that will be created later (by F1) down the path of this goroutine,
	// that goroutine's launch (go xxx) happens-after what happened before this point on execution path;
	// and this test-setup function also happens-after even that, meaning it obverves fully initialized "processor",
	// so the safe publication requirement is satisfied.
	setupTest := func(t *testing.T) testing.RunFn {
		return func(t *testing.T) {
			var start = time.Now()
			var response = processor.Request("api1", 1)
			stretch := time.Since(start).Nanoseconds()
			//We add to "stretch" just for better distribution - to avoid millions of 0's going into same bucket
			index := (stretch+int64(start.Nanosecond()))&0x1f
			timecounts[index] <- TestResult{stretch: stretch}
			grantCounts[index].Add(int64(response.Granted))
		}
	}

	var stop atomic.Bool

	go func() { //background statistics-gathering for grant-counts only
		var last int64 = 0
		for {
			if stop.Load() {
				break
			}
			var total int64 = 0
			for i := range grantCounts { //unlike using _value_ extracted by "range", access by index per se does not make a copy
				total += grantCounts[i].Load() 
			}
			granted.AddInt64(total - last)
			last = total
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()

	go func() {
		time.Sleep(time.Duration(3) * time.Second) //Do not start at once, because of slow ectd handshake

		if err := f1.New().Add("tests", setupTest).ExecuteWithArgs(os.Args[1:]); err != nil {
			slog.Error("Unable to start F1 tests", "error", err)
			serverFailed <- struct{}{}
		}
		
		slog.Info("Gathering handler statistics")
		//the goroutine started after the slice had been populated, so it observes it
		for i := range timecounts {
		drain:
			for {
				select {
				case result := <-timecounts[i]:
					handlerTime.Update(float64(result.stretch) / float64(time.Second.Nanoseconds()))
				default:
					break drain
				}
			}
		}
		slog.Info("Done")
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
