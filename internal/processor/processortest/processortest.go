// Package processortest holds behavioural checks shared by all limiter.Processor implementations.
package processortest

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/mgumerov/limiter/internal/limiter"
)

// Factory starts a fresh Processor for the given config.
type Factory func(failed chan<- struct{}, cfg *limiter.Config, m *metrics.Set) limiter.Processor

// Run exercises a Processor implementation against the contract every implementation is expected to honour.
func Run(t *testing.T, start Factory) {
	newProcessor := func(t *testing.T, maxRequest int32, limit int32) limiter.Processor {
		t.Helper()
		cfg := &limiter.Config{MaxRequest: maxRequest, APIs: map[string]int32{"api1": limit}}
		p := start(make(chan struct{}, 1), cfg, metrics.NewSet())
		t.Cleanup(p.Close)
		return p
	}

	t.Run("unknown key grants nothing", func(t *testing.T) {
		p := newProcessor(t, 10, 1000)
		time.Sleep(20 * time.Millisecond)
		if got := p.Request("nope", 1).Granted; got != 0 {
			t.Errorf("Granted = %d, want 0", got)
		}
	})

	t.Run("bucket starts empty", func(t *testing.T) {
		// 1 token per second: nothing can be issued right after start.
		p := newProcessor(t, 10, 1)
		if got := p.Request("api1", 1).Granted; got != 0 {
			t.Errorf("Granted = %d, want 0", got)
		}
	})

	t.Run("refilled tokens are granted", func(t *testing.T) {
		p := newProcessor(t, 10, 1000)
		time.Sleep(50 * time.Millisecond) // ~50 tokens
		if got := p.Request("api1", 5).Granted; got != 5 {
			t.Errorf("Granted = %d, want 5", got)
		}
	})

	t.Run("amount is capped at max_requests", func(t *testing.T) {
		p := newProcessor(t, 3, 1000)
		time.Sleep(50 * time.Millisecond)
		if got := p.Request("api1", 20).Granted; got != 3 {
			t.Errorf("Granted = %d, want 3", got)
		}
	})

	t.Run("never grants more than refilled", func(t *testing.T) {
		const limit = 1000
		p := newProcessor(t, 1_000_000, limit)
		started := time.Now()
		time.Sleep(30 * time.Millisecond)
		got := p.Request("api1", 1_000_000).Granted
		upper := int32(float64(limit)*time.Since(started).Seconds()) + 1
		if got <= 0 || got > upper {
			t.Errorf("Granted = %d, want 1..%d", got, upper)
		}
	})

	t.Run("concurrent requests stay within the rate", func(t *testing.T) {
		const limit = 2000
		p := newProcessor(t, 10, limit)
		started := time.Now()
		deadline := started.Add(150 * time.Millisecond)

		var total atomic.Int64
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					total.Add(int64(p.Request("api1", 1).Granted))
				}
			}()
		}
		wg.Wait()

		upper := int64(float64(limit)*time.Since(started).Seconds()) + 1
		if got := total.Load(); got <= 0 || got > upper {
			t.Errorf("granted %d in total, want 1..%d", got, upper)
		}
	})
}
