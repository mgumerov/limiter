package casproc

import (
	"testing"

	"github.com/VictoriaMetrics/metrics"

	"github.com/mgumerov/limiter/internal/limiter"
	"github.com/mgumerov/limiter/internal/processor/processortest"
)

func TestCasProcessor(t *testing.T) {
	processortest.Run(t, func(failed chan<- struct{}, cfg *limiter.Config, m *metrics.Set) limiter.Processor {
		return StartCasProcessor(failed, cfg, m, 3)
	})
}
