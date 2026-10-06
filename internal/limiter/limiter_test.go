package limiter

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRefill(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		bucket     Bucket
		elapsed    time.Duration
		wantCount  int32
		wantIssued int64
	}{
		{
			name:       "nothing elapsed adds nothing",
			bucket:     Bucket{Limit: 100, StartedAt: start},
			elapsed:    0,
			wantCount:  0,
			wantIssued: 0,
		},
		{
			name:       "half a second adds half the limit",
			bucket:     Bucket{Limit: 100, StartedAt: start},
			elapsed:    500 * time.Millisecond,
			wantCount:  50,
			wantIssued: 50,
		},
		{
			name:       "only tokens not yet issued are added",
			bucket:     Bucket{Count: 10, Limit: 100, StartedAt: start, Issued: 40},
			elapsed:    500 * time.Millisecond,
			wantCount:  20,
			wantIssued: 50,
		},
		{
			name:       "long idle period does not overflow the bucket",
			bucket:     Bucket{Limit: 100, StartedAt: start},
			elapsed:    time.Hour,
			wantCount:  100,
			wantIssued: 360000,
		},
		{
			name:       "count is capped at the limit",
			bucket:     Bucket{Count: 90, Limit: 100, StartedAt: start},
			elapsed:    500 * time.Millisecond,
			wantCount:  100,
			wantIssued: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.bucket
			Refill(&b, start.Add(tt.elapsed))
			if b.Count != tt.wantCount {
				t.Errorf("Count = %d, want %d", b.Count, tt.wantCount)
			}
			if b.Issued != tt.wantIssued {
				t.Errorf("Issued = %d, want %d", b.Issued, tt.wantIssued)
			}
		})
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	path := writeConfig(t, `
max_requests: 30
api:
  api1: 1000
  api2: 0
etcd:
  endpoints:
    - "localhost:2379"
  dial-timeout: 5s
etcd-key: limiter
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxRequest != 30 {
		t.Errorf("MaxRequest = %d, want 30", cfg.MaxRequest)
	}
	if got := cfg.APIs["api1"]; got != 1000 {
		t.Errorf("APIs[api1] = %d, want 1000", got)
	}
	if _, ok := cfg.APIs["api2"]; !ok {
		t.Errorf("APIs[api2] missing")
	}
	if len(cfg.ETCD.Endpoints) != 1 || cfg.ETCD.Endpoints[0] != "localhost:2379" {
		t.Errorf("ETCD.Endpoints = %v", cfg.ETCD.Endpoints)
	}
	if cfg.ETCDkey != "limiter" {
		t.Errorf("ETCDkey = %q, want limiter", cfg.ETCDkey)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"missing file", filepath.Join(t.TempDir(), "nope.yaml")},
		{"invalid yaml", writeConfig(t, "max_requests: [")},
		{"max_requests not set", writeConfig(t, "api:\n  api1: 10\n")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := LoadConfig(tt.path); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// The repository's own config.yaml must stay loadable, since it is the default CONFIG_PATH.
func TestLoadRepoConfig(t *testing.T) {
	if _, err := LoadConfig("../../config.yaml"); err != nil {
		t.Fatalf("LoadConfig(config.yaml): %v", err)
	}
}

type fixedTracker bool

func (f fixedTracker) IAmMaster() bool { return bool(f) }

type stubProcessor struct {
	calls  int
	closed bool
}

func (s *stubProcessor) Request(key string, amount int32) Response {
	s.calls++
	return Response{Granted: amount}
}

func (s *stubProcessor) Close() { s.closed = true }

func TestGuardedProcessor(t *testing.T) {
	t.Run("master passes requests through", func(t *testing.T) {
		next := &stubProcessor{}
		p := NewGuardedProcessor(next, fixedTracker(true))
		if got := p.Request("api1", 3).Granted; got != 3 {
			t.Errorf("Granted = %d, want 3", got)
		}
		if next.calls != 1 {
			t.Errorf("next called %d times, want 1", next.calls)
		}
	})

	t.Run("non-master grants nothing", func(t *testing.T) {
		next := &stubProcessor{}
		p := NewGuardedProcessor(next, fixedTracker(false))
		if got := p.Request("api1", 3).Granted; got != 0 {
			t.Errorf("Granted = %d, want 0", got)
		}
		if next.calls != 0 {
			t.Errorf("next called %d times, want 0", next.calls)
		}
	})

	t.Run("close is forwarded", func(t *testing.T) {
		next := &stubProcessor{}
		NewGuardedProcessor(next, fixedTracker(true)).Close()
		if !next.closed {
			t.Error("next was not closed")
		}
	})
}

func TestNoOpProcessor(t *testing.T) {
	p := &NoOpProcessor{}
	if got := p.Request("api1", 5).Granted; got != 0 {
		t.Errorf("Granted = %d, want 0", got)
	}
	p.Close()
}
