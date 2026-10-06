package mgmt

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func StartManagementServer(failed chan<- struct{}, port string, metrics *metrics.Set) *Mgmt {
	prom := &Mgmt { httpServer: nil, m: &sync.Mutex{} }
	if port != "0" {
		mux := http.NewServeMux()

		mux.Handle("/metrics", http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
			metrics.WritePrometheus(w)
		}))

		//Yes I do realize risks involved in opening a file indicated by the caller;
		// however this manamenent server is only intended to be used in debugging
		// and benchmarks, so I'm not going to make this any more secure.
		mux.Handle("POST /prof/start/{filename}", http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
			if err := prom.startProfiling(r.PathValue("filename")); err != nil {
				w.WriteHeader(500)
				_,_ = w.Write([]byte(err.Error()))
			} 
		}))

		mux.Handle("POST /prof/stop", http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
			session, err := prom.stopProfiling();
			if err != nil {
				w.WriteHeader(500)
				_,_ = w.Write([]byte(err.Error()))
			} else {
				_,_ = w.Write([]byte(session.filename)) //todo download
			}
		}))

		slog.Info("Starting management endpoint server (use MPORT=0 to disable endpoint)", "Port", port)
		prom.httpServer = &http.Server{
			Addr: ":" + port,
			Handler: mux,
		}

		go func() {
			if err := prom.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed  {
				slog.Error("Failed to set up management http server", "Error", err)
				failed <- struct{}{}
			}
		}()
	} else {
		slog.Warn("Running without management endpoint")
	}

	return prom
}

type Mgmt struct {
	httpServer *http.Server
	prof *Profiling
	m *sync.Mutex
}

func (p *Mgmt) startProfiling(filename string) error {
	var err error
	p.m.Lock()
	defer p.m.Unlock()
	if p.prof != nil {
		err = errors.New("the profiling session is already started")
	} else {
		p.prof, err = startProfiling(filename)
	}
	return err
}

func (p *Mgmt) stopProfiling() (*Profiling, error) {
	p.m.Lock()
	if p.prof == nil {
		return nil, errors.New("no active profiling session")
	}
	defer p.m.Unlock()
	stopProfiling(p.prof)
	prof := p.prof
	p.prof = nil
	return prof, nil
}

func (p *Mgmt) Shutdown() {
	if p.httpServer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3) * time.Second)
	defer cancel()
	if err := p.httpServer.Shutdown(ctx); err != nil {
		slog.Error("error while shutting down managements endpoint", "Error", err)
	}
}
