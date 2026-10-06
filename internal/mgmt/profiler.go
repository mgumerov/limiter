package mgmt

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

type Profiling struct {
	file *os.File
	bfile *os.File
	filename string
}

func startProfiling(filename string) (*Profiling, error) {
	f, err := os.Create(filename+"-cpu.prof")
	if err != nil {
		return nil, fmt.Errorf("failed to create file for CPU profiling: %w", err)
	}
	f2, err := os.Create(filename+"-block.prof")
	if err != nil {
		return nil, fmt.Errorf("failed to create file for block profiling: %w", err)
	}
	runtime.SetBlockProfileRate(1)
	if err = pprof.StartCPUProfile(f); err != nil {
		return nil, fmt.Errorf("failed to start CPU profiling: %w", err)
	}
	return &Profiling{ file: f, bfile: f2, filename: filename }, nil
}

func stopProfiling(p *Profiling) {
	pprof.StopCPUProfile()
	_ = p.file.Close()
	runtime.SetBlockProfileRate(0)
	if bp := pprof.Lookup("block"); bp != nil {
		_ = bp.WriteTo(p.bfile, 0)
	}
	_ = p.bfile.Close()
}
