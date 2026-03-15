package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
)

type profilerStopFn func() error

func startStressProfiling(opts options) (profilerStopFn, error) {
	if opts.PprofHTTP != "" {
		addr := opts.PprofHTTP
		go func() {
			log.Printf("pprof HTTP server listening on %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				log.Printf("pprof HTTP server error: %v", err)
			}
		}()
	}

	var cpuFile *os.File
	if opts.CPUProfile != "" {
		f, err := os.Create(opts.CPUProfile)
		if err != nil {
			return nil, fmt.Errorf("create cpu profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
		cpuFile = f
		log.Printf("CPU profiling enabled: %s", opts.CPUProfile)
	}

	return func() error {
		var firstErr error

		if cpuFile != nil {
			pprof.StopCPUProfile()
			if err := cpuFile.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close cpu profile: %w", err)
			}
		}

		if opts.HeapProfile != "" {
			runtime.GC()
			f, err := os.Create(opts.HeapProfile)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("create heap profile: %w", err)
				}
			} else {
				if err := pprof.WriteHeapProfile(f); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("write heap profile: %w", err)
				}
				if err := f.Close(); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("close heap profile: %w", err)
				}
				log.Printf("Heap profiling snapshot saved: %s", opts.HeapProfile)
			}
		}

		return firstErr
	}, nil
}
