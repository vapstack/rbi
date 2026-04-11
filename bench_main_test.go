package rbi

import (
	"os"
	"sync"
	"testing"
)

var (
	benchSuiteCleanupMu sync.Mutex
	benchSuiteCleanups  []func()
)

func registerBenchSuiteCleanup(fn func()) {
	if fn == nil {
		return
	}
	benchSuiteCleanupMu.Lock()
	benchSuiteCleanups = append(benchSuiteCleanups, fn)
	benchSuiteCleanupMu.Unlock()
}

func runBenchSuiteCleanups() {
	benchSuiteCleanupMu.Lock()
	cleanups := benchSuiteCleanups
	benchSuiteCleanups = nil
	benchSuiteCleanupMu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	runBenchSuiteCleanups()
	os.Exit(code)
}
