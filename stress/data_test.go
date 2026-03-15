package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestOpenBenchDBFailsFastOnLockedDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.db")
	raw, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Fatalf("raw close: %v", closeErr)
		}
	}()

	_, err = OpenBenchDB(DBConfig{
		DBFile:      path,
		OpenTimeout: 25 * time.Millisecond,
	}, 0)
	if err == nil {
		t.Fatalf("OpenBenchDB unexpectedly succeeded on locked db")
	}
	if !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("OpenBenchDB error = %q, want lock timeout", err)
	}
}
