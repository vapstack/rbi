//go:build !windows

package persist

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

func TestStoreCreatesSidecar0600(t *testing.T) {
	oldUmask := syscall.Umask(0o022)
	defer syscall.Umask(oldUmask)

	file := filepath.Join(t.TempDir(), "sidecar.rbi")
	if err := Store(StoreConfig{
		File:      file,
		BucketSeq: 1,
		Schema:    &schema.Schema{},
		Snapshot:  &snapshot.View{},
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	info, err := os.Stat(file)
	if err != nil {
		t.Fatalf("stat sidecar: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("sidecar mode=%#o, want 0600", got)
	}
}
