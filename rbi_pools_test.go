package rbi

import (
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
)

type pooledSparseRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
	Opt  *string  `db:"opt"  rbi:"index"`
}

func (r *pooledSparseRec) MarshalMsgpack() ([]byte, error) {
	m := make(map[string]any, 3)
	if r.Name != "" {
		m["name"] = r.Name
	}
	if len(r.Tags) != 0 {
		m["tags"] = r.Tags
	}
	if r.Opt != nil {
		m["opt"] = *r.Opt
	}
	return msgpack.Marshal(m)
}

func (r *pooledSparseRec) UnmarshalMsgpack(b []byte) error {
	// Intentionally update only fields present in the payload. If decode reuses a
	// dirty pooled struct without zeroing it first, absent fields leak through.
	var tmp struct {
		Name *string   `msgpack:"name"`
		Tags *[]string `msgpack:"tags"`
		Opt  *string   `msgpack:"opt"`
	}
	if err := msgpack.Unmarshal(b, &tmp); err != nil {
		return err
	}
	if tmp.Name != nil {
		r.Name = *tmp.Name
	}
	if tmp.Tags != nil {
		r.Tags = append(r.Tags[:0], *tmp.Tags...)
	}
	if tmp.Opt != nil {
		v := *tmp.Opt
		r.Opt = &v
	}
	return nil
}

func openTempDBUint64PooledSparseRec(t *testing.T, options ...Options) (*DB[uint64, pooledSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_pooled_sparse.db")
	db, raw := openBoltAndNew[uint64, pooledSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func TestRecordPool_ReusedDecodeLeaksAbsentFields(t *testing.T) {
	db, _ := openTempDBUint64PooledSparseRec(t, Options{AutoBatchMax: 1})

	opt := "sticky"
	if err := db.Set(1, &pooledSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &pooledSparseRec{Name: "second"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		db.ReleaseRecords(first)

		second, err := db.Get(2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			db.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		db.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
}
