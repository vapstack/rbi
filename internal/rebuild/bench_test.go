package rebuild

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

func rebuildBenchRows() int {
	if testing.Short() {
		return 512
	}
	return 8192
}

func seedRebuildBenchBolt(b *testing.B, bucket []byte, rows int, strKey bool) *bbolt.DB {
	b.Helper()

	db, err := bbolt.Open(filepath.Join(b.TempDir(), "rebuild_bench.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, e := tx.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}
		var key [8]byte
		for i := 1; i <= rows; i++ {
			rec := rebuildTestRec{
				Name:  fmt.Sprintf("user_%06d", i%1024),
				Score: uint64(i * 3),
			}
			switch i & 3 {
			case 0:
				rec.Tags = []string{"go", "db", fmt.Sprintf("tag_%d", i%128)}
			case 1:
				rec.Tags = []string{"go"}
			}
			data, e := msgpack.Marshal(&rec)
			if e != nil {
				return e
			}
			if strKey {
				if e = bkt.Put(keycodec.StringBytes(fmt.Sprintf("key_%08d", i)), data); e != nil {
					return e
				}
			} else if e = bkt.Put(keycodec.U64BytesWithBuf(uint64(i), &key), data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("seed bolt: %v", err)
	}
	return db
}

func compileRebuildBenchSchema(b *testing.B) *schema.Schema {
	b.Helper()

	rt, err := schema.Compile(reflect.TypeOf(rebuildTestRec{}), schema.Config{})
	if err != nil {
		b.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func BenchmarkBuildUint64(b *testing.B) {
	bucket := []byte("bench_uint64")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, false)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, newRebuildTestState(rt))
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
		result.Storage.Release()
	}
}

func BenchmarkBuildStringKeys(b *testing.B) {
	bucket := []byte("bench_string")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, true)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.StrKey = true

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.StrMap = strmap.New(0, 256)
		result, err := Build(cfg, newRebuildTestState(rt))
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
		result.Storage.Release()
	}
}

func BenchmarkBuildPartialSkippedFields(b *testing.B) {
	bucket := []byte("bench_partial")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, false)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)

	base, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		b.Fatalf("base Build: %v", err)
	}

	manager := snapshot.NewRegistry(false)
	prev := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, base.Storage)
	manager.Publish(prev)

	cfg.Current = prev
	cfg.SkipFields = map[string]struct{}{"tags": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, State{
			Index:             prev.Index,
			NilIndex:          prev.NilIndex,
			LenIndex:          prev.LenIndex,
			LenZeroComplement: prev.LenZeroComplement,
			Measure:           prev.Measure,
			Universe:          prev.Universe,
		})
		if err != nil {
			b.Fatalf("partial Build: %v", err)
		}
		next := snapshot.NewView(uint64(i+2), prev, rt, snapshot.CacheConfig{}, result.Storage)
		manager.Publish(next)
		prev = next
		cfg.Current = prev
	}
}
