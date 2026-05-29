package rbi

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/rebuild"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}, skipMeasureFields map[string]struct{}) error {
	seq, err := currentBucketSequence(db.bolt, db.bucket)
	if err != nil {
		return err
	}
	qe := db.engine
	result, err := rebuild.Build(rebuild.Config{
		Bolt:              db.bolt,
		Bucket:            db.bucket,
		Schema:            qe.schema,
		Current:           qe.snapshot.Current(),
		StrKey:            db.strKey,
		StrMap:            db.strMap,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasureFields,
		Decode:            db.decodeBuildIndexRecord,
		Release:           db.releaseBuildIndexRecord,
	}, rebuild.State{
		Index:             qe.index,
		NilIndex:          qe.nilIndex,
		LenIndex:          qe.lenIndex,
		LenZeroComplement: qe.lenZeroComplement,
		Measure:           qe.measure,
		Universe:          qe.universe,
		LenLoaded:         qe.lenIndexLoaded,
	})
	if err != nil {
		return err
	}
	if result.Publish {
		st := result.Storage
		result.Storage = snapshot.Storage{}
		qe.publishStorageSnapshotNoLock(seq, db.strMap, st)
	}
	qe.lenIndexLoaded = result.LenLoaded
	if result.Stats {
		db.stats.BuildTime = result.BuildTime
		db.stats.BuildRPS = result.BuildRPS
	}
	return nil
}

func (db *DB[K, V]) decodeBuildIndexRecord(data []byte) (unsafe.Pointer, error) {
	val, err := db.decode(data)
	if err != nil {
		return nil, err
	}
	return unsafe.Pointer(val), nil
}

func (db *DB[K, V]) releaseBuildIndexRecord(ptr unsafe.Pointer) {
	var zero V
	val := (*V)(ptr)
	*val = zero
	db.recPool.Put(val)
}

func (db *DB[K, V]) validateIndexedStringValues(val *V) error {
	if db.engine == nil || val == nil {
		return nil
	}
	ptr := unsafe.Pointer(val)
	for _, acc := range db.engine.schema.StringValidation {
		var fieldErr error
		acc.WriteBuild(ptr, schema.BuildSink{
			Field: acc.Name,
			Err:   &fieldErr,
		})
		if fieldErr != nil {
			return fieldErr
		}
	}
	return nil
}

func (db *DB[K, V]) isLenZeroComplementField(field string) bool {
	if field == "" {
		return false
	}
	acc, ok := db.engine.schema.IndexedByName[field]
	if !ok || acc.Ordinal >= len(db.engine.lenZeroComplement) {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.engine.lenZeroComplement[acc.Ordinal]
}

const (
	nilIndexEntryKey = indexdata.NilIndexEntryKey
)

func (db *DB[K, V]) loadIndex() (
	skipFields map[string]struct{},
	skipMeasureFields map[string]struct{},
	plannerStats *qexec.PlannerStatsSnapshot,
	err error,
) {
	currentSeq, err := currentBucketSequence(db.bolt, db.bucket)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	start := time.Now()
	result, err := persist.Load(persist.LoadConfig{
		File:            db.rbiFile,
		DBPath:          db.bolt.Path(),
		Bucket:          db.bucket,
		CurrentSeq:      currentSeq,
		Schema:          db.schema,
		StrMapCompactAt: defaultSnapshotStrMapCompactDepth,
		Errors: persist.Errors{
			Stale:   errPersistedIndexStale,
			Invalid: errPersistedIndexInvalid,
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	installed := false
	defer func() {
		if !installed {
			result.Storage.Release()
		}
	}()

	qe := db.engine
	indexdata.ReleaseFieldStorageSlots(qe.index)
	indexdata.ReleaseFieldStorageSlots(qe.nilIndex)
	indexdata.ReleaseFieldStorageSlots(qe.lenIndex)
	indexdata.ReleaseMeasureStorageSlots(qe.measure)
	if qe.lenZeroComplement != nil {
		pooled.ReleaseBoolSlice(qe.lenZeroComplement)
	}
	qe.universe.Release()
	qe.lenIndexLoaded = result.LenLoaded

	publishStrMap := result.StrMap
	if !db.strKey {
		publishStrMap = nil
	}
	st := result.Storage
	result.Storage = snapshot.Storage{}
	qe.publishStorageSnapshotNoLock(currentSeq, publishStrMap, st)
	installed = true

	if db.strKey {
		db.strMap = result.StrMap
	}
	db.stats.LoadTime = time.Since(start)

	return result.SkipFields, result.SkipMeasureFields, result.PlannerStats, nil
}

func (db *DB[K, V]) storeIndex() error {
	if db.testHooks != nil {
		if hook := db.testHooks.beforeStoreIndex; hook != nil {
			if err := hook(); err != nil {
				return err
			}
		}
	}

	return db.engine.storeIndex(db.rbiFile, db.bolt, db.bucket)
}

func (qe *queryEngine) storeIndex(rbiFile string, bolt *bbolt.DB, bucket []byte) error {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return fmt.Errorf("store: reading bucket sequence: %w", err)
	}
	snap, snapSeq, ref := qe.snapshot.PinCurrent()
	defer qe.snapshot.Unpin(snapSeq, ref)

	if snap.Seq != seq {
		return fmt.Errorf("store: snapshot sequence mismatch: snapshot=%d bucket=%d", snap.Seq, seq)
	}
	statsVersion := qe.exec.StatsVersion.Load()
	if statsVersion == 0 {
		statsVersion = 1
	} else {
		statsVersion++
	}
	plannerStats := qe.exec.PlannerStatsSnapshotForPersist(snap, statsVersion)
	return persist.Store(persist.StoreConfig{
		File:         rbiFile,
		BucketSeq:    seq,
		Schema:       qe.schema,
		Snapshot:     snap,
		PlannerStats: plannerStats,
	})
}

func countDistinct(s []string) int {
	n := len(s)
	switch n {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		if s[0] != s[1] {
			return 2
		}
		return 1
	case 3:
		a, b, c := s[0], s[1], s[2]
		if a == b {
			if b == c {
				return 1
			}
			return 2
		}
		if a == c || b == c {
			return 2
		}
		return 3
	}
	if n <= 8 {
		return countDistinctLinear(s, n)
	}
	return len(dedupStringsInplace(s))
}

func countDistinctLinear(s []string, n int) int {
	uniq := 0
OUTER:
	for i := 0; i < n; i++ {
		v := s[i]
		for k := 0; k < i; k++ {
			if s[k] == v {
				continue OUTER
			}
		}
		uniq++
	}
	return uniq
}
