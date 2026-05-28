package rbi

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

func cleanupBuildIndexFailure(buildOK *bool, fieldStates []*schema.BuildFieldState, localUniverse []posting.List) {
	if *buildOK {
		return
	}
	for i := range fieldStates {
		fieldStates[i].Release()
	}
	for i := range localUniverse {
		localUniverse[i].Release()
	}
}

func releaseBuildIndexLocalStates(localStates []schema.BuildFieldLocalState) {
	for i := range localStates {
		localStates[i].Release()
	}
}

func formatDiagnosticBytesPrefix(b []byte, limit int) string {
	if len(b) == 0 {
		return "empty"
	}
	if limit <= 0 || len(b) <= limit {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%x...(len=%d)", b[:limit], len(b))
}

type buildIndexDecodeFn func([]byte) (unsafe.Pointer, error)

type buildIndexReleaseFn func(unsafe.Pointer)

type buildIndexResult struct {
	stats     bool
	buildTime time.Duration
	buildRPS  int
}

func formatBuildIndexKeyDiagnostic(strKey bool, key []byte) string {
	if strKey {
		s := string(key)
		if len(s) > 64 {
			return fmt.Sprintf("id=%q...(len=%d key_prefix_hex=%s)", s[:64], len(s), formatDiagnosticBytesPrefix(key, 24))
		}
		return fmt.Sprintf("id=%q", s)
	}
	if len(key) == 8 {
		return fmt.Sprintf("id=%d", keycodec.U64FromBytes(key))
	}
	return fmt.Sprintf("key_len=%d key_prefix_hex=%s", len(key), formatDiagnosticBytesPrefix(key, 24))
}

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}, skipMeasureFields map[string]struct{}) error {
	result, err := db.engine.buildIndex(
		db.bolt,
		db.bucket,
		db.strKey,
		db.strMap,
		skipFields,
		skipMeasureFields,
		db.decodeBuildIndexRecord,
		db.releaseBuildIndexRecord,
	)
	if err != nil {
		return err
	}
	if result.stats {
		db.stats.BuildTime = result.buildTime
		db.stats.BuildRPS = result.buildRPS
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

func (qe *queryEngine) buildIndex(
	bolt *bbolt.DB,
	bucket []byte,
	strKey bool,
	strMap *strmap.Mapper,
	skipFields map[string]struct{},
	skipMeasureFields map[string]struct{},
	decode buildIndexDecodeFn,
	release buildIndexReleaseFn,
) (buildIndexResult, error) {
	if skipFields == nil {
		skipFields = map[string]struct{}{}
	}
	if skipMeasureFields == nil {
		skipMeasureFields = map[string]struct{}{}
	}

	type buildField struct {
		acc     schema.IndexedFieldAccessor
		slice   bool
		numeric bool
	}

	active := make([]buildField, 0, len(qe.schema.Indexed))
	for _, acc := range qe.schema.Indexed {
		if _, skip := skipFields[acc.Name]; skip {
			continue
		}
		active = append(active, buildField{
			acc:     acc,
			slice:   acc.Field.Slice,
			numeric: acc.Field.KeyKind == schema.FieldWriteKeysOrderedU64,
		})
	}
	activeMeasures := make([]schema.MeasureFieldAccessor, 0, len(qe.schema.Measures))
	for _, acc := range qe.schema.Measures {
		if _, skip := skipMeasureFields[acc.Name]; skip {
			continue
		}
		activeMeasures = append(activeMeasures, acc)
	}

	fcnt := len(active)
	mcnt := len(activeMeasures)
	if fcnt <= 0 && mcnt <= 0 {
		if !qe.lenIndexLoaded {
			seq, err := currentBucketSequence(bolt, bucket)
			if err != nil {
				return buildIndexResult{}, err
			}
			nextLenIndex, nextLenZeroComplement := qe.buildLenIndexStorage(qe.index, qe.universe)
			qe.publishStorageSnapshotNoLock(seq, strMap, snapshot.Storage{
				Index:             indexdata.CloneFieldStorageSlots(qe.index, len(qe.schema.Indexed)),
				NilIndex:          indexdata.CloneFieldStorageSlots(qe.nilIndex, len(qe.schema.Indexed)),
				LenIndex:          nextLenIndex,
				LenZeroComplement: nextLenZeroComplement,
				Measure:           indexdata.CloneMeasureStorageSlots(qe.measure, len(qe.schema.Measures)),
				Universe:          qe.universe,
			})
		}
		qe.lenIndexLoaded = false
		return buildIndexResult{}, nil
	}

	start := time.Now()

	type rawdata struct {
		v   []byte
		key []byte
		idx uint64
		pos uint64
	}

	fieldStates := make([]*schema.BuildFieldState, len(active))
	for i := range active {
		fieldStates[i] = schema.NewBuildFieldState(active[i].slice)
	}

	jobs := make(chan rawdata, 10000)

	workers := runtime.NumCPU()
	workerErrs := make([]error, workers)

	localUniverse := make([]posting.List, workers)
	localMeasureStates := make([][][]indexdata.MeasureEntry, workers)
	buildOK := false

	defer cleanupBuildIndexFailure(&buildOK, fieldStates, localUniverse)
	defer indexdata.CleanupBuildMeasureStates(&buildOK, localMeasureStates)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()

			lu := localUniverse[widx]
			lu.Release()
			lu = posting.List{}
			localStates := make([]schema.BuildFieldLocalState, len(active))
			for i := range active {
				localStates[i] = schema.NewBuildFieldLocalState(active[i].numeric, active[i].slice)
			}
			defer releaseBuildIndexLocalStates(localStates)
			localMeasures := make([][]indexdata.MeasureEntry, len(qe.schema.Measures))
			measureOK := false
			defer func() {
				if !measureOK {
					indexdata.ReleaseMeasureEntryBufs(localMeasures)
				}
			}()

			for kv := range jobs {
				ptr, err := decode(kv.v)
				if err != nil {
					localUniverse[widx] = lu
					workerErrs[widx] = fmt.Errorf(
						"worker=%d stage=decode scan_pos=%d %s idx=%d value_len=%d value_prefix_hex=%s: %w",
						widx,
						kv.pos,
						formatBuildIndexKeyDiagnostic(strKey, kv.key),
						kv.idx,
						len(kv.v),
						formatDiagnosticBytesPrefix(kv.v, 32),
						err,
					)
					cancel()
					return
				}
				idx := kv.idx

				lu = lu.BuildAdded(idx)

				for k := range active {
					var fieldErr error
					active[k].acc.WriteBuild(ptr, schema.BuildSink{
						State: &localStates[k],
						Idx:   idx,
						Field: active[k].acc.Name,
						Err:   &fieldErr,
					})
					if fieldErr != nil {
						release(ptr)
						localUniverse[widx] = lu
						workerErrs[widx] = fmt.Errorf(
							"worker=%d stage=index scan_pos=%d %s idx=%d field=%q: %w",
							widx,
							kv.pos,
							formatBuildIndexKeyDiagnostic(strKey, kv.key),
							kv.idx,
							active[k].acc.Name,
							fieldErr,
						)
						cancel()
						return
					}
					if localStates[k].ShouldFlushRegular() {
						localStates[k].FlushRegularInto(fieldStates[k])
					}
				}
				for _, acc := range activeMeasures {
					if value, ok := acc.Read(ptr); ok {
						buf := localMeasures[acc.Ordinal]
						if buf == nil {
							buf = indexdata.GetMeasureEntrySlice(0)
							localMeasures[acc.Ordinal] = buf
						}
						buf = append(buf, indexdata.MeasureEntry{ID: idx, Value: value})
						localMeasures[acc.Ordinal] = buf
					}
				}
				release(ptr)
			}
			for i := range localStates {
				localStates[i].FlushAllInto(fieldStates[i])
			}
			localUniverse[widx] = lu
			localMeasureStates[widx] = localMeasures
			measureOK = true
		}(i)
	}

	err := bolt.View(func(tx *bbolt.Tx) error {

		defer wg.Wait()
		defer close(jobs)

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		done := ctx.Done()
		c := b.Cursor()
		var strWriter strmap.Writer
		if strKey {
			strWriter = strMap.LockWriter()
			defer strWriter.Unlock()
		}
		nextGCAt := uint64(indexBuildGCStride)
		nextReleaseAt := uint64(indexBuildReleaseOSMemoryStride)
		scanned := uint64(0)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !strKey && len(k) != 8 {
				return fmt.Errorf(
					"invalid uint64 key size scan_pos=%d key_len=%d key_prefix_hex=%s",
					scanned+1,
					len(k),
					formatDiagnosticBytesPrefix(k, 24),
				)
			}
			var idx uint64
			if strKey {
				idx, _ = strWriter.Create(string(k))
			} else {
				idx = keycodec.U64FromBytes(k)
			}
			select {
			case <-done:
				return nil
			case jobs <- rawdata{v: v, key: k, idx: idx, pos: scanned + 1}:
			}
			scanned++
			if scanned >= nextReleaseAt {
				forceMemoryCleanup(true)
				nextReleaseAt += indexBuildReleaseOSMemoryStride
				nextGCAt = scanned + indexBuildGCStride
			} else if scanned >= nextGCAt {
				forceMemoryCleanup(false)
				nextGCAt += indexBuildGCStride
			}
		}
		return nil
	})

	if err != nil {
		return buildIndexResult{}, fmt.Errorf("scan error: db=%q bucket=%q: %w", bolt.Path(), string(bucket), err)
	}
	for _, err = range workerErrs {
		if err != nil {
			return buildIndexResult{}, fmt.Errorf("scan error: db=%q bucket=%q: %w", bolt.Path(), string(bucket), err)
		}
	}

	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return buildIndexResult{}, err
	}

	slotCount := len(qe.schema.Indexed)
	firstFullBuild := qe.snapshot.Current() == nil && len(skipFields) == 0 && len(skipMeasureFields) == 0
	var nextIndex []indexdata.FieldStorage
	if firstFullBuild {
		for i := range qe.index {
			qe.index[i].Release()
			qe.index[i] = indexdata.FieldStorage{}
		}
		nextIndex = qe.index
	} else if len(skipFields) == 0 {
		nextIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	} else {
		nextIndex = indexdata.CloneFieldStorageSlots(qe.index, slotCount)
	}
	var nextNilIndex []indexdata.FieldStorage
	if firstFullBuild {
		for i := range qe.nilIndex {
			qe.nilIndex[i].Release()
			qe.nilIndex[i] = indexdata.FieldStorage{}
		}
		nextNilIndex = qe.nilIndex
	} else if len(skipFields) == 0 {
		nextNilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	} else {
		nextNilIndex = indexdata.CloneFieldStorageSlots(qe.nilIndex, slotCount)
	}
	var nextLenIndex []indexdata.FieldStorage
	var nextLenZeroComplement []bool
	if firstFullBuild {
		for i := range qe.lenIndex {
			qe.lenIndex[i].Release()
			qe.lenIndex[i] = indexdata.FieldStorage{}
		}
		nextLenIndex = qe.lenIndex
		nextLenZeroComplement = qe.lenZeroComplement
		clear(nextLenZeroComplement)
	} else if len(skipFields) == 0 {
		nextLenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
		nextLenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(nextLenZeroComplement)
	} else {
		nextLenIndex = indexdata.CloneFieldStorageSlots(qe.lenIndex, slotCount)
		nextLenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(nextLenZeroComplement)
		copy(nextLenZeroComplement, qe.lenZeroComplement[:min(slotCount, len(qe.lenZeroComplement))])
	}
	measureSlotCount := len(qe.schema.Measures)
	var nextMeasure []indexdata.MeasureStorage
	if firstFullBuild {
		for i := range qe.measure {
			qe.measure[i].Release()
			qe.measure[i] = indexdata.MeasureStorage{}
		}
		nextMeasure = qe.measure
		qe.universe.Release()
		qe.universe = posting.List{}
	} else if len(skipMeasureFields) == 0 {
		nextMeasure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	} else {
		nextMeasure = indexdata.CloneMeasureStorageSlots(qe.measure, measureSlotCount)
	}

	nextUniverse := posting.List{}
	for i := range localUniverse {
		if localUniverse[i].IsEmpty() {
			continue
		}
		nextUniverse = nextUniverse.BuildMergedOwned(localUniverse[i])
		localUniverse[i] = posting.List{}
	}

	for i := range fieldStates {
		ordinal := active[i].acc.Ordinal
		if storage := fieldStates[i].MaterializeStorage(); storage.KeyCount() > 0 {
			nextIndex[ordinal] = storage
		} else {
			nextIndex[ordinal] = indexdata.FieldStorage{}
		}
		if storage := fieldStates[i].MaterializeNilStorage(); storage.KeyCount() > 0 {
			nextNilIndex[ordinal] = storage
		} else {
			nextNilIndex[ordinal] = indexdata.FieldStorage{}
		}
		if storage, useZeroComplement := fieldStates[i].MaterializeLenStorage(nextUniverse); active[i].slice {
			if storage.KeyCount() > 0 {
				nextLenIndex[ordinal] = storage
			} else {
				nextLenIndex[ordinal] = indexdata.FieldStorage{}
			}
			nextLenZeroComplement[ordinal] = false
			if useZeroComplement {
				nextLenZeroComplement[ordinal] = true
			}
		}
	}
	if len(activeMeasures) > 0 {
		for _, acc := range activeMeasures {
			i := acc.Ordinal
			entries := indexdata.GetMeasureEntrySlice(0)
			for worker := range localMeasureStates {
				if i >= len(localMeasureStates[worker]) {
					continue
				}
				buf := localMeasureStates[worker][i]
				if buf == nil {
					continue
				}
				for entryPos := 0; entryPos < len(buf); entryPos++ {
					entries = append(entries, buf[entryPos])
				}
				indexdata.ReleaseMeasureEntrySlice(buf)
				localMeasureStates[worker][i] = nil
			}
			storage := indexdata.NewMeasureStorageFromEntriesOwned(entries)
			nextMeasure[i] = storage
		}
	}

	recordCount := nextUniverse.Cardinality()
	buildTime := time.Since(start)
	buildRPS := int(float64(recordCount) / max(buildTime.Seconds(), 1))

	for name, f := range qe.schema.Fields {
		if f.Slice {
			acc, ok := qe.schema.IndexedByName[name]
			if !ok || nextLenIndex[acc.Ordinal].KeyCount() == 0 {
				for i := range active {
					if active[i].slice {
						nextLenIndex[active[i].acc.Ordinal].Release()
					}
				}
				indexdata.ReleaseFieldStorageSlice(nextLenIndex)
				pooled.ReleaseBoolSlice(nextLenZeroComplement)
				nextLenIndex, nextLenZeroComplement = qe.buildLenIndexStorage(nextIndex, nextUniverse)
				break
			}
		}
	}
	qe.lenIndexLoaded = false
	qe.publishStorageSnapshotNoLock(seq, strMap, snapshot.Storage{
		Index:             nextIndex,
		NilIndex:          nextNilIndex,
		LenIndex:          nextLenIndex,
		LenZeroComplement: nextLenZeroComplement,
		Measure:           nextMeasure,
		Universe:          nextUniverse,
	})

	active = nil
	fieldStates = nil
	localUniverse = nil
	workerErrs = nil
	localMeasureStates = nil
	forceMemoryCleanup(true)
	buildOK = true

	return buildIndexResult{
		stats:     true,
		buildTime: buildTime,
		buildRPS:  buildRPS,
	}, nil
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

func (qe *queryEngine) buildLenIndexStorage(index []indexdata.FieldStorage, universe posting.List) ([]indexdata.FieldStorage, []bool) {
	slotCount := len(qe.schema.Indexed)
	lenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	lenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(lenZeroComplement)

	for _, acc := range qe.schema.Indexed {
		if !acc.Field.Slice {
			continue
		}
		storage, useZeroComplement := indexdata.RebuildLenFieldStorageFromIndexView(universe, indexdata.NewFieldIndexViewFromStorage(index[acc.Ordinal]))
		lenIndex[acc.Ordinal] = storage
		if useZeroComplement {
			lenZeroComplement[acc.Ordinal] = true
		}
	}
	return lenIndex, lenZeroComplement
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
	initialIndexLen                 = 32 << 10
	indexBuildGCStride              = 100_000
	indexBuildReleaseOSMemoryStride = 1_000_000

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
	forceMemoryCleanup(true)
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
	forceMemoryCleanup(true)

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
