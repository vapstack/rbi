package rbi

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
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

type persistedIndexLoadDiag struct {
	file         string
	dbPath       string
	bucket       string
	size         int64
	version      byte
	versionKnown bool
}

func (d persistedIndexLoadDiag) context() string {
	version := "unknown"
	if d.versionKnown {
		version = fmt.Sprintf("%d", d.version)
	}
	if d.size >= 0 {
		return fmt.Sprintf("persisted index file=%q db=%q bucket=%q size=%d version=%s", d.file, d.dbPath, d.bucket, d.size, version)
	}
	return fmt.Sprintf("persisted index file=%q db=%q bucket=%q version=%s", d.file, d.dbPath, d.bucket, version)
}

func (d persistedIndexLoadDiag) wrap(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s stage=%s: %w", d.context(), stage, err)
}

func recoverLoadIndex(err *error, diag *persistedIndexLoadDiag) {
	if r := recover(); r != nil {
		panicErr := fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		if diag != nil {
			*err = diag.wrap("panic", panicErr)
			return
		}
		*err = panicErr
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

type loadIndexResult struct {
	skipFields        map[string]struct{}
	skipMeasureFields map[string]struct{}
	plannerStats      *qexec.PlannerStatsSnapshot
	strMap            *strmap.Mapper
	loadTime          time.Duration
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
	result, err := db.engine.loadIndex(db.rbiFile, db.bolt, db.bucket, db.strKey)
	if err != nil {
		return nil, nil, nil, err
	}
	if db.strKey {
		db.strMap = result.strMap
	}
	db.stats.LoadTime = result.loadTime
	return result.skipFields, result.skipMeasureFields, result.plannerStats, nil
}

func (qe *queryEngine) loadIndex(
	rbiFile string,
	bolt *bbolt.DB,
	bucket []byte,
	strKey bool,
) (result loadIndexResult, err error) {
	diag := persistedIndexLoadDiag{
		file:   rbiFile,
		dbPath: bolt.Path(),
		bucket: string(bucket),
		size:   -1,
	}
	defer recoverLoadIndex(&err, &diag)

	f, err := os.Open(rbiFile)
	if err != nil {
		return loadIndexResult{}, err
	}
	defer closeFile(f)
	if info, statErr := f.Stat(); statErr == nil {
		diag.size = info.Size()
	}

	start := time.Now()
	reader := bufio.NewReaderSize(f, 32<<20)

	ver, err := reader.ReadByte()
	if err != nil {
		return loadIndexResult{}, diag.wrap("read_version", fmt.Errorf("%w: reading version: %w", errPersistedIndexInvalid, err))
	}
	diag.version = ver
	diag.versionKnown = true

	var lenLoaded bool
	switch ver {
	case 26:
		result.skipFields, result.skipMeasureFields, result.plannerStats, result.strMap, lenLoaded, err = qe.loadIndexV26(reader, bolt, bucket)
	default:
		return loadIndexResult{}, diag.wrap("version", fmt.Errorf("%w: unsupported persisted index version: %v", errPersistedIndexInvalid, ver))
	}
	if err != nil {
		if errors.Is(err, errPersistedIndexStale) || errors.Is(err, errPersistedIndexInvalid) {
			return loadIndexResult{}, diag.wrap("load_v26", err)
		}
		return loadIndexResult{}, diag.wrap("load_v26", fmt.Errorf("error loading index: %w", err))
	}

	qe.lenIndexLoaded = lenLoaded
	publishStrMap := result.strMap
	if !strKey {
		publishStrMap = nil
	}
	if err = qe.publishCurrentSequenceSnapshotNoLock(bolt, bucket, publishStrMap); err != nil {
		return loadIndexResult{}, diag.wrap("publish_snapshot", fmt.Errorf("publish snapshot: %w", err))
	}
	result.loadTime = time.Since(start)
	forceMemoryCleanup(true)

	return result, nil
}

func (qe *queryEngine) loadIndexV26(
	reader *bufio.Reader,
	bolt *bbolt.DB,
	bucket []byte,
) (map[string]struct{}, map[string]struct{}, *qexec.PlannerStatsSnapshot, *strmap.Mapper, bool, error) {
	storedSeq, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading bucket sequence: %w", err)
	}

	currentSeq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	if storedSeq != currentSeq {
		return nil, nil, nil, nil, false, fmt.Errorf("%w: bucket sequence mismatch (stored=%v, current=%v)", errPersistedIndexStale, storedSeq, currentSeq)
	}

	skipFields, skipMeasureFields, plannerStats, strMap, lenLoaded, err := qe.loadIndexPayload(reader)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("%w: %w", errPersistedIndexInvalid, err)
	}
	return skipFields, skipMeasureFields, plannerStats, strMap, lenLoaded, nil
}

func (qe *queryEngine) loadIndexPayload(
	reader *bufio.Reader,
) (map[string]struct{}, map[string]struct{}, *qexec.PlannerStatsSnapshot, *strmap.Mapper, bool, error) {

	var universe posting.List
	universe, err := posting.ReadFrom(reader)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading universe: %w", err)
	}

	sm, err := strmap.Read(reader, defaultSnapshotStrMapCompactDepth)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}

	compatible, err := readFieldCompatibility(reader, qe.schema.Fields)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	measureCompatible, err := readFieldCompatibility(reader, qe.schema.MeasureFields)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}

	indexes, err := readIndexSections(reader, compatible, "regular index")
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading index sections: %w", err)
	}

	nilIndexes, err := readIndexSections(reader, compatible, "nil index")
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading nil index sections: %w", err)
	}

	lenIndexes, err := readIndexSections(reader, compatible, "len index")
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading len index sections: %w", err)
	}

	measureIndexes, err := readMeasureIndexSections(reader, measureCompatible)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading measure index sections: %w", err)
	}

	plannerStats, err := readPlannerStatsSnapshot(reader, compatible)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading planner stats: %w", err)
	}

	skipFields := make(map[string]struct{}, len(qe.schema.Fields))
	for name := range qe.schema.Fields {
		_, hasRegular := indexes[name]
		_, hasNil := nilIndexes[name]
		if compatible[name] && (hasRegular || hasNil) {
			skipFields[name] = struct{}{}
		}
	}
	skipMeasureFields := make(map[string]struct{}, len(qe.schema.MeasureFields))
	for name := range qe.schema.MeasureFields {
		if measureCompatible[name] {
			if _, hasMeasure := measureIndexes[name]; hasMeasure {
				skipMeasureFields[name] = struct{}{}
			}
		}
	}

	qe.universe = universe
	indexdata.ReleaseFieldStorageSlots(qe.index)
	indexdata.ReleaseFieldStorageSlots(qe.nilIndex)
	indexdata.ReleaseFieldStorageSlots(qe.lenIndex)
	indexdata.ReleaseMeasureStorageSlots(qe.measure)
	if qe.lenZeroComplement != nil {
		pooled.ReleaseBoolSlice(qe.lenZeroComplement)
	}
	slotCount := len(qe.schema.Indexed)
	qe.index = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.nilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	measureSlotCount := len(qe.schema.Measures)
	qe.measure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for _, acc := range qe.schema.Indexed {
		if storage, ok := indexes[acc.Name]; ok {
			qe.index[acc.Ordinal] = storage
			delete(indexes, acc.Name)
		}
		if storage, ok := nilIndexes[acc.Name]; ok {
			qe.nilIndex[acc.Ordinal] = storage
			delete(nilIndexes, acc.Name)
		}
		if storage, ok := lenIndexes[acc.Name]; ok {
			qe.lenIndex[acc.Ordinal] = storage
			delete(lenIndexes, acc.Name)
		}
	}
	for _, acc := range qe.schema.Measures {
		if storage, ok := measureIndexes[acc.Name]; ok {
			qe.measure[acc.Ordinal] = storage
			delete(measureIndexes, acc.Name)
		}
	}
	indexdata.ReleaseFieldStorageMap(indexes)
	indexdata.ReleaseFieldStorageMap(nilIndexes)
	indexdata.ReleaseFieldStorageMap(lenIndexes)
	indexdata.ReleaseMeasureStorageMap(measureIndexes)
	qe.lenZeroComplement = detectLenZeroComplement(qe.lenIndex, qe.schema.Indexed)

	lenLoaded := true
	for name := range qe.schema.Fields {
		if _, ok := skipFields[name]; !ok {
			lenLoaded = false
			break
		}
	}
	if lenLoaded && !universe.IsEmpty() {
		for name, f := range qe.schema.Fields {
			if f == nil || !f.Slice {
				continue
			}
			if _, ok := skipFields[name]; !ok {
				continue
			}
			acc, ok := qe.schema.IndexedByName[name]
			if !ok {
				lenLoaded = false
				break
			}
			if qe.lenIndex[acc.Ordinal].KeyCount() == 0 {
				lenLoaded = false
				break
			}
		}
	}

	return skipFields, skipMeasureFields, plannerStats, sm, lenLoaded, nil
}

func detectLenZeroComplement(indexes []indexdata.FieldStorage, access []schema.IndexedFieldAccessor) []bool {
	out := pooled.GetBoolSlice(len(access))[:len(access)]
	clear(out)
	if indexes == nil {
		return out
	}
	for _, acc := range access {
		if acc.Ordinal >= len(indexes) {
			continue
		}
		if indexdata.NewFieldIndexViewFromStorage(indexes[acc.Ordinal]).LookupCardinality(indexdata.LenIndexNonEmptyKey) > 0 {
			out[acc.Ordinal] = true
		}
	}
	return out
}

func readFieldCompatibility(reader *bufio.Reader, current map[string]*schema.Field) (map[string]bool, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading fields len: %w", err)
	}
	if count > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: stored field count overflows int: %v", count)
	}
	compatible := make(map[string]bool, max(0, min(int(count), len(current))))
	seen := make(map[string]struct{}, max(0, min(int(count), len(current))))

	for i := uint64(0); i < count; i++ {
		name, stored, err := readField(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading field %d/%d: %w", i+1, count, err)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("decode: duplicate field %q at entry %d/%d", name, i+1, count)
		}
		seen[name] = struct{}{}
		cur := current[name]
		if sameFieldDefinition(cur, stored) {
			compatible[name] = true
		}
	}
	return compatible, nil
}

func sameFieldDefinition(a, b *schema.Field) bool {
	if a == nil || b == nil {
		return false
	}
	if a.Unique != b.Unique ||
		a.IndexKind != b.IndexKind ||
		a.Kind != b.Kind ||
		a.Ptr != b.Ptr ||
		a.Slice != b.Slice ||
		a.UseVI != b.UseVI ||
		a.DBName != b.DBName ||
		!slices.Equal(a.Index, b.Index) {
		return false
	}
	return true
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

	tmpFile := rbiFile + ".temp"
	defer os.Remove(tmpFile)

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer closeFile(f)

	buf := bufio.NewWriterSize(f, 32<<20)

	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return fmt.Errorf("store: reading bucket sequence: %w", err)
	}

	if err = qe.storeIndexV26(buf, seq); err != nil {
		return err
	}

	if err = buf.Flush(); err != nil {
		return fmt.Errorf("flushing write buffers: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("syncing persisted index temp file: %w", err)
	}

	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpFile, rbiFile); err != nil {
		return err
	}
	if err = syncDir(rbiFile); err != nil {
		return fmt.Errorf("syncing persisted index directory: %w", err)
	}
	return nil
}

func (qe *queryEngine) storeIndexV26(writer *bufio.Writer, bucketSeq uint64) error {
	if err := writer.WriteByte(26); err != nil {
		return fmt.Errorf("store: writing version: %w", err)
	}
	if err := writeSidecarUvarint(writer, bucketSeq); err != nil {
		return fmt.Errorf("store: writing bucket sequence: %w", err)
	}

	return qe.storeIndexPayload(writer)
}

func (qe *queryEngine) storeIndexPayload(writer *bufio.Writer) error {
	snap := qe.snapshot.Current()
	universe := snap.Universe.Borrow()

	if err := universe.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	if err := strmap.WriteSnapshot(writer, snap.StrMap); err != nil {
		return err
	}

	if err := writeFields(writer, qe.schema.Fields); err != nil {
		return err
	}
	if err := writeFields(writer, qe.schema.MeasureFields); err != nil {
		return err
	}

	fieldNames := sortedFieldNames(snap.FieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(fieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range fieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		storage, _ := snap.FieldIndexStorage(field)
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	nilFieldNames := sortedFieldNames(snap.NilFieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(nilFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range nilFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.IndexedFieldByName[field]
		if err := snap.NilIndex[acc.Ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	lenFieldNames := sortedFieldNames(snap.LenFieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(lenFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range lenFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.IndexedFieldByName[field]
		if err := snap.LenIndex[acc.Ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	measureFieldNames := sortedMapFieldNames(qe.schema.MeasureFields)

	if err := writeSidecarUvarint(writer, uint64(len(measureFieldNames))); err != nil {
		return fmt.Errorf("encode: writing measure index family len: %w", err)
	}
	for _, field := range measureFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing measure field %q name: %w", field, err)
		}
		acc := qe.schema.MeasuresByName[field]
		var storage indexdata.MeasureStorage
		if snap.Measure == nil || acc.Ordinal >= len(snap.Measure) {
			storage = indexdata.MeasureStorage{}
		} else {
			storage = snap.Measure[acc.Ordinal]
		}
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing measure field %q: %w", field, err)
		}
	}

	statsVersion := qe.exec.StatsVersion.Load()
	if statsVersion == 0 {
		statsVersion = 1
	} else {
		statsVersion++
	}
	if err := writePlannerStatsSnapshot(writer, qe.plannerStatsSnapshotForPersistLocked(statsVersion)); err != nil {
		return err
	}

	return nil
}

func writeFields(writer *bufio.Writer, fields map[string]*schema.Field) error {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	if err := writeSidecarUvarint(writer, uint64(len(names))); err != nil {
		return fmt.Errorf("encode: writing fields len: %w", err)
	}
	for _, name := range names {
		if err := writeField(writer, name, fields[name]); err != nil {
			return fmt.Errorf("encode: writing field %q: %w", name, err)
		}
	}
	return nil
}

func writeField(writer *bufio.Writer, name string, f *schema.Field) error {
	if err := writeSidecarString(writer, name); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Unique); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(f.IndexKind)); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(f.Kind)); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Ptr); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Slice); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.UseVI); err != nil {
		return err
	}
	if err := writeSidecarString(writer, f.DBName); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(len(f.Index))); err != nil {
		return err
	}
	for _, idx := range f.Index {
		if idx < 0 {
			return fmt.Errorf("negative field index")
		}
		if err := writeSidecarUvarint(writer, uint64(idx)); err != nil {
			return err
		}
	}
	return nil
}

func readField(reader *bufio.Reader) (string, *schema.Field, error) {
	name, err := readSidecarString(reader)
	if err != nil {
		return "", nil, err
	}
	unique, err := readSidecarBool(reader)
	if err != nil {
		return "", nil, err
	}
	indexKind, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", nil, err
	}
	if indexKind > uint64(^schema.IndexKind(0)) {
		return "", nil, fmt.Errorf("invalid IndexKind %d", indexKind)
	}
	fieldIndexKind := schema.IndexKind(indexKind)
	if err := schema.ValidateIndexKind(fieldIndexKind); err != nil {
		return "", nil, err
	}
	kind, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", nil, err
	}
	ptr, err := readSidecarBool(reader)
	if err != nil {
		return "", nil, err
	}
	slice, err := readSidecarBool(reader)
	if err != nil {
		return "", nil, err
	}
	useVI, err := readSidecarBool(reader)
	if err != nil {
		return "", nil, err
	}
	dbName, err := readSidecarString(reader)
	if err != nil {
		return "", nil, err
	}
	valIndexLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", nil, err
	}
	valIndex := make([]int, 0, valIndexLen)
	for i := uint64(0); i < valIndexLen; i++ {
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return "", nil, err
		}
		if v > uint64(^uint(0)>>1) {
			return "", nil, fmt.Errorf("field index element overflows int")
		}
		valIndex = append(valIndex, int(v))
	}
	return name, &schema.Field{
		Unique:    unique,
		IndexKind: fieldIndexKind,
		Kind:      reflect.Kind(kind),
		Ptr:       ptr,
		Slice:     slice,
		UseVI:     useVI,
		DBName:    dbName,
		Index:     valIndex,
	}, nil
}

func writePlannerStatsSnapshot(writer *bufio.Writer, s *qexec.PlannerStatsSnapshot) error {
	if s == nil {
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats version: %w", err)
		}
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
		}
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats universe: %w", err)
		}
		return writeSidecarUvarint(writer, 0)
	}

	if err := writeSidecarUvarint(writer, s.Version); err != nil {
		return fmt.Errorf("encode: writing planner stats version: %w", err)
	}

	generatedAt := uint64(0)
	if !s.GeneratedAt.IsZero() {
		generatedAt = uint64(s.GeneratedAt.UnixNano())
	}
	if err := writeSidecarUvarint(writer, generatedAt); err != nil {
		return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
	}
	if err := writeSidecarUvarint(writer, s.UniverseCardinality); err != nil {
		return fmt.Errorf("encode: writing planner stats universe: %w", err)
	}

	fields := sortedMapFieldNames(s.Fields)
	if err := writeSidecarUvarint(writer, uint64(len(fields))); err != nil {
		return fmt.Errorf("encode: writing planner stats field count: %w", err)
	}
	for _, f := range fields {
		if err := writeSidecarString(writer, f); err != nil {
			return fmt.Errorf("encode: writing planner stats field name: %w", err)
		}
		if err := writePlannerFieldStats(writer, s.Fields[f]); err != nil {
			return fmt.Errorf("encode: writing planner stats field %q: %w", f, err)
		}
	}
	return nil
}

func readPlannerStatsSnapshot(reader *bufio.Reader, compatible map[string]bool) (*qexec.PlannerStatsSnapshot, error) {
	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats version: %w", err)
	}
	generatedAtNanos, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats generated_at: %w", err)
	}
	universe, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats universe: %w", err)
	}
	fieldCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats field count: %w", err)
	}
	if fieldCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: planner stats field count overflows int: %v", fieldCount)
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && fieldCount == 0 {
		return nil, nil
	}

	fields := make(map[string]PlannerFieldStats, min(int(fieldCount), len(compatible)))
	for i := uint64(0); i < fieldCount; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field name %d/%d: %w", i+1, fieldCount, err)
		}
		stats, err := readPlannerFieldStats(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field %q (%d/%d): %w", f, i+1, fieldCount, err)
		}
		if compatible[f] {
			if _, exists := fields[f]; exists {
				return nil, fmt.Errorf("decode: duplicate planner stats field %q at entry %d/%d", f, i+1, fieldCount)
			}
			fields[f] = stats
		}
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && len(fields) == 0 {
		return nil, nil
	}
	for f := range compatible {
		if _, ok := fields[f]; !ok {
			return nil, fmt.Errorf("decode: missing planner stats field %q (loaded=%d compatible=%d)", f, len(fields), len(compatible))
		}
	}

	out := &qexec.PlannerStatsSnapshot{
		Version:             version,
		UniverseCardinality: universe,
		Fields:              fields,
	}
	if generatedAtNanos > 0 {
		out.GeneratedAt = time.Unix(0, int64(generatedAtNanos)).UTC()
	}
	return out, nil
}

func writePlannerFieldStats(writer *bufio.Writer, s PlannerFieldStats) error {
	if err := writeSidecarUvarint(writer, s.DistinctKeys); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.NonEmptyKeys); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.TotalBucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.MaxBucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.P50BucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.P95BucketCard); err != nil {
		return err
	}
	return nil
}

func readPlannerFieldStats(reader *bufio.Reader) (PlannerFieldStats, error) {
	distinct, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	nonEmpty, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	total, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	maxCard, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	p50, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}
	p95, err := binary.ReadUvarint(reader)
	if err != nil {
		return PlannerFieldStats{}, err
	}

	out := PlannerFieldStats{
		DistinctKeys:    distinct,
		NonEmptyKeys:    nonEmpty,
		TotalBucketCard: total,
		MaxBucketCard:   maxCard,
		P50BucketCard:   p50,
		P95BucketCard:   p95,
	}
	if distinct > 0 {
		out.AvgBucketCard = float64(total) / float64(distinct)
	}
	return out, nil
}

func sortedMapFieldNames[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for f := range m {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func readIndexSections(
	reader *bufio.Reader,
	compatible map[string]bool,
	section string,
) (map[string]indexdata.FieldStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s field count: %w", section, err)
	}
	if count == 0 {
		return make(map[string]indexdata.FieldStorage), nil
	}

	out := make(map[string]indexdata.FieldStorage, min(int(count), len(compatible)))
	seen := make(map[string]struct{}, min(int(count), len(compatible)))
	for i := uint64(0); i < count; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			return nil, fmt.Errorf("reading %s field name %d/%d: %w", section, i+1, count, err)
		}
		if _, exists := seen[f]; exists {
			return nil, fmt.Errorf("duplicate %s field %q at entry %d/%d", section, f, i+1, count)
		}
		seen[f] = struct{}{}

		keep := compatible[f]
		storage, err := indexdata.ReadFieldStorage(reader, keep, section, f)
		if err != nil {
			return nil, fmt.Errorf("reading %s storage for field %q (%d/%d keep=%t): %w", section, f, i+1, count, keep, err)
		}
		if keep && storage.KeyCount() > 0 {
			out[f] = storage
		}
	}

	return out, nil
}

func readMeasureIndexSections(
	reader *bufio.Reader,
	compatible map[string]bool,
) (map[string]indexdata.MeasureStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("reading measure index field count: %w", err)
	}
	if count == 0 {
		return make(map[string]indexdata.MeasureStorage), nil
	}

	out := make(map[string]indexdata.MeasureStorage, min(int(count), len(compatible)))
	seen := make(map[string]struct{}, min(int(count), len(compatible)))
	for i := uint64(0); i < count; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			return nil, fmt.Errorf("reading measure index field name %d/%d: %w", i+1, count, err)
		}
		if _, exists := seen[f]; exists {
			return nil, fmt.Errorf("duplicate measure index field %q at entry %d/%d", f, i+1, count)
		}
		seen[f] = struct{}{}

		keep := compatible[f]
		storage, err := indexdata.ReadMeasureStorage(reader, keep)
		if err != nil {
			return nil, fmt.Errorf("reading measure index storage for field %q (%d/%d keep=%t): %w", f, i+1, count, keep, err)
		}
		if keep {
			out[f] = storage
		}
	}

	return out, nil
}

func sortedFieldNames(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func writeSidecarUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	if _, err := writer.Write(buf[:n]); err != nil {
		return err
	}
	return nil
}

func writeSidecarBool(writer *bufio.Writer, v bool) error {
	if v {
		return writer.WriteByte(1)
	}
	return writer.WriteByte(0)
}

func readSidecarBool(reader *bufio.Reader) (bool, error) {
	v, err := reader.ReadByte()
	if v != 0 && v != 1 {
		return false, fmt.Errorf("corrupted bool value: %v", v)
	}
	return v > 0, err
}

func writeSidecarString(writer *bufio.Writer, s string) error {
	if err := writeSidecarUvarint(writer, uint64(len(s))); err != nil {
		return err
	}
	if len(s) == 0 {
		return nil
	}
	if _, err := io.WriteString(writer, s); err != nil {
		return err
	}
	return nil
}

func readSidecarString(reader *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n > indexdata.MaxStoredStringLen {
		return "", fmt.Errorf("string len %v exceeds limit (%v)", n, indexdata.MaxStoredStringLen)
	}
	if n > uint64(^uint(0)>>1) {
		return "", fmt.Errorf("string len %v overflows int", n)
	}
	b := make([]byte, int(n))
	if _, err = io.ReadFull(reader, b); err != nil {
		return "", err
	}
	s := unsafe.String(unsafe.SliceData(b), len(b))
	return s, nil
}

type uniqueBatchCheckState struct {
	leaving map[string]map[keycodec.IndexLookupKey]posting.List
	seen    map[string]map[keycodec.IndexLookupKey]uint64
}

type uniqueLeavingTouch struct {
	field string
	key   keycodec.IndexLookupKey
	added bool
}

type uniqueSeenWrite struct {
	field string
	key   keycodec.IndexLookupKey
}

func markUniqueBatchLeaving(state uniqueBatchCheckState, field string, key keycodec.IndexLookupKey, idx uint64) bool {
	fm := state.leaving[field]
	if fm == nil {
		fm = uniqueLeavingInnerPool.Get()
		state.leaving[field] = fm
	}
	bm := fm[key]
	var added bool
	bm, added = bm.BuildAddedChecked(idx)
	if !added {
		return false
	}
	fm[key] = bm
	return true
}

func appendUniqueBatchLeavingTouch(state uniqueBatchCheckState, touched []uniqueLeavingTouch, field string, key keycodec.IndexLookupKey, idx uint64) []uniqueLeavingTouch {
	added := markUniqueBatchLeaving(state, field, key, idx)
	return append(touched, uniqueLeavingTouch{field: field, key: key, added: added})
}

func rollbackUniqueBatchLeaving(state uniqueBatchCheckState, touched []uniqueLeavingTouch, idx uint64) {
	for i := len(touched) - 1; i >= 0; i-- {
		t := touched[i]
		if !t.added {
			continue
		}
		fm := state.leaving[t.field]
		if fm == nil {
			continue
		}
		ids := fm[t.key]
		if ids.IsEmpty() {
			continue
		}
		ids = ids.BuildRemoved(idx)
		if ids.IsEmpty() {
			delete(fm, t.key)
		} else {
			fm[t.key] = ids
		}
		if len(fm) == 0 {
			delete(state.leaving, t.field)
			uniqueLeavingInnerPool.Put(fm)
		}
	}
}

func (qe *queryEngine) checkUniqueBatchCandidateAndCollectSeen(
	state uniqueBatchCheckState,
	idx uint64,
	acc schema.IndexedFieldAccessor,
	ptr unsafe.Pointer,
	seenWrites []uniqueSeenWrite,
) ([]uniqueSeenWrite, error) {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return seenWrites, nil
	}

	if sm := state.seen[acc.Name]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.Name)
		}
	}

	ids := qe.snapshot.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
	}

	var lv posting.List
	if fm := state.leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
	}

	iter := ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
	}
	return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
}

func (qe *queryEngine) checkUniqueBatchAppend(state uniqueBatchCheckState, idx uint64, oldVal, newVal unsafe.Pointer) error {
	if len(qe.schema.Unique) == 0 {
		return nil
	}
	var (
		touchedInline [8]uniqueLeavingTouch
		touched       = touchedInline[:0]
	)
	if oldVal != nil {
		if newVal == nil {
			for _, acc := range qe.schema.Unique {
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
			}
		} else {
			for _, acc := range qe.schema.Unique {
				if !acc.Modified(oldVal, newVal) {
					continue
				}
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
			}
		}
	}

	if newVal == nil {
		return nil
	}

	var (
		seenWritesInline [8]uniqueSeenWrite
		seenWrites       = seenWritesInline[:0]
	)

	if oldVal == nil {
		for _, acc := range qe.schema.Unique {
			var err error
			seenWrites, err = qe.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	} else {
		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			var err error
			seenWrites, err = qe.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	}

	for _, w := range seenWrites {
		sm := state.seen[w.field]
		if sm == nil {
			sm = uniqueSeenInnerPool.Get()
			state.seen[w.field] = sm
		}
		sm[w.key] = idx
	}
	return nil
}

func collapseUniqueWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer, pos map[uint64]int) ([]uint64, []unsafe.Pointer, []unsafe.Pointer) {
	n := 0
	for i, idx := range idxs {
		p := pos[idx]
		if p != 0 {
			if newVals != nil {
				newVals[p-1] = newVals[i]
			}
			continue
		}
		pos[idx] = n + 1
		if n != i {
			idxs[n] = idx
			oldVals[n] = oldVals[i]
			if newVals != nil {
				newVals[n] = newVals[i]
			}
		}
		n++
	}
	idxs = idxs[:n]
	oldVals = oldVals[:n]
	if newVals != nil {
		newVals = newVals[:n]
	}
	return idxs, oldVals, newVals
}

func (qe *queryEngine) checkUniqueOnWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer) error {
	if len(idxs) == 0 || len(qe.schema.Unique) == 0 {
		return nil
	}

	pos := uint64IntMapPool.Get(len(idxs))
	defer uint64IntMapPool.Put(pos)

	idxs, oldVals, newVals = collapseUniqueWriteMulti(idxs, oldVals, newVals, pos)

	leaving := uniqueLeavingOuterPool.Get()
	defer uniqueLeavingOuterPool.Put(leaving)

	seen := uniqueSeenOuterPool.Get()
	defer uniqueSeenOuterPool.Put(seen)

	for i, idx := range idxs {
		oldVal := oldVals[i]
		var newVal unsafe.Pointer
		if newVals != nil {
			newVal = newVals[i]
		}
		if oldVal == nil {
			continue
		}

		// On delete (newVal == nil), the record releases all unique scalar values.
		// On update, only modified unique fields can release previous values.
		if newVal == nil {
			for _, acc := range qe.schema.Unique {
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				m := leaving[acc.Name]
				if m == nil {
					m = uniqueLeavingInnerPool.Get()
					leaving[acc.Name] = m
				}
				ids := m[single]
				ids = ids.BuildAdded(idx)
				m[single] = ids
			}
			continue
		}

		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			single, ok, isNil := acc.UniqueGetter(oldVal)
			if !ok || isNil {
				continue
			}
			m := leaving[acc.Name]
			if m == nil {
				m = uniqueLeavingInnerPool.Get()
				leaving[acc.Name] = m
			}
			ids := m[single]
			ids = ids.BuildAdded(idx)
			m[single] = ids
		}
	}

	if newVals == nil {
		return nil // deletion
	}

	for i, idx := range idxs {

		newVal := newVals[i]
		if newVal == nil {
			continue
		}

		if oldVals[i] == nil {
			for _, acc := range qe.schema.Unique {
				if err := qe.checkUniqueBatchCandidate(idx, newVal, acc, seen, leaving); err != nil {
					return err
				}
			}
			continue
		}

		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVals[i], newVal) {
				continue
			}
			if err := qe.checkUniqueBatchCandidate(idx, newVal, acc, seen, leaving); err != nil {
				return err
			}
		}
	}
	return nil
}

func (qe *queryEngine) checkUniqueBatchCandidate(
	idx uint64,
	ptr unsafe.Pointer,
	acc schema.IndexedFieldAccessor,
	seen map[string]map[keycodec.IndexLookupKey]uint64,
	leaving map[string]map[keycodec.IndexLookupKey]posting.List,
) error {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return nil
	}

	sm := seen[acc.Name]
	if sm == nil {
		sm = uniqueSeenInnerPool.Get()
		seen[acc.Name] = sm
	}
	if prev, ok := sm[single]; ok && prev != idx {
		return fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.Name)
	}
	sm[single] = idx

	ids := qe.snapshot.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return nil
	}

	var lv posting.List
	if fm := leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return nil
		}
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
	}

	iter := ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
	}
	return nil
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
