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
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

func cleanupBuildIndexFailure(buildOK *bool, fieldStates []*buildIndexFieldState, localUniverse []posting.List) {
	if *buildOK {
		return
	}
	for i := range fieldStates {
		fieldStates[i].release()
	}
	for i := range localUniverse {
		localUniverse[i].Release()
	}
}

func releaseBuildIndexLocalStates(localStates []buildIndexFieldLocalState) {
	for i := range localStates {
		localStates[i].release()
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
	plannerStats      *plannerStatsSnapshot
	strMap            *strMapper
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
		return fmt.Sprintf("id=%d", binary.BigEndian.Uint64(key))
	}
	return fmt.Sprintf("key_len=%d key_prefix_hex=%s", len(key), formatDiagnosticBytesPrefix(key, 24))
}

/**/

type buildIndexFieldState struct {
	runsMu sync.Mutex
	runs   []indexdata.FieldStorageRun

	nilMu sync.Mutex
	nils  posting.List

	lenMu  sync.Mutex
	lenMap map[uint32]posting.List
}

type buildIndexFieldLocalState struct {
	numeric bool
	vals    map[string]posting.List
	fixed   map[uint64]posting.List
	lenMap  map[uint32]posting.List
	nils    posting.List
}

func buildIndexRunTargetEntries() int {
	return max(4<<10, indexdata.FieldChunkTargetEntries*16)
}

func newBuildIndexFieldState(slice bool) *buildIndexFieldState {
	state := &buildIndexFieldState{}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func newBuildIndexFieldLocalState(numeric bool, slice bool) buildIndexFieldLocalState {
	state := buildIndexFieldLocalState{numeric: numeric}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func (s *buildIndexFieldLocalState) addValue(key string, idx uint64) {
	if s.vals == nil {
		s.vals = indexdata.GetPostingMap()
	}
	s.vals[key] = s.vals[key].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addFixedValue(key uint64, idx uint64) {
	if s.fixed == nil {
		s.fixed = indexdata.GetFixedPostingMap()
	}
	s.fixed[key] = s.fixed[key].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addNil(idx uint64) {
	s.nils = s.nils.BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) addLen(length int, idx uint64) {
	if length < 0 {
		return
	}
	if s.lenMap == nil {
		s.lenMap = make(map[uint32]posting.List, 8)
	}
	ln := uint32(length)
	s.lenMap[ln] = s.lenMap[ln].BuildAdded(idx)
}

func (s *buildIndexFieldLocalState) shouldFlushRegular() bool {
	if s.numeric {
		return len(s.fixed) >= buildIndexRunTargetEntries()
	}
	return len(s.vals) >= buildIndexRunTargetEntries()
}

func (s *buildIndexFieldState) appendRun(run indexdata.FieldStorageRun) {
	if s == nil || run.KeyCount() == 0 {
		return
	}
	s.runsMu.Lock()
	s.runs = append(s.runs, run)
	s.runsMu.Unlock()
}

func (s *buildIndexFieldLocalState) flushRegularInto(dst *buildIndexFieldState) {
	if s == nil || dst == nil {
		return
	}
	if s.numeric {
		if run := indexdata.NewFixedFieldStorageRunFromPostingMap(s.fixed); run.KeyCount() > 0 {
			dst.appendRun(run)
		}
		return
	}
	if run := indexdata.NewStringFieldStorageRunFromPostingMap(s.vals); run.KeyCount() > 0 {
		dst.appendRun(run)
	}
}

func (s *buildIndexFieldLocalState) flushAllInto(dst *buildIndexFieldState) {
	if s == nil || dst == nil {
		return
	}
	s.flushRegularInto(dst)
	if !s.nils.IsEmpty() {
		dst.nilMu.Lock()
		ids := dst.nils
		ids = ids.BuildMergedOwned(s.nils)
		dst.nils = ids
		dst.nilMu.Unlock()
		s.nils = posting.List{}
	}
	if len(s.lenMap) > 0 {
		dst.lenMu.Lock()
		for ln, ids := range s.lenMap {
			merged := dst.lenMap[ln]
			merged = merged.BuildMergedOwned(ids)
			dst.lenMap[ln] = merged
		}
		dst.lenMu.Unlock()
		clear(s.lenMap)
	}
}

func (s *buildIndexFieldLocalState) release() {
	if s == nil {
		return
	}

	posting.ReleaseMap(s.vals)
	indexdata.PutPostingMap(s.vals)
	s.vals = nil

	posting.ReleaseMap(s.fixed)
	indexdata.PutFixedPostingMap(s.fixed)
	s.fixed = nil

	posting.ReleaseMap(s.lenMap)
	s.lenMap = nil

	s.nils.Release()
	s.nils = posting.List{}
}

func (s *buildIndexFieldState) materializeStorage() indexdata.FieldStorage {
	if s == nil || len(s.runs) == 0 {
		return indexdata.FieldStorage{}
	}
	runs := s.runs
	s.runs = nil
	return indexdata.NewRegularFieldStorageFromRunsOwned(runs)
}

func (s *buildIndexFieldState) release() {
	if s == nil {
		return
	}
	indexdata.ReleaseFieldStorageRunsOwned(s.runs)
	s.runs = nil

	s.nils.Release()
	s.nils = posting.List{}

	posting.ReleaseMap(s.lenMap)
	s.lenMap = nil
}

func (s *buildIndexFieldState) materializeNilStorage() indexdata.FieldStorage {
	if s == nil {
		return indexdata.FieldStorage{}
	}
	ids := s.nils
	s.nils = posting.List{}
	return indexdata.NewNilFieldStorageOwned(ids)
}

func (s *buildIndexFieldState) materializeLenStorage(universe posting.List) (indexdata.FieldStorage, bool) {
	if s == nil {
		return indexdata.FieldStorage{}, false
	}
	lenMap := s.lenMap
	s.lenMap = nil
	return indexdata.NewLenFieldStorageFromMapOwned(universe, lenMap)
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
	strMap *strMapper,
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
		acc     indexedFieldAccessor
		slice   bool
		numeric bool
	}

	active := make([]buildField, 0, len(qe.indexedFieldAccess))
	for _, acc := range qe.indexedFieldAccess {
		if _, skip := skipFields[acc.name]; skip {
			continue
		}
		active = append(active, buildField{
			acc:     acc,
			slice:   acc.field.Slice,
			numeric: acc.field.KeyKind == fieldWriteKeysOrderedU64,
		})
	}
	activeMeasures := make([]measureFieldAccessor, 0, len(qe.measureFieldAccess))
	for _, acc := range qe.measureFieldAccess {
		if _, skip := skipMeasureFields[acc.name]; skip {
			continue
		}
		activeMeasures = append(activeMeasures, acc)
	}

	fcnt := len(active)
	mcnt := len(activeMeasures)
	if fcnt <= 0 && mcnt <= 0 {
		if !qe.lenIndexLoaded {
			qe.buildLenIndex()
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

	fieldStates := make([]*buildIndexFieldState, len(active))
	for i := range active {
		fieldStates[i] = newBuildIndexFieldState(active[i].slice)
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
			localStates := make([]buildIndexFieldLocalState, len(active))
			for i := range active {
				localStates[i] = newBuildIndexFieldLocalState(active[i].numeric, active[i].slice)
			}
			defer releaseBuildIndexLocalStates(localStates)
			localMeasures := make([][]indexdata.MeasureEntry, len(qe.measureFieldAccess))
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
					active[k].acc.writeBuild(ptr, buildFieldWriteSink{
						state: &localStates[k],
						idx:   idx,
						field: active[k].acc.name,
						err:   &fieldErr,
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
							active[k].acc.name,
							fieldErr,
						)
						cancel()
						return
					}
					if localStates[k].shouldFlushRegular() {
						localStates[k].flushRegularInto(fieldStates[k])
					}
				}
				for _, acc := range activeMeasures {
					if value, ok := acc.read(ptr); ok {
						buf := localMeasures[acc.ordinal]
						if buf == nil {
							buf = indexdata.GetMeasureEntrySlice(0)
							localMeasures[acc.ordinal] = buf
						}
						buf = append(buf, indexdata.MeasureEntry{ID: idx, Value: value})
						localMeasures[acc.ordinal] = buf
					}
				}
				release(ptr)
			}
			for i := range localStates {
				localStates[i].flushAllInto(fieldStates[i])
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
		if strKey {
			strMap.Lock()
			defer strMap.Unlock()
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
			idx := idxFromKeyNoLock(strKey, strMap, k)
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

	slotCount := len(qe.indexedFieldAccess)
	if qe.index == nil || len(qe.index) != slotCount {
		indexdata.ReleaseFieldStorageSlots(qe.index)
		qe.index = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	}
	if qe.nilIndex == nil || len(qe.nilIndex) != slotCount {
		indexdata.ReleaseFieldStorageSlots(qe.nilIndex)
		qe.nilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	}
	if qe.lenIndex == nil || len(qe.lenIndex) != slotCount {
		indexdata.ReleaseFieldStorageSlots(qe.lenIndex)
		qe.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	}
	if len(qe.lenZeroComplement) != slotCount {
		if qe.lenZeroComplement != nil {
			pooled.PutBoolSlice(qe.lenZeroComplement)
		}
		qe.lenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(qe.lenZeroComplement)
	} else if len(skipFields) == 0 {
		clear(qe.lenZeroComplement)
	} else {
		for i := range active {
			qe.lenZeroComplement[active[i].acc.ordinal] = false
		}
	}
	if qe.measure == nil || len(qe.measure) != len(qe.measureFieldAccess) {
		indexdata.ReleaseMeasureStorageSlots(qe.measure)
		measureSlotCount := len(qe.measureFieldAccess)
		qe.measure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	}

	qe.universe = posting.List{}
	for i := range localUniverse {
		if localUniverse[i].IsEmpty() {
			continue
		}
		qe.universe = qe.universe.BuildMergedOwned(localUniverse[i])
	}

	for i := range fieldStates {
		ordinal := active[i].acc.ordinal
		oldIndexStorage := qe.index[ordinal]
		if storage := fieldStates[i].materializeStorage(); storage.KeyCount() > 0 {
			oldIndexStorage.Release()
			qe.index[ordinal] = storage
		} else {
			oldIndexStorage.Release()
			qe.index[ordinal] = indexdata.FieldStorage{}
		}
		oldNilStorage := qe.nilIndex[ordinal]
		if storage := fieldStates[i].materializeNilStorage(); storage.KeyCount() > 0 {
			oldNilStorage.Release()
			qe.nilIndex[ordinal] = storage
		} else {
			oldNilStorage.Release()
			qe.nilIndex[ordinal] = indexdata.FieldStorage{}
		}
		oldLenStorage := qe.lenIndex[ordinal]
		if storage, useZeroComplement := fieldStates[i].materializeLenStorage(qe.universe); active[i].slice {
			if storage.KeyCount() > 0 {
				oldLenStorage.Release()
				qe.lenIndex[ordinal] = storage
			} else {
				oldLenStorage.Release()
				qe.lenIndex[ordinal] = indexdata.FieldStorage{}
			}
			if useZeroComplement {
				qe.lenZeroComplement[ordinal] = true
			}
		}
	}
	var oldMeasureStorages []indexdata.MeasureStorage
	if len(activeMeasures) > 0 {
		oldMeasureStorages = indexdata.GetMeasureStorageSlice(len(activeMeasures))
		for _, acc := range activeMeasures {
			i := acc.ordinal
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
				indexdata.PutMeasureEntrySlice(buf)
				localMeasureStates[worker][i] = nil
			}
			oldStorage := qe.measure[i]
			storage := indexdata.NewMeasureStorageFromEntriesOwned(entries)
			oldMeasureStorages = append(oldMeasureStorages, oldStorage)
			qe.measure[i] = storage
		}
	}

	recordCount := qe.universe.Cardinality()
	buildTime := time.Since(start)
	buildRPS := int(float64(recordCount) / max(buildTime.Seconds(), 1))

	for name, f := range qe.fields {
		if f.Slice {
			acc, ok := qe.indexedFieldMap[name]
			if !ok || qe.lenIndex[acc.ordinal].KeyCount() == 0 {
				qe.buildLenIndex()
				break
			}
		}
	}
	qe.lenIndexLoaded = false
	err = qe.publishCurrentSequenceSnapshotNoLock(bolt, bucket, strMap)
	if oldMeasureStorages != nil {
		for i := 0; i < len(oldMeasureStorages); i++ {
			oldMeasureStorages[i].Release()
		}
		indexdata.PutMeasureStorageSlice(oldMeasureStorages)
	}
	if err != nil {
		return buildIndexResult{}, fmt.Errorf("publish snapshot: %w", err)
	}

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

func addDistinctStrings(n int, valueAt func(int) string, add func(string)) int {
	if n == 0 {
		return 0
	}
	if n == 1 {
		add(valueAt(0))
		return 1
	}
	seen := stringSetPool.Get(n)
	defer stringSetPool.Put(seen)
	distinct := 0
	for i := 0; i < n; i++ {
		cur := valueAt(i)
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		add(cur)
	}
	return distinct
}

type buildFieldWriteSink struct {
	state *buildIndexFieldLocalState
	idx   uint64
	field string
	err   *error
}

func (s buildFieldWriteSink) setNil() {
	if s.state == nil {
		return
	}
	s.state.addNil(s.idx)
}

func (s buildFieldWriteSink) setLen(length int) {
	if s.state == nil {
		return
	}
	s.state.addLen(length, s.idx)
}

func (s buildFieldWriteSink) addString(key string) {
	if s.err != nil && *s.err == nil && len(key) > indexdata.FieldStringRefMax {
		if s.field != "" {
			*s.err = fmt.Errorf("field %q indexed string value len %d exceeds limit %d", s.field, len(key), indexdata.FieldStringRefMax)
		} else {
			*s.err = indexdata.ValidateIndexedStringKeyLen(len(key))
		}
		return
	}
	if s.state == nil {
		return
	}
	s.state.addValue(key, s.idx)
}

func (s buildFieldWriteSink) addFixed(key uint64) {
	if s.state == nil {
		return
	}
	s.state.addFixedValue(key, s.idx)
}

func (db *DB[K, V]) validateIndexedStringValues(val *V) error {
	if db.engine == nil || val == nil {
		return nil
	}
	ptr := unsafe.Pointer(val)
	for _, acc := range db.engine.indexedStringValidationAccess {
		var fieldErr error
		acc.writeBuild(ptr, buildFieldWriteSink{
			field: acc.name,
			err:   &fieldErr,
		})
		if fieldErr != nil {
			return fieldErr
		}
	}
	return nil
}

func idxFromKeyNoLock(strKey bool, strMap *strMapper, key []byte) uint64 {
	if strKey {
		return strMap.createIdxNoLock(string(key))
	}
	return binary.BigEndian.Uint64(key)
}

func (qe *queryEngine) buildLenIndex() {
	indexdata.ReleaseFieldStorageSlots(qe.lenIndex)
	slotCount := len(qe.indexedFieldAccess)
	qe.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	if qe.lenZeroComplement != nil {
		pooled.PutBoolSlice(qe.lenZeroComplement)
	}
	qe.lenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(qe.lenZeroComplement)

	for _, acc := range qe.indexedFieldAccess {
		if !acc.field.Slice {
			continue
		}
		storage, useZeroComplement := indexdata.RebuildLenFieldStorageFromOverlay(qe.universe, indexdata.NewFieldOverlayStorage(qe.index[acc.ordinal]))
		qe.lenIndex[acc.ordinal] = storage
		if useZeroComplement {
			qe.lenZeroComplement[acc.ordinal] = true
		}
	}
}

func (db *DB[K, V]) isLenZeroComplementField(field string) bool {
	if field == "" {
		return false
	}
	acc, ok := db.engine.indexedFieldMap[field]
	if !ok || acc.ordinal >= len(db.engine.lenZeroComplement) {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.engine.lenZeroComplement[acc.ordinal]
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
	plannerStats *plannerStatsSnapshot,
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
) (map[string]struct{}, map[string]struct{}, *plannerStatsSnapshot, *strMapper, bool, error) {
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
) (map[string]struct{}, map[string]struct{}, *plannerStatsSnapshot, *strMapper, bool, error) {

	var universe posting.List
	universe, err := posting.ReadFrom(reader)
	if err != nil {
		return nil, nil, nil, nil, false, fmt.Errorf("decode: reading universe: %w", err)
	}

	strmap, err := readStrMap(reader, defaultSnapshotStrMapCompactDepth)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}

	compatible, err := readFieldCompatibility(reader, qe.fields)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	measureCompatible, err := readFieldCompatibility(reader, qe.measureFields)
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

	skipFields := make(map[string]struct{}, len(qe.fields))
	for name := range qe.fields {
		_, hasRegular := indexes[name]
		_, hasNil := nilIndexes[name]
		if compatible[name] && (hasRegular || hasNil) {
			skipFields[name] = struct{}{}
		}
	}
	skipMeasureFields := make(map[string]struct{}, len(qe.measureFields))
	for name := range qe.measureFields {
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
		pooled.PutBoolSlice(qe.lenZeroComplement)
	}
	slotCount := len(qe.indexedFieldAccess)
	qe.index = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.nilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	measureSlotCount := len(qe.measureFieldAccess)
	qe.measure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for _, acc := range qe.indexedFieldAccess {
		if storage, ok := indexes[acc.name]; ok {
			qe.index[acc.ordinal] = storage
			delete(indexes, acc.name)
		}
		if storage, ok := nilIndexes[acc.name]; ok {
			qe.nilIndex[acc.ordinal] = storage
			delete(nilIndexes, acc.name)
		}
		if storage, ok := lenIndexes[acc.name]; ok {
			qe.lenIndex[acc.ordinal] = storage
			delete(lenIndexes, acc.name)
		}
	}
	for _, acc := range qe.measureFieldAccess {
		if storage, ok := measureIndexes[acc.name]; ok {
			qe.measure[acc.ordinal] = storage
			delete(measureIndexes, acc.name)
		}
	}
	indexdata.ReleaseFieldStorageMap(indexes)
	indexdata.ReleaseFieldStorageMap(nilIndexes)
	indexdata.ReleaseFieldStorageMap(lenIndexes)
	indexdata.ReleaseMeasureStorageMap(measureIndexes)
	qe.lenZeroComplement = detectLenZeroComplement(qe.lenIndex, qe.indexedFieldAccess)

	lenLoaded := true
	for name := range qe.fields {
		if _, ok := skipFields[name]; !ok {
			lenLoaded = false
			break
		}
	}
	if lenLoaded && !universe.IsEmpty() {
		for name, f := range qe.fields {
			if f == nil || !f.Slice {
				continue
			}
			if _, ok := skipFields[name]; !ok {
				continue
			}
			acc, ok := qe.indexedFieldMap[name]
			if !ok {
				lenLoaded = false
				break
			}
			if qe.lenIndex[acc.ordinal].KeyCount() == 0 {
				lenLoaded = false
				break
			}
		}
	}

	return skipFields, skipMeasureFields, plannerStats, strmap, lenLoaded, nil
}

func detectLenZeroComplement(indexes []indexdata.FieldStorage, access []indexedFieldAccessor) []bool {
	out := pooled.GetBoolSlice(len(access))[:len(access)]
	clear(out)
	if indexes == nil {
		return out
	}
	for _, acc := range access {
		if acc.ordinal >= len(indexes) {
			continue
		}
		if indexdata.NewFieldOverlayStorage(indexes[acc.ordinal]).LookupCardinality(indexdata.LenIndexNonEmptyKey) > 0 {
			out[acc.ordinal] = true
		}
	}
	return out
}

func readFieldCompatibility(reader *bufio.Reader, current map[string]*field) (map[string]bool, error) {
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

func sameFieldDefinition(a, b *field) bool {
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
	snap := qe.getSnapshot()
	universe := snap.universe.Borrow()

	if err := universe.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	if err := writeStrMapSnapshot(writer, snap.strmap); err != nil {
		return err
	}

	if err := writeFields(writer, qe.fields); err != nil {
		return err
	}
	if err := writeFields(writer, qe.measureFields); err != nil {
		return err
	}

	fieldNames := sortedFieldNames(snap.fieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(fieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range fieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		storage, _ := snap.fieldIndexStorage(field)
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	nilFieldNames := sortedFieldNames(snap.nilFieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(nilFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range nilFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.indexedFieldByName[field]
		if err := snap.nilIndex[acc.ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	lenFieldNames := sortedFieldNames(snap.lenFieldNameSet())

	if err := writeSidecarUvarint(writer, uint64(len(lenFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range lenFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.indexedFieldByName[field]
		if err := snap.lenIndex[acc.ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	measureFieldNames := sortedMapFieldNames(qe.measureFields)

	if err := writeSidecarUvarint(writer, uint64(len(measureFieldNames))); err != nil {
		return fmt.Errorf("encode: writing measure index family len: %w", err)
	}
	for _, field := range measureFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing measure field %q name: %w", field, err)
		}
		acc := qe.measureFieldMap[field]
		var storage indexdata.MeasureStorage
		if snap.measure == nil || acc.ordinal >= len(snap.measure) {
			storage = indexdata.MeasureStorage{}
		} else {
			storage = snap.measure[acc.ordinal]
		}
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing measure field %q: %w", field, err)
		}
	}

	statsVersion := qe.planner.statsVersion.Load()
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

const (
	strMapEncodingDense  byte = 1
	strMapEncodingSparse byte = 2
)

func writeFields(writer *bufio.Writer, fields map[string]*field) error {
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

func writeField(writer *bufio.Writer, name string, f *field) error {
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

func readField(reader *bufio.Reader) (string, *field, error) {
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
	if indexKind > uint64(^IndexKind(0)) {
		return "", nil, fmt.Errorf("invalid IndexKind %d", indexKind)
	}
	fieldIndexKind := IndexKind(indexKind)
	if err := validateIndexKind(fieldIndexKind); err != nil {
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
	return name, &field{
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

func writeStrMapSnapshot(writer *bufio.Writer, sm *strMapSnapshot) error {
	if sm == nil {
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing strmap next: %w", err)
		}
		if err := writer.WriteByte(strMapEncodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		return writeSidecarUvarint(writer, 0)
	}

	if err := writeSidecarUvarint(writer, sm.Next); err != nil {
		return fmt.Errorf("encode: writing strmap next: %w", err)
	}

	var chainInline [32]strMapSnapshotPersistNode
	chain, usedCount := strMapSnapshotPersistNodes(sm, chainInline[:0])

	if strMapSnapshotShouldPersistSparse(sm, usedCount) {
		if err := writer.WriteByte(strMapEncodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		if err := writeSidecarUvarint(writer, uint64(usedCount)); err != nil {
			return fmt.Errorf("encode: writing strmap sparse len: %w", err)
		}
		it := strMapSnapshotPersistIter{chain: chain}
		for {
			idx, value, ok := it.next()
			if !ok {
				break
			}
			if err := writeSidecarUvarint(writer, idx); err != nil {
				return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
			}
			if err := writeSidecarString(writer, value); err != nil {
				return fmt.Errorf("encode: writing strmap sparse string: %w", err)
			}
		}
		return nil
	}

	if err := writer.WriteByte(strMapEncodingDense); err != nil {
		return fmt.Errorf("encode: writing strmap encoding: %w", err)
	}

	denseLen := int(sm.Next) + 1
	if err := writeSidecarUvarint(writer, uint64(denseLen)); err != nil {
		return fmt.Errorf("encode: writing strmap dense len: %w", err)
	}

	flagIter := strMapSnapshotPersistIter{chain: chain}
	idx, _, ok := flagIter.next()
	for base := 0; base < denseLen; base += 8 {
		baseIdx := uint64(base)
		limit := baseIdx + 8
		var flags byte
		for ok && idx < limit {
			flags |= 1 << uint(idx-baseIdx)
			idx, _, ok = flagIter.next()
		}
		if err := writer.WriteByte(flags); err != nil {
			return fmt.Errorf("encode: writing strmap flags: %w", err)
		}
	}

	valueIter := strMapSnapshotPersistIter{chain: chain}
	for {
		_, value, ok := valueIter.next()
		if !ok {
			break
		}
		if err := writeSidecarString(writer, value); err != nil {
			return fmt.Errorf("encode: writing strmap string: %w", err)
		}
	}
	return nil
}

type strMapSnapshotPersistNode struct {
	start     uint64
	next      uint64
	strs      map[uint64]string
	denseStrs []string
	denseUsed []bool
}

func strMapSnapshotPersistNodes(sm *strMapSnapshot, inline []strMapSnapshotPersistNode) ([]strMapSnapshotPersistNode, int) {
	if sm == nil {
		return nil, 0
	}
	if len(sm.readDirs) > 0 {
		var nodes []strMapSnapshotPersistNode
		usedCount := 0
		pageCount := strMapReadPageCount(sm.Next)
		for page := 0; page < pageCount; page++ {
			readPage := sm.readPageAtNoLock(page)
			if readPage == nil {
				continue
			}
			nodes = append(nodes, strMapSnapshotPersistNode{
				start:     readPage.Start,
				next:      readPage.Next,
				strs:      readPage.Strs,
				denseStrs: readPage.DenseStrs,
				denseUsed: readPage.DenseUsed,
			})
			usedCount += readPage.usedCountNoLock()
		}
		if len(nodes) == 0 {
			return nil, usedCount
		}
		if len(nodes) <= cap(inline) {
			copy(inline[:len(nodes)], nodes)
			nodes = inline[:len(nodes)]
		}
		return nodes, usedCount
	}

	depth := 0
	usedCount := 0
	for cur := sm; cur != nil; cur = cur.base {
		depth++
		usedCount += strMapSnapshotOwnUsedCount(cur)
	}
	if depth == 0 {
		return nil, 0
	}

	var nodes []strMapSnapshotPersistNode
	if depth <= cap(inline) {
		nodes = inline[:depth]
	} else {
		nodes = make([]strMapSnapshotPersistNode, depth)
	}
	i := depth
	for cur := sm; cur != nil; cur = cur.base {
		i--
		start := cur.baseNextNoLock() + 1
		if cur.base == nil {
			start = 1
		}
		denseStrs, denseUsed := strMapDenseWindowNoLock(cur.DenseStrs, cur.DenseUsed, start, cur.Next)
		nodes[i] = strMapSnapshotPersistNode{
			start:     start,
			next:      cur.Next,
			strs:      cur.Strs,
			denseStrs: denseStrs,
			denseUsed: denseUsed,
		}
	}
	return nodes, usedCount
}

type strMapSnapshotPersistIter struct {
	chain      []strMapSnapshotPersistNode
	node       int
	mode       byte
	densePos   int
	denseLimit int
	sparse     []strMapSparseEntry
	sparsePos  int
}

func (it *strMapSnapshotPersistIter) next() (uint64, string, bool) {
	for it.node < len(it.chain) {
		switch it.mode {
		case 1:
			node := it.chain[it.node]
			for it.densePos < it.denseLimit {
				idx := uint64(it.densePos)
				it.densePos++
				pos := int(idx - node.start)
				if pos < 0 || pos >= len(node.denseUsed) || !node.denseUsed[pos] {
					continue
				}
				return idx, node.denseStrs[pos], true
			}
			it.mode = 0
			it.node++
			continue
		case 2:
			if it.sparsePos < len(it.sparse) {
				entry := it.sparse[it.sparsePos]
				it.sparsePos++
				return entry.idx, entry.value, true
			}
			it.sparse = it.sparse[:0]
			it.sparsePos = 0
			it.mode = 0
			it.node++
			continue
		}

		node := it.chain[it.node]
		if len(node.denseStrs) > 0 || len(node.denseUsed) > 0 {
			start := int(node.start)
			limit := start + min(len(node.denseStrs), len(node.denseUsed))
			if start >= limit {
				it.node++
				continue
			}
			it.densePos = start
			it.denseLimit = limit
			it.mode = 1
			continue
		}
		if len(node.strs) == 0 {
			it.node++
			continue
		}

		it.sparse = it.sparse[:0]
		for idx, value := range node.strs {
			if idx < node.start || idx > node.next {
				continue
			}
			it.sparse = append(it.sparse, strMapSparseEntry{idx: idx, value: value})
		}
		slices.SortFunc(it.sparse, func(a, b strMapSparseEntry) int {
			switch {
			case a.idx < b.idx:
				return -1
			case a.idx > b.idx:
				return 1
			default:
				return 0
			}
		})
		it.mode = 2
	}
	return 0, "", false
}

func readStrMap(reader *bufio.Reader, compactAt int) (*strMapper, error) {
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap next: %w", err)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap encoding: %w", err)
	}
	switch enc {
	case strMapEncodingDense:
		denseLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap dense len: %w", err)
		}
		if denseLen == 0 {
			return nil, fmt.Errorf("decode: invalid zero strmap dense len")
		}
		if denseLen > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("decode: strmap dense len overflows int: %v", denseLen)
		}
		if next >= denseLen {
			return nil, fmt.Errorf("decode: strmap next out of range: next=%v denseLen=%v", next, denseLen)
		}

		flags := make([]byte, (int(denseLen)+7)>>3)
		if _, err := io.ReadFull(reader, flags); err != nil {
			return nil, fmt.Errorf("decode: reading strmap flags: %w", err)
		}

		usedCount := 0
		for _, b := range flags {
			for x := b; x != 0; x &= x - 1 {
				usedCount++
			}
		}

		strs := make([]string, int(denseLen))
		used := make([]bool, int(denseLen))
		keys := make(map[string]uint64, max(0, usedCount-1))

		for i := 0; i < int(denseLen); i++ {
			if flags[i>>3]&(1<<uint(i&7)) == 0 {
				continue
			}
			s, err := readSidecarString(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap dense string idx=%d: %w", i, err)
			}
			strs[i] = s
			used[i] = true
			if i > 0 {
				keys[s] = uint64(i)
			}
		}

		strmap := newStrMapper(uint64(max(0, usedCount-1)), compactAt)
		strmap.replaceAllDenseNoLock(keys, strs, used, next)
		return strmap, nil

	case strMapEncodingSparse:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap sparse len: %w", err)
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("decode: strmap sparse len overflows int: %v", count)
		}
		keys := make(map[string]uint64, int(count))
		strs := make(map[uint64]string, int(count))
		for i := uint64(0); i < count; i++ {
			idx, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse idx entry=%d/%d: %w", i+1, count, err)
			}
			if idx == 0 || idx > next {
				return nil, fmt.Errorf("decode: strmap sparse idx out of range at entry=%d/%d: idx=%v next=%v", i+1, count, idx, next)
			}
			if _, exists := strs[idx]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse idx at entry=%d/%d: %v", i+1, count, idx)
			}
			s, err := readSidecarString(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse string idx=%d entry=%d/%d: %w", idx, i+1, count, err)
			}
			if _, exists := keys[s]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse key at entry=%d/%d: %q", i+1, count, s)
			}
			keys[s] = idx
			strs[idx] = s
		}
		strmap := newStrMapper(count, compactAt)
		strmap.replaceAllSparseNoLock(keys, strs, next)
		return strmap, nil

	default:
		return nil, fmt.Errorf("decode: invalid strmap encoding %v", enc)
	}
}

type strMapSparseEntry struct {
	idx   uint64
	value string
}

func strMapSnapshotShouldPersistSparse(sm *strMapSnapshot, usedCount int) bool {
	if sm == nil {
		return false
	}
	if sm.Next > uint64(^uint(0)>>1) {
		return true
	}
	denseLen := max(len(sm.DenseStrs), len(sm.DenseUsed))
	if denseLen <= 1 {
		denseLen = int(sm.Next) + 1
	}
	if denseLen <= 1 {
		return false
	}
	if usedCount == 0 {
		return false
	}
	return estimateSparseStrMapReverseBytes(usedCount) < estimateDenseStrMapReverseBytes(denseLen)
}

func strMapSnapshotOwnUsedCount(sm *strMapSnapshot) int {
	if sm == nil {
		return 0
	}
	if len(sm.Keys) > 0 {
		return len(sm.Keys)
	}
	if sm.Strs != nil {
		return len(sm.Strs)
	}
	limit := min(len(sm.DenseStrs), len(sm.DenseUsed))
	if limit <= 1 {
		return 0
	}
	count := 0
	for i := 1; i < limit; i++ {
		if sm.DenseUsed[i] {
			count++
		}
	}
	return count
}

func estimateDenseStrMapReverseBytes(denseLen int) uint64 {
	if denseLen <= 0 {
		return 0
	}
	return uint64(denseLen) * uint64(unsafe.Sizeof("")+unsafe.Sizeof(false))
}

func estimateSparseStrMapReverseBytes(usedCount int) uint64 {
	if usedCount <= 0 {
		return 0
	}
	const sparseMapLoadNumerator = 2
	const sparseMapLoadDenominator = 13 // ceil(n / 6.5) == ceil(2n / 13)
	buckets := (usedCount*sparseMapLoadNumerator + sparseMapLoadDenominator - 1) / sparseMapLoadDenominator
	if buckets < 1 {
		buckets = 1
	}
	bucketSize := uint64(8 + 8*unsafe.Sizeof(uint64(0)) + 8*unsafe.Sizeof("") + unsafe.Sizeof(uintptr(0)))
	return bucketSize * uint64(buckets)
}

func writePlannerStatsSnapshot(writer *bufio.Writer, s *plannerStatsSnapshot) error {
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

func readPlannerStatsSnapshot(reader *bufio.Reader, compatible map[string]bool) (*plannerStatsSnapshot, error) {
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

	out := &plannerStatsSnapshot{
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
