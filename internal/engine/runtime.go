package engine

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/rebuild"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"github.com/vapstack/rbi/internal/wexec"
	"go.etcd.io/bbolt"
)

const randStreamMix uint64 = 0x9e3779b97f4a7c15

type Index struct {
	snapshot               *snapshot.Registry
	schema                 *schema.Schema
	strKey                 bool
	strMap                 *strmap.Mapper
	index                  []indexdata.FieldStorage
	nilIndex               []indexdata.FieldStorage
	lenIndex               []indexdata.FieldStorage
	lenZeroComplement      []bool
	measure                []indexdata.MeasureStorage
	universe               posting.List
	lenIndexLoaded         bool
	matPredCacheMaxEntries int
	matPredCacheMaxCard    uint64

	exec *qexec.Runtime
}

type Config struct {
	Schema *schema.Schema
	StrKey bool

	SnapshotStats bool

	SnapshotMaterializedPredCacheMaxEntries     int
	SnapshotMaterializedPredCacheMaxCardinality int

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	AnalyzeInterval  time.Duration
	TraceSink        func(qexec.TraceEvent)
	TraceSampleEvery int

	StrMapCompactAt int
}

type LoadErrors struct {
	Stale   error
	Invalid error
}

type LoadResult struct {
	SkipFields        map[string]struct{}
	SkipMeasureFields map[string]struct{}
	PlannerStats      *qexec.PlannerStatsSnapshot
}

type BuildResult struct {
	Stats     bool
	BuildTime time.Duration
	BuildRPS  int
}

type KeySet struct {
	IDs    []uint64
	lookup strmap.Lookup
}

type DBStats struct {
	KeyCount uint64
	Sequence uint64
	HasLast  bool
	LastID   uint64
	LastKey  string
}

type IndexStats struct {
	UniqueFieldKeys        map[string]uint64
	Size                   uint64
	FieldSize              map[string]uint64
	FieldKeyBytes          map[string]uint64
	FieldTotalCardinality  map[string]uint64
	FieldApproxStructBytes map[string]uint64
	FieldApproxHeapBytes   map[string]uint64
	EntryCount             uint64
	KeyBytes               uint64
	PostingCardinality     uint64
	ApproxStructBytes      uint64
	ApproxHeapBytes        uint64
}

type PlannerStats struct {
	Version             uint64
	GeneratedAt         time.Time
	UniverseCardinality uint64
	FieldCount          int
	Fields              map[string]qexec.PlannerFieldStats
	AnalyzeInterval     time.Duration
	TraceSampleEvery    uint64
}

type SnapshotStats struct {
	Sequence     uint64
	UniverseCard uint64
	RegistrySize int
	PinnedRefs   int
}

type StagedSnapshot struct {
	view *snapshot.View
}

type DecodeFunc func([]byte) (unsafe.Pointer, error)
type ReleaseFunc func(unsafe.Pointer)

func NewIndex(cfg Config) (*Index, error) {
	if !cfg.Schema.HasQueryFields() {
		return nil, nil
	}

	var sm *strmap.Mapper
	if cfg.StrKey {
		sm = strmap.New(0, cfg.StrMapCompactAt)
	}

	r := &Index{
		schema:                 cfg.Schema,
		strKey:                 cfg.StrKey,
		strMap:                 sm,
		universe:               posting.List{},
		matPredCacheMaxEntries: max(0, cfg.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxCard:    qcache.MaterializedPredMaxCardinality(cfg.SnapshotMaterializedPredCacheMaxCardinality),
		snapshot:               snapshot.NewRegistry(cfg.SnapshotStats),
		exec: qexec.NewRuntime(qexec.Config{
			Schema:                         cfg.Schema,
			NumericRangeBucketSize:         cfg.NumericRangeBucketSize,
			NumericRangeBucketMinFieldKeys: cfg.NumericRangeBucketMinFieldKeys,
			NumericRangeBucketMinSpanKeys:  cfg.NumericRangeBucketMinSpanKeys,
			AnalyzeInterval:                cfg.AnalyzeInterval,
			TraceSink:                      cfg.TraceSink,
			TraceSampleEvery:               cfg.TraceSampleEvery,
		}),
	}
	slotCount := len(cfg.Schema.Indexed)
	r.index = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	r.nilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	r.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	r.lenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(r.lenZeroComplement)
	measureSlotCount := len(cfg.Schema.Measures)
	r.measure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	return r, nil
}

func (index *Index) SnapshotCacheConfig() snapshot.CacheConfig {
	return snapshot.CacheConfig{
		MatPredMaxEntries: index.matPredCacheMaxEntries,
		MatPredMaxCard:    index.matPredCacheMaxCard,
	}
}

func (index *Index) ConfigureWrite(cfg *wexec.Config, broken *atomic.Bool, logger *log.Logger, uniqueViolation error, brokenErr error) {
	cfg.Indexed = true
	cfg.StrMap = index.strMap
	cfg.Unique = wexec.UniqueContext{
		Schema:          index.schema,
		Current:         index.snapshot.Current,
		UniqueViolation: uniqueViolation,
	}
	cfg.SnapshotOps = wexec.SnapshotOps{
		Manager:     index.snapshot,
		Schema:      index.schema,
		CacheConfig: index.SnapshotCacheConfig,
		StrMap:      index.strMap,
		PatchFields: index.schema.Patch.Fields,
	}
	cfg.IndexPublishOps = wexec.IndexPublishOps{
		PublishCommitted: func(seq uint64, op string, snap *snapshot.View) error {
			return index.PublishCommittedView(broken, logger, brokenErr, seq, op, snap)
		},
	}
}

func (index *Index) ValidateStringValues(ptr unsafe.Pointer) error {
	for _, acc := range index.schema.StringValidation {
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

func (index *Index) Count(exprs ...qx.Expr) (uint64, error) {
	prepared, err := qagg.PrepareCount(index.schema, exprs...)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()

	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)
	return qagg.Count(view, prepared, true)
}

func (index *Index) Aggregate(q *qx.QX) (qagg.Result, error) {
	prepared, err := qagg.Prepare(q, index.schema)
	if err != nil {
		return qagg.Result{}, err
	}
	defer prepared.Release()

	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)

	return qagg.Execute(view, snap, prepared)
}

func (keys *KeySet) StringAt(i int) string {
	s, ok := keys.lookup.String(keys.IDs[i])
	if !ok {
		panic("rbi: no string key associated with snapshot idx")
	}
	return s
}

func (index *Index) QueryKeys(q *qx.QX, fn func(KeySet) error) error {
	prepared, shape, err := index.prepareQuery(q)
	if err != nil {
		return err
	}
	defer prepared.Release()

	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	keys, err := index.queryKeysOnSnapshot(snap, &shape, false)
	if err != nil {
		return err
	}
	return fn(keys)
}

func (index *Index) Query(q *qx.QX, bolt *bbolt.DB, bucketName []byte, unavailable func() error, fn func(*bbolt.Tx, KeySet) error) error {
	prepared, shape, err := index.prepareQuery(q)
	if err != nil {
		return err
	}
	defer prepared.Release()

	guard := index.snapshot.PinGuardLock()
	tx, err := bolt.Begin(false)
	if err != nil {
		guard.Unlock()
		return fmt.Errorf("tx error: %w", err)
	}

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		guard.Unlock()
		_ = tx.Rollback()
		return fmt.Errorf("bucket does not exist")
	}

	seq := bucket.Sequence()

	snap, ref, ok := guard.PinBySeq(seq)
	if !ok {
		guard.Unlock()
		_ = tx.Rollback()
		if err = unavailable(); err != nil {
			return err
		}
		return fmt.Errorf("snapshot sequence %d is not available", seq)
	}
	guard.Unlock()

	defer index.snapshot.Unpin(seq, ref)
	defer func() { _ = tx.Rollback() }()

	keys, err := index.queryKeysOnSnapshot(snap, &shape, true)
	if err != nil {
		return err
	}
	if len(keys.IDs) == 0 {
		return nil
	}
	return fn(tx, keys)
}

func (index *Index) prepareQuery(q *qx.QX) (*qir.Query, qir.Shape, error) {
	if q == nil {
		return nil, qir.Shape{}, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, index.schema.IndexedByName)
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func (index *Index) queryKeysOnSnapshot(snap *snapshot.View, q *qir.Shape, tryEmpty bool) (KeySet, error) {
	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)

	if tryEmpty && !index.exec.TraceSamplingEnabled() {
		if empty, err := view.TryQueryEmptyOnSnapshot(q); empty || err != nil {
			return KeySet{}, err
		}
	}

	ids, err := view.Query(q, true)
	if err != nil {
		return KeySet{}, err
	}
	if len(ids) == 0 {
		return KeySet{}, nil
	}
	if !index.strKey {
		return KeySet{IDs: ids}, nil
	}
	return KeySet{IDs: ids, lookup: snap.StrMap.Lookup()}, nil
}

func (index *Index) ScanUintKeys(seek uint64, fn func(uint64) (bool, error)) error {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	universe := snap.Universe
	iter := universe.Iter()
	defer iter.Release()

	for iter.HasNext() {
		idx := iter.Next()
		if idx < seek {
			continue
		}
		cont, err := fn(idx)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

func (index *Index) ScanStringKeys(seek string, invalidKeyErr error, fn func(string) (bool, error)) error {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	universe := snap.Universe
	if universe.IsEmpty() {
		return nil
	}

	iter := universe.Iter()
	defer iter.Release()

	return scanStringKeys(snap.StrMap, universe, iter, seek, invalidKeyErr, fn)
}

func scanStringKeys(snap *strmap.Snapshot, universe posting.List, iter posting.Iterator, seek string, invalidKeyErr error, fn func(string) (bool, error)) error {
	next := snap.Next()
	card := universe.Cardinality()
	minIdx, hasMin := universe.Minimum()
	maxIdx, hasMax := universe.Maximum()

	lookup := snap.Lookup()
	if card == next && card > 0 && hasMin && hasMax && minIdx == 1 && maxIdx == next {
		for idx := uint64(1); idx <= next; idx++ {
			s, ok := lookup.String(idx)
			if !ok {
				return fmt.Errorf("%w: %v", invalidKeyErr, idx)
			}
			cont, err := emitScannedStringKey(seek, s, fn)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		return nil
	}

	for iter.HasNext() {
		idx := iter.Next()
		s, ok := lookup.String(idx)
		if !ok {
			return fmt.Errorf("%w: %v", invalidKeyErr, idx)
		}
		cont, err := emitScannedStringKey(seek, s, fn)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func emitScannedStringKey(seek string, s string, fn func(string) (bool, error)) (bool, error) {
	if s < seek {
		return true, nil
	}
	return fn(s)
}

func (index *Index) BuildIndex(bolt *bbolt.DB, bucket []byte, skipFields map[string]struct{}, skipMeasureFields map[string]struct{}, decode DecodeFunc, release ReleaseFunc) (BuildResult, error) {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return BuildResult{}, err
	}
	result, err := rebuild.Build(rebuild.Config{
		Bolt:              bolt,
		Bucket:            bucket,
		Schema:            index.schema,
		Current:           index.snapshot.Current(),
		StrKey:            index.strKey,
		StrMap:            index.strMap,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasureFields,
		Decode:            rebuild.DecodeFunc(decode),
		Release:           rebuild.ReleaseFunc(release),
	}, rebuild.State{
		Index:             index.index,
		NilIndex:          index.nilIndex,
		LenIndex:          index.lenIndex,
		LenZeroComplement: index.lenZeroComplement,
		Measure:           index.measure,
		Universe:          index.universe,
		LenLoaded:         index.lenIndexLoaded,
	})
	if err != nil {
		return BuildResult{}, err
	}
	if result.Publish {
		st := result.Storage
		result.Storage = snapshot.Storage{}
		index.publishStorageSnapshotNoLock(seq, st)
	}
	index.lenIndexLoaded = result.LenLoaded
	return BuildResult{
		Stats:     result.Stats,
		BuildTime: result.BuildTime,
		BuildRPS:  result.BuildRPS,
	}, nil
}

func (index *Index) LoadIndex(file string, dbPath string, bucket []byte, currentSeq uint64, compactAt int, errs LoadErrors) (LoadResult, error) {
	result, err := persist.Load(persist.LoadConfig{
		File:            file,
		DBPath:          dbPath,
		Bucket:          bucket,
		CurrentSeq:      currentSeq,
		Schema:          index.schema,
		StrMapCompactAt: compactAt,
		Errors: persist.Errors{
			Stale:   errs.Stale,
			Invalid: errs.Invalid,
		},
	})
	if err != nil {
		return LoadResult{}, err
	}
	installed := false
	defer func() {
		if !installed {
			result.Storage.Release()
		}
	}()

	index.releaseCurrentStorage()
	index.lenIndexLoaded = result.LenLoaded

	if index.strKey {
		index.strMap = result.StrMap
	}
	st := result.Storage
	result.Storage = snapshot.Storage{}
	index.publishStorageSnapshotNoLock(currentSeq, st)
	installed = true

	return LoadResult{
		SkipFields:        result.SkipFields,
		SkipMeasureFields: result.SkipMeasureFields,
		PlannerStats:      result.PlannerStats,
	}, nil
}

func (index *Index) StoreIndex(file string, bolt *bbolt.DB, bucket []byte) error {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return fmt.Errorf("store: reading bucket sequence: %w", err)
	}
	snap, snapSeq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(snapSeq, ref)

	if snap.Seq != seq {
		return fmt.Errorf("store: snapshot sequence mismatch: snapshot=%d bucket=%d", snap.Seq, seq)
	}
	statsVersion := index.exec.StatsVersion.Load()
	if statsVersion == 0 {
		statsVersion = 1
	} else {
		statsVersion++
	}
	plannerStats := index.exec.PlannerStatsSnapshotForPersist(snap, statsVersion)
	return persist.Store(persist.StoreConfig{
		File:         file,
		BucketSeq:    seq,
		Schema:       index.schema,
		Snapshot:     snap,
		PlannerStats: plannerStats,
	})
}

func (index *Index) PublishCurrentSequenceSnapshot(bolt *bbolt.DB, bucket []byte) error {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return err
	}
	var strSnap *strmap.Snapshot
	if index.strMap != nil {
		strSnap = index.strMap.Snapshot()
	}
	prev := index.snapshot.Current()
	var snap *snapshot.View
	if prev != nil {
		lenZeroComplement := pooled.GetBoolSlice(len(index.schema.Indexed))[:len(index.schema.Indexed)]
		clear(lenZeroComplement)
		copy(lenZeroComplement, index.lenZeroComplement)
		snap = snapshot.NewView(seq, prev, index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(index.index, len(index.schema.Indexed)),
			NilIndex:          indexdata.CloneFieldStorageSlots(index.nilIndex, len(index.schema.Indexed)),
			LenIndex:          indexdata.CloneFieldStorageSlots(index.lenIndex, len(index.schema.Indexed)),
			LenZeroComplement: lenZeroComplement,
			Measure:           indexdata.CloneMeasureStorageSlots(index.measure, len(index.schema.Measures)),
			Universe:          index.universe,
			StrMap:            strSnap,
		})
	} else {
		snap = snapshot.NewView(seq, nil, index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
			Index:             index.index,
			NilIndex:          index.nilIndex,
			LenIndex:          index.lenIndex,
			LenZeroComplement: index.lenZeroComplement,
			Measure:           index.measure,
			Universe:          index.universe,
			StrMap:            strSnap,
		})
	}
	index.installViewNoLock(snap)
	if index.strMap != nil {
		index.strMap.MarkCommittedPublished(strSnap)
	}
	return nil
}

func (index *Index) publishStorageSnapshotNoLock(seq uint64, st snapshot.Storage) {
	if index.strMap != nil {
		st.StrMap = index.strMap.Snapshot()
	}
	snap := snapshot.NewView(seq, index.snapshot.Current(), index.schema, index.SnapshotCacheConfig(), st)
	index.installViewNoLock(snap)
	if index.strMap != nil {
		index.strMap.MarkCommittedPublished(st.StrMap)
	}
}

func (index *Index) installViewNoLock(s *snapshot.View) {
	index.index = s.Index
	index.nilIndex = s.NilIndex
	index.lenIndex = s.LenIndex
	index.lenZeroComplement = s.LenZeroComplement
	index.measure = s.Measure
	index.universe = s.Universe
	index.snapshot.Publish(s)
}

func (index *Index) StageTruncate(seq uint64) StagedSnapshot {
	slotCount := len(index.schema.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	measureSlotCount := len(index.schema.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	nextUniverse := posting.List{}
	var nextStrMap *strmap.Snapshot
	if index.strKey {
		nextStrMap = strmap.EmptySnapshot()
	}
	snap := snapshot.NewView(seq, index.snapshot.Current(), index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
		Index:             nextIndex,
		NilIndex:          nextNilIndex,
		LenIndex:          nextLenIndex,
		LenZeroComplement: nextLenZeroComplement,
		Measure:           nextMeasure,
		Universe:          nextUniverse,
		StrMap:            nextStrMap,
	})
	index.snapshot.Stage(snap)
	return StagedSnapshot{view: snap}
}

func (index *Index) DropStaged(seq uint64) {
	index.snapshot.DropStaged(seq)
}

func (index *Index) PublishCommittedStaged(broken *atomic.Bool, logger *log.Logger, brokenErr error, seq uint64, op string, staged StagedSnapshot) error {
	if index.strKey {
		index.strMap.Truncate()
	}
	return index.PublishCommittedView(broken, logger, brokenErr, seq, op, staged.view)
}

func (index *Index) PublishCommittedView(broken *atomic.Bool, logger *log.Logger, brokenErr error, seq uint64, op string, snap *snapshot.View) (err error) {
	defer index.recoverCommittedPublishPanic(broken, logger, brokenErr, seq, op, &err)
	index.installViewNoLock(snap)
	if index.strMap != nil {
		index.strMap.MarkCommittedPublished(snap.StrMap)
	}
	return nil
}

func (index *Index) recoverCommittedPublishPanic(broken *atomic.Bool, logger *log.Logger, brokenErr error, seq uint64, op string, err *error) {
	if v := recover(); v != nil {
		if broken.CompareAndSwap(false, true) {
			logger.Printf("rbi: index entered broken state: post-commit snapshot publish failed (%v): %v", op, v)
			index.stopAnalyzerSignal()
		}
		*err = brokenErr
		index.snapshot.DropStaged(seq)
	}
}

func (index *Index) PublishLoadedPlannerStats(s *qexec.PlannerStatsSnapshot) {
	index.exec.PublishLoadedPlannerStats(s, index.snapshot.Current())
}

func (index *Index) RefreshPlannerStats(unavailable func() error) error {
	index.exec.Analyzer.Lock()
	defer index.exec.Analyzer.Unlock()

	if err := unavailable(); err != nil {
		return err
	}
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	index.exec.RefreshPlannerStatsOnSnapshot(snap)
	return nil
}

func (index *Index) RefreshPlannerStatsLocked() {
	version := index.exec.StatsVersion.Add(1)
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)
	s := index.exec.BuildPlannerStatsSnapshot(snap, version)
	index.exec.Stats.Store(s)
}

func (index *Index) StartAnalyzeLoop(refresh func() error, terminal func(error) bool, resetFailure func(error) bool) {
	interval := index.exec.Analyzer.Interval
	if interval <= 0 {
		return
	}
	if index.exec.Analyzer.Stop != nil {
		return
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	index.exec.Analyzer.Stop = stop
	index.exec.Analyzer.Done = done

	go index.runPlannerAnalyzeLoop(stop, done, interval, refresh, terminal, resetFailure)
}

func (index *Index) StopAnalyzeLoop() {
	stop := index.exec.Analyzer.Stop
	done := index.exec.Analyzer.Done

	if stop != nil {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	if done != nil {
		<-done
	}

	index.exec.Analyzer.Stop = nil
	index.exec.Analyzer.Done = nil
}

func (index *Index) stopAnalyzerSignal() {
	if stop := index.exec.Analyzer.Stop; stop != nil {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
}

func (index *Index) runPlannerAnalyzeLoop(stop <-chan struct{}, done chan<- struct{}, base time.Duration, refresh func() error, terminal func(error) bool, resetFailure func(error) bool) {
	defer close(done)

	rng := newRand(time.Now().UnixNano())
	failures := 0

	timer := time.NewTimer(nextAnalyzeDelay(base, failures, rng))
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		err := refresh()
		if err != nil {
			if terminal(err) {
				return
			}
			if resetFailure(err) {
				failures = 0
			} else {
				failures++
			}
		} else {
			failures = 0
		}

		timer.Reset(nextAnalyzeDelay(base, failures, rng))
	}
}

func nextAnalyzeDelay(base time.Duration, failures int, rng *rand.Rand) time.Duration {
	d := base
	if failures > 0 {
		pow := failures
		if pow > 3 {
			pow = 3
		}
		d = base * time.Duration(1<<pow)
		m := base * 8
		if d > m {
			d = m
		}
	}
	return addPositiveJitter(d, rng)
}

func addPositiveJitter(d time.Duration, rng *rand.Rand) time.Duration {
	if d <= 0 {
		return d
	}
	j := d / 5
	if j <= 0 {
		return d
	}
	return d + time.Duration(rng.Int64N(int64(j)+1))
}

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
}

func (index *Index) DBStats() DBStats {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	out := DBStats{Sequence: snap.Seq}
	out.KeyCount = snap.UniverseCardinality()
	if snap.Universe.IsEmpty() {
		return out
	}
	maxIdx, _ := snap.Universe.Maximum()
	out.LastID = maxIdx
	out.HasLast = true
	if index.strKey {
		if key, ok := snap.StrMap.String(maxIdx); ok {
			out.LastKey = key
		} else {
			out.HasLast = false
		}
	}
	return out
}

func (index *Index) IndexStats() IndexStats {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)
	return index.indexStats(snap)
}

func (index *Index) indexStats(snap *snapshot.View) IndexStats {
	idx := IndexStats{
		UniqueFieldKeys:        make(map[string]uint64),
		FieldSize:              make(map[string]uint64),
		FieldKeyBytes:          make(map[string]uint64),
		FieldTotalCardinality:  make(map[string]uint64),
		FieldApproxStructBytes: make(map[string]uint64),
		FieldApproxHeapBytes:   make(map[string]uint64),
	}
	sharedStructBytes := indexdata.FieldStorageSlotsApproxBytes(snap.Index) + indexdata.FieldStorageSlotsApproxBytes(snap.NilIndex)
	fieldCount := len(index.schema.Indexed)
	sharedStructPerField := uint64(0)
	sharedStructRemainder := uint64(0)
	if fieldCount > 0 {
		sharedStructPerField = sharedStructBytes / uint64(fieldCount)
		sharedStructRemainder = sharedStructBytes % uint64(fieldCount)
	}

	for i, acc := range index.schema.Indexed {
		name := acc.Name
		fieldStats := snap.Index[acc.Ordinal].Stats(true)
		nilStats := snap.NilIndex[acc.Ordinal].Stats(false)

		unique := fieldStats.Unique + nilStats.Unique
		size := fieldStats.PostingBytes + nilStats.PostingBytes
		keyLen := fieldStats.KeyBytes + nilStats.KeyBytes
		card := fieldStats.PostingCardinality + nilStats.PostingCardinality

		idx.UniqueFieldKeys[name] = unique
		idx.FieldSize[name] = size
		idx.FieldKeyBytes[name] = keyLen
		idx.FieldTotalCardinality[name] = card
		fieldStructBytes := fieldStats.ApproxStructBytes + nilStats.ApproxStructBytes + sharedStructPerField
		if uint64(i) < sharedStructRemainder {
			fieldStructBytes++
		}
		idx.FieldApproxStructBytes[name] = fieldStructBytes
		idx.FieldApproxHeapBytes[name] = size + keyLen + fieldStructBytes

		idx.Size += size
		idx.EntryCount += fieldStats.EntryCount + nilStats.EntryCount
		idx.KeyBytes += keyLen
		idx.PostingCardinality += card
		idx.ApproxStructBytes += fieldStructBytes
	}
	idx.ApproxHeapBytes = idx.Size + idx.KeyBytes + idx.ApproxStructBytes
	return idx
}

func (index *Index) PlannerStats() PlannerStats {
	var out PlannerStats
	out.Fields = make(map[string]qexec.PlannerFieldStats)

	if ps := index.exec.Stats.Load(); ps != nil {
		out.Version = ps.Version
		out.GeneratedAt = ps.GeneratedAt
		out.UniverseCardinality = ps.UniverseCardinality
		out.FieldCount = len(ps.Fields)
		if out.FieldCount > 0 {
			out.Fields = make(map[string]qexec.PlannerFieldStats, out.FieldCount)
			for k, v := range ps.Fields {
				out.Fields[k] = v
			}
		}
	}
	out.TraceSampleEvery = index.exec.Tracer.SampleEvery()
	index.exec.Analyzer.Lock()
	out.AnalyzeInterval = index.exec.Analyzer.Interval
	index.exec.Analyzer.Unlock()
	return out
}

func (index *Index) SnapshotStats() SnapshotStats {
	if !index.snapshot.StatsEnabled() {
		return SnapshotStats{}
	}
	snap, seq, ref := index.snapshot.PinCurrent()
	if snap == nil {
		return SnapshotStats{}
	}
	defer index.snapshot.Unpin(seq, ref)

	stats := index.snapshot.Stats(snap, ref)
	return SnapshotStats{
		Sequence:     stats.Sequence,
		UniverseCard: stats.UniverseCard,
		RegistrySize: stats.RegistrySize,
		PinnedRefs:   stats.PinnedRefs,
	}
}

func (index *Index) releaseCurrentStorage() {
	indexdata.ReleaseFieldStorageSlots(index.index)
	indexdata.ReleaseFieldStorageSlots(index.nilIndex)
	indexdata.ReleaseFieldStorageSlots(index.lenIndex)
	indexdata.ReleaseMeasureStorageSlots(index.measure)
	if index.lenZeroComplement != nil {
		pooled.ReleaseBoolSlice(index.lenZeroComplement)
	}
	index.universe.Release()
}

func currentBucketSequence(bolt *bbolt.DB, bucket []byte) (uint64, error) {
	var seq uint64
	if err := bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		seq = b.Sequence()
		return nil
	}); err != nil {
		return 0, err
	}
	return seq, nil
}
