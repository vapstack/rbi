package engine

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/rebuild"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
	"go.etcd.io/bbolt"
)

type Index struct {
	snapshot               *snapshot.Registry
	schema                 *schema.Schema
	strKey                 bool
	keyMode                qexec.KeyMode
	index                  []indexdata.FieldStorage
	keyIndex               indexdata.FieldStorage
	keyIndexLoaded         bool
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
	Schema      *schema.Schema
	StrKey      bool
	StrKeyIndex bool

	SnapshotStats bool

	SnapshotMaterializedPredCacheMaxEntries     int
	SnapshotMaterializedPredCacheMaxCardinality int

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	AnalyzeInterval  time.Duration
	TraceSink        func(rbitrace.Event)
	TraceSampleEvery int
}

type LoadResult struct {
	SkipFields        map[string]struct{}
	SkipMeasureFields map[string]struct{}
	PlannerStats      *rbistats.PlannerSnapshot
}

type BuildResult struct {
	Stats     bool
	BuildTime time.Duration
	BuildRPS  int
}

type DBStats struct {
	KeyCount uint64
	Sequence uint64
	HasLast  bool
	LastKey  keycodec.DataKey
}

type StagedSnapshot struct {
	view *snapshot.View
}

type (
	DecodeFunc  func([]byte) (unsafe.Pointer, error)
	ReleaseFunc func(unsafe.Pointer)
)

func NewIndex(cfg Config) (*Index, error) {
	keyMode := qexec.KeyModeNone
	if cfg.StrKey && cfg.StrKeyIndex {
		keyMode = qexec.KeyModeString
	} else if !cfg.StrKey && cfg.Schema.HasQueryFields() {
		keyMode = qexec.KeyModeNumeric
	}
	if !cfg.Schema.HasQueryFields() && keyMode == qexec.KeyModeNone {
		return nil, nil
	}

	r := &Index{
		schema:                 cfg.Schema,
		strKey:                 cfg.StrKey,
		keyMode:                keyMode,
		universe:               posting.List{},
		matPredCacheMaxEntries: max(0, cfg.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxCard:    qcache.MaterializedPredMaxCardinality(cfg.SnapshotMaterializedPredCacheMaxCardinality),
		snapshot:               snapshot.NewRegistry(cfg.SnapshotStats),
		exec: qexec.NewRuntime(qexec.Config{
			StrKey:                         cfg.StrKey,
			KeyMode:                        keyMode,
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

func (index *Index) ConfigureWrite(cfg *wexec.Config, broken *atomic.Bool, logger *log.Logger) {
	cfg.Indexed = true
	cfg.Unique = wexec.UniqueContext{
		Schema:  index.schema,
		Current: index.snapshot.Current,
	}
	cfg.SnapshotOps = wexec.SnapshotOps{
		Manager:     index.snapshot,
		Schema:      index.schema,
		CacheConfig: index.SnapshotCacheConfig(),
		PatchFields: index.schema.Patch.Fields,
		StrKeyIndex: index.keyMode == qexec.KeyModeString,
	}
	cfg.PublishCommitted = func(seq uint64, op string, snap *snapshot.View) error {
		return index.PublishCommittedView(broken, logger, seq, op, snap)
	}
}

func (index *Index) ValidateStringValues(ptr unsafe.Pointer) error {
	for _, acc := range index.schema.StringValidation {
		if err := acc.Validate(ptr); err != nil {
			return err
		}
	}
	return nil
}

func (index *Index) ScanKeys(seek uint64, fn func(uint64) (bool, error)) error {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	it := snap.Universe.Borrow().AdvancingIter()
	defer it.Release()

	it.AdvanceIfNeeded(seek)
	for it.HasNext() {
		id := it.Next()
		more, err := fn(id)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

func (index *Index) HasStringKeyIndex() bool {
	return index.keyMode == qexec.KeyModeString
}

func (index *Index) ScanStringKeys(seek string, fn func(string) (bool, error)) error {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	view := indexdata.NewFieldIndexViewFromStorage(snap.KeyIndex)
	cur := view.NewCursor(view.RangeByRanks(view.LowerBound(seek), view.KeyCount()), false)
	for {
		key, ids, _, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			return nil
		}
		more, err := fn(strings.Clone(key.UnsafeString()))
		if !single {
			ids.Release()
		}
		if err != nil || !more {
			return err
		}
	}
}

func (index *Index) Count(exprs ...qx.Expr) (uint64, error) {
	prepared, err := qagg.PrepareCount(index.exec, exprs...)
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
	prepared, err := qagg.Prepare(q, index.schema, index.exec)
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

func (index *Index) QueryKeys(q *qx.QX, fn func([]uint64) error) error {
	prepared, shape, err := index.prepareQuery(q)
	if err != nil {
		return err
	}
	defer prepared.Release()

	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)

	ids, err := index.queryKeysOnSnapshot(snap, &shape, false)
	if err != nil {
		return err
	}
	return fn(ids)
}

func (index *Index) Query(q *qx.QX, bolt *bbolt.DB, bucketName []byte, unavailable func() error, fn func(*bbolt.Tx, []uint64) error) error {
	prepared, shape, err := index.prepareQuery(q)
	if err != nil {
		return err
	}
	defer prepared.Release()

	for {
		tx, err := bolt.Begin(false)
		if err != nil {
			return fmt.Errorf("tx error: %w", err)
		}

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			_ = tx.Rollback()
			return fmt.Errorf("bucket does not exist")
		}

		seq := bucket.Sequence()
		snap, ref, ok := index.snapshot.PinBySeq(seq)
		if !ok {
			_ = tx.Rollback()
			if err = unavailable(); err != nil {
				return err
			}
			continue
		}

		// deferred unpin/rollback lead to heap escape and 2 more allocs

		ids, err := index.queryKeysOnSnapshot(snap, &shape, true)
		if err != nil {
			index.snapshot.Unpin(seq, ref)
			_ = tx.Rollback()
			return err
		}
		if len(ids) == 0 {
			index.snapshot.Unpin(seq, ref)
			_ = tx.Rollback()
			return nil
		}
		err = fn(tx, ids)
		index.snapshot.Unpin(seq, ref)
		_ = tx.Rollback()
		return err
	}
}

func (index *Index) prepareQuery(q *qx.QX) (*qir.Query, qir.Shape, error) {
	if q == nil {
		return nil, qir.Shape{}, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, index.exec)
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func (index *Index) ResolveField(name string) (qir.FieldInfo, bool) {
	return index.exec.ResolveField(name)
}

func (index *Index) queryKeysOnSnapshot(snap *snapshot.View, q *qir.Shape, tryEmpty bool) ([]uint64, error) {
	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)

	if tryEmpty && !index.exec.TraceSamplingEnabled() {
		if empty, err := view.TryQueryEmptyOnSnapshot(q); empty || err != nil {
			return nil, err
		}
	}

	ids, err := view.Query(q, true)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

func (index *Index) BuildIndex(
	bolt *bbolt.DB,
	dataBucket, strmapBucket []byte,
	skipFields, skipMeasureFields map[string]struct{},
	decode DecodeFunc,
	release ReleaseFunc,
) (BuildResult, error) {

	seq, err := currentBucketSequence(bolt, dataBucket)
	if err != nil {
		return BuildResult{}, err
	}
	result, err := rebuild.Build(rebuild.Config{
		Bolt:              bolt,
		DataBucket:        dataBucket,
		StrMapBucket:      strmapBucket,
		Schema:            index.schema,
		Current:           index.snapshot.Current(),
		StrKey:            index.strKey,
		StrKeyIndex:       index.keyMode == qexec.KeyModeString,
		KeyIndexLoaded:    index.keyIndexLoaded,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasureFields,
		Decode:            rebuild.DecodeFunc(decode),
		Release:           rebuild.ReleaseFunc(release),
	}, rebuild.State{
		Index:             index.index,
		KeyIndex:          index.keyIndex,
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
		if index.keyMode == qexec.KeyModeString {
			index.keyIndexLoaded = result.KeyIndexLoaded
		}
	}
	index.lenIndexLoaded = result.LenLoaded
	return BuildResult{
		Stats:     result.Stats,
		BuildTime: result.BuildTime,
		BuildRPS:  result.BuildRPS,
	}, nil
}

func (index *Index) LoadIndex(file string, dbPath string, bucket []byte, currentSeq uint64, uid [persist.UIDLen]byte) (LoadResult, error) {
	result, err := persist.Load(persist.LoadConfig{
		File:        file,
		DBPath:      dbPath,
		Bucket:      bucket,
		CurrentSeq:  currentSeq,
		UID:         uid,
		Schema:      index.schema,
		StrKey:      index.strKey,
		StrKeyIndex: index.keyMode == qexec.KeyModeString,
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

	if index.snapshot.Current() == nil {
		// After the first publish, current storage is owned and retired by snapshot.Registry.
		index.releaseCurrentStorage()
	}
	index.lenIndexLoaded = result.LenLoaded
	index.keyIndexLoaded = result.KeyIndexLoaded

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

func (index *Index) StoreIndex(file string, bolt *bbolt.DB, bucket []byte, uid [persist.UIDLen]byte) error {
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
		File:           file,
		BucketSeq:      seq,
		UID:            uid,
		Schema:         index.schema,
		StrKey:         index.strKey,
		StrKeyIndex:    index.keyMode == qexec.KeyModeString,
		KeyIndexLoaded: index.keyIndexLoaded,
		Snapshot:       snap,
		PlannerStats:   plannerStats,
	})
}

func (index *Index) PublishCurrentSnapshot(bolt *bbolt.DB, bucket []byte) error {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return err
	}
	prev := index.snapshot.Current()
	var snap *snapshot.View
	if prev != nil {
		lenZeroComplement := pooled.GetBoolSlice(len(index.schema.Indexed))[:len(index.schema.Indexed)]
		clear(lenZeroComplement)
		copy(lenZeroComplement, index.lenZeroComplement)
		snap = snapshot.NewView(seq, prev, index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(index.index, len(index.schema.Indexed)),
			KeyIndex:          index.keyIndex,
			NilIndex:          indexdata.CloneFieldStorageSlots(index.nilIndex, len(index.schema.Indexed)),
			LenIndex:          indexdata.CloneFieldStorageSlots(index.lenIndex, len(index.schema.Indexed)),
			LenZeroComplement: lenZeroComplement,
			Measure:           indexdata.CloneMeasureStorageSlots(index.measure, len(index.schema.Measures)),
			Universe:          index.universe,
		})
	} else {
		snap = snapshot.NewView(seq, nil, index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
			Index:             index.index,
			KeyIndex:          index.keyIndex,
			NilIndex:          index.nilIndex,
			LenIndex:          index.lenIndex,
			LenZeroComplement: index.lenZeroComplement,
			Measure:           index.measure,
			Universe:          index.universe,
		})
	}
	index.installViewNoLock(snap)
	return nil
}

func (index *Index) publishStorageSnapshotNoLock(seq uint64, st snapshot.Storage) {
	snap := snapshot.NewView(seq, index.snapshot.Current(), index.schema, index.SnapshotCacheConfig(), st)
	index.installViewNoLock(snap)
}

func (index *Index) installViewNoLock(s *snapshot.View) {
	index.index = s.Index
	index.keyIndex = s.KeyIndex
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
	snap := snapshot.NewView(seq, index.snapshot.Current(), index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
		Index:             nextIndex,
		KeyIndex:          indexdata.FieldStorage{},
		NilIndex:          nextNilIndex,
		LenIndex:          nextLenIndex,
		LenZeroComplement: nextLenZeroComplement,
		Measure:           nextMeasure,
		Universe:          nextUniverse,
	})
	index.snapshot.Stage(snap)
	return StagedSnapshot{view: snap}
}

func (index *Index) DropStaged(seq uint64) {
	index.snapshot.DropStaged(seq)
}

func (index *Index) PublishCommittedStaged(broken *atomic.Bool, logger *log.Logger, seq uint64, op string, staged StagedSnapshot) error {
	return index.PublishCommittedView(broken, logger, seq, op, staged.view)
}

func (index *Index) PublishCommittedView(broken *atomic.Bool, logger *log.Logger, seq uint64, op string, snap *snapshot.View) (err error) {
	defer index.recoverCommittedPublishPanic(broken, logger, seq, op, &err)
	index.installViewNoLock(snap)
	if index.keyMode == qexec.KeyModeString {
		index.keyIndexLoaded = true
	}
	return nil
}

func (index *Index) recoverCommittedPublishPanic(broken *atomic.Bool, logger *log.Logger, seq uint64, op string, err *error) {
	if v := recover(); v != nil {
		if broken.CompareAndSwap(false, true) {
			logger.Printf("rbi: index entered broken state: post-commit snapshot publish failed (%v): %v", op, v)
			index.stopAnalyzerSignal()
		}
		*err = rbierrors.ErrBroken
		index.snapshot.DropStaged(seq)
	}
}

func (index *Index) PublishLoadedPlannerStats(s *rbistats.PlannerSnapshot) {
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
	index.exec.Analyzer.Lock()
	interval := index.exec.Analyzer.Interval
	if interval <= 0 {
		index.exec.Analyzer.Unlock()
		return
	}
	if index.exec.Analyzer.Stop != nil || index.exec.Analyzer.Done != nil {
		index.exec.Analyzer.Unlock()
		return
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	index.exec.Analyzer.Stop = stop
	index.exec.Analyzer.Done = done
	index.exec.Analyzer.Unlock()

	go index.runPlannerAnalyzeLoop(stop, done, interval, refresh, terminal, resetFailure)
}

func (index *Index) StopAnalyzeLoop() {
	index.exec.Analyzer.Lock()
	stop := index.exec.Analyzer.Stop
	done := index.exec.Analyzer.Done

	if stop != nil {
		close(stop)
		index.exec.Analyzer.Stop = nil
	}
	index.exec.Analyzer.Unlock()

	if done != nil {
		<-done
	}

	index.exec.Analyzer.Lock()
	if index.exec.Analyzer.Done == done {
		index.exec.Analyzer.Done = nil
	}
	index.exec.Analyzer.Unlock()
}

func (index *Index) stopAnalyzerSignal() {
	index.exec.Analyzer.Lock()
	if stop := index.exec.Analyzer.Stop; stop != nil {
		close(stop)
		index.exec.Analyzer.Stop = nil
	}
	index.exec.Analyzer.Unlock()
}

func (index *Index) runPlannerAnalyzeLoop(stop <-chan struct{}, done chan<- struct{}, base time.Duration, refresh func() error, terminal func(error) bool, resetFailure func(error) bool) {
	defer close(done)

	rng := mathutil.NewRand(time.Now().UnixNano())
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

func (index *Index) DBStats(bolt *bbolt.DB, bucketName []byte, unavailable func() error) (DBStats, error) {
	for {
		tx, err := bolt.Begin(false)
		if err != nil {
			return DBStats{}, fmt.Errorf("tx error: %w", err)
		}

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			_ = tx.Rollback()
			return DBStats{}, fmt.Errorf("bucket does not exist")
		}

		seq := bucket.Sequence()
		snap, ref, ok := index.snapshot.PinBySeq(seq)
		if !ok {
			_ = tx.Rollback()
			if unavailable == nil {
				return DBStats{}, fmt.Errorf("snapshot sequence %d is not available", seq)
			}
			if err = unavailable(); err != nil {
				return DBStats{}, err
			}
			continue
		}

		defer index.snapshot.Unpin(seq, ref)
		defer func() { _ = tx.Rollback() }()

		out := DBStats{Sequence: seq}
		out.KeyCount = snap.UniverseCardinality()
		key, _ := bucket.Cursor().Last()
		if key != nil {
			out.HasLast = true
			if !index.strKey && len(key) != 8 {
				return DBStats{}, fmt.Errorf("invalid numeric data key length: %d", len(key))
			}
			out.LastKey = keycodec.DataKeyFromBytes(key, index.strKey)
		}
		return out, nil
	}
}

func (index *Index) IndexStats() rbistats.Index {
	snap, seq, ref := index.snapshot.PinCurrent()
	defer index.snapshot.Unpin(seq, ref)
	return index.indexStats(snap)
}

func (index *Index) indexStats(snap *snapshot.View) rbistats.Index {
	idx := rbistats.Index{
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
	if index.keyMode == qexec.KeyModeString {
		keyStats := snap.KeyIndex.Stats(true)
		idx.StringKeyIndex = &rbistats.StringKeyIndex{
			Size:              keyStats.PostingBytes,
			KeyBytes:          keyStats.KeyBytes,
			Cardinality:       keyStats.PostingCardinality,
			ApproxStructBytes: keyStats.ApproxStructBytes,
			ApproxHeapBytes:   keyStats.PostingBytes + keyStats.KeyBytes + keyStats.ApproxStructBytes,
		}
	}
	return idx
}

func (index *Index) PlannerStats() rbistats.Planner {
	var out rbistats.Planner
	out.Fields = make(map[string]rbistats.PlannerField)

	if ps := index.exec.Stats.Load(); ps != nil {
		out.Version = ps.Version
		out.GeneratedAt = ps.GeneratedAt
		out.UniverseCardinality = ps.UniverseCardinality
		out.FieldCount = len(ps.Fields)
		if out.FieldCount > 0 {
			out.Fields = make(map[string]rbistats.PlannerField, out.FieldCount)
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

func (index *Index) SnapshotStats() rbistats.Snapshot {
	if !index.snapshot.StatsEnabled() {
		return rbistats.Snapshot{}
	}
	snap, seq, ref := index.snapshot.PinCurrent()
	if snap == nil {
		return rbistats.Snapshot{}
	}
	defer index.snapshot.Unpin(seq, ref)

	stats := index.snapshot.Stats(snap, ref)
	return stats
}

func (index *Index) releaseCurrentStorage() {
	indexdata.ReleaseFieldStorageSlots(index.index)
	index.keyIndex.Release()
	index.keyIndex = indexdata.FieldStorage{}
	index.keyIndexLoaded = false
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
