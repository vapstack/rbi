package engine

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
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
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
	"go.etcd.io/bbolt"
)

type Index struct {
	current                   atomic.Pointer[snapshot.View]
	schema                    *schema.Schema
	strKey                    bool
	keyMode                   qexec.KeyMode
	index                     []indexdata.FieldStorage
	keyIndex                  indexdata.FieldStorage
	keyIndexLoaded            bool
	nilIndex                  []indexdata.FieldStorage
	lenIndex                  []indexdata.FieldStorage
	lenZeroComplement         []bool
	measure                   []indexdata.MeasureStorage
	universe                  posting.List
	lenIndexLoaded            bool
	matPredCacheMaxEntries    int
	matPredCacheMaxCard       uint64
	numericSpanMaxEntries     int
	numericSpanMaxEntryBytes  uint64
	numericExactMaxEntries    int
	numericExactMaxEntryBytes uint64
	runtimeCachesDirty        *atomic.Bool
	runtimeCachesEpoch        *atomic.Uint64

	exec *qexec.Runtime
}

type Config struct {
	Schema      *schema.Schema
	StrKey      bool
	StrKeyIndex bool

	MaterializedPredCacheMaxEntries     int
	MaterializedPredCacheMaxCardinality int
	RuntimeCachesDirtyOwner             *atomic.Bool
	RuntimeCachesRetireEpoch            *atomic.Uint64

	NumericRangeBucketSize              int
	NumericRangeBucketMinFieldKeys      int
	NumericRangeBucketMinSpanKeys       int
	NumericRangeSpanCacheMaxEntries     int
	NumericRangeSpanCacheMaxEntryBytes  int64
	NumericRangeExactCacheMaxEntries    int
	NumericRangeExactCacheMaxEntryBytes int64

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

type StagedSnapshot struct {
	view *snapshot.View
}

func (s StagedSnapshot) View() *snapshot.View {
	return s.view
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
		schema:                    cfg.Schema,
		strKey:                    cfg.StrKey,
		keyMode:                   keyMode,
		universe:                  posting.List{},
		matPredCacheMaxEntries:    max(0, cfg.MaterializedPredCacheMaxEntries),
		matPredCacheMaxCard:       qcache.MaterializedPredMaxCardinality(cfg.MaterializedPredCacheMaxCardinality),
		numericSpanMaxEntries:     qcache.NumericRangeSpanCacheMaxEntries(cfg.NumericRangeSpanCacheMaxEntries),
		numericSpanMaxEntryBytes:  qcache.NumericRangeSpanCacheMaxEntryBytes(cfg.NumericRangeSpanCacheMaxEntryBytes),
		numericExactMaxEntries:    qcache.NumericRangeExactCacheMaxEntries(cfg.NumericRangeExactCacheMaxEntries),
		numericExactMaxEntryBytes: qcache.NumericRangeExactCacheMaxEntryBytes(cfg.NumericRangeExactCacheMaxEntryBytes),
		runtimeCachesDirty:        cfg.RuntimeCachesDirtyOwner,
		runtimeCachesEpoch:        cfg.RuntimeCachesRetireEpoch,
		exec: qexec.NewRuntime(qexec.Config{
			StrKey:                              cfg.StrKey,
			KeyMode:                             keyMode,
			Schema:                              cfg.Schema,
			NumericRangeBucketSize:              cfg.NumericRangeBucketSize,
			NumericRangeBucketMinFieldKeys:      cfg.NumericRangeBucketMinFieldKeys,
			NumericRangeBucketMinSpanKeys:       cfg.NumericRangeBucketMinSpanKeys,
			NumericRangeSpanCacheMaxEntries:     cfg.NumericRangeSpanCacheMaxEntries,
			NumericRangeSpanCacheMaxEntryBytes:  cfg.NumericRangeSpanCacheMaxEntryBytes,
			NumericRangeExactCacheMaxEntries:    cfg.NumericRangeExactCacheMaxEntries,
			NumericRangeExactCacheMaxEntryBytes: cfg.NumericRangeExactCacheMaxEntryBytes,
			AnalyzeInterval:                     cfg.AnalyzeInterval,
			TraceSink:                           cfg.TraceSink,
			TraceSampleEvery:                    cfg.TraceSampleEvery,
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
		MatPredMaxEntries:         index.matPredCacheMaxEntries,
		MatPredMaxCard:            index.matPredCacheMaxCard,
		NumericSpanMaxEntries:     index.numericSpanMaxEntries,
		NumericSpanMaxEntryBytes:  index.numericSpanMaxEntryBytes,
		NumericExactMaxEntries:    index.numericExactMaxEntries,
		NumericExactMaxEntryBytes: index.numericExactMaxEntryBytes,
		RuntimeCachesDirtyOwner:   index.runtimeCachesDirty,
		RuntimeCachesRetireEpoch:  index.runtimeCachesEpoch,
	}
}

func (index *Index) RuntimeCacheStats(snap *snapshot.View) rbistats.RuntimeCaches {
	if snap == nil {
		return rbistats.RuntimeCaches{}
	}
	num := snap.NumericRangeSpanCacheStats()
	exact := snap.NumericRangeExactResultCacheStats()
	return rbistats.RuntimeCaches{
		NumericRangeSpan: rbistats.NumericRangeSpanCache{
			CurrentBytes:           num.CurrentBytes,
			RetiredBytes:           num.RetiredBytes,
			EntryCount:             num.EntryCount,
			MaxEntryBytes:          num.MaxEntryBytes,
			MaxEntries:             num.MaxEntries,
			Stores:                 num.Stores,
			Evictions:              num.Evictions,
			RejectedTooLarge:       num.RejectedTooLarge,
			RejectedCapacity:       num.RejectedCapacity,
			RejectedRetiredBacklog: num.RejectedRetiredBacklog,
		},
		NumericRangeExactResult: rbistats.NumericRangeExactResultCache{
			CurrentBytes:             exact.CurrentBytes,
			RetiredBytes:             exact.RetiredBytes,
			EntryCount:               exact.EntryCount,
			MaxEntryBytes:            exact.MaxEntryBytes,
			MaxEntries:               exact.MaxEntries,
			Stores:                   exact.Stores,
			Evictions:                exact.Evictions,
			RejectedTooLarge:         exact.RejectedTooLarge,
			RejectedCapacity:         exact.RejectedCapacity,
			RejectedRetiredBacklog:   exact.RejectedRetiredBacklog,
			RejectedFirstObservation: exact.RejectedFirstObservation,
		},
	}
}

func (index *Index) CurrentSnapshot() *snapshot.View {
	return index.current.Load()
}

// ReleaseUnpublishedSnapshot is used when collection startup aborts before the root
// registry owns the initial view.
func (index *Index) ReleaseUnpublishedSnapshot() {
	if snap := index.current.Swap(nil); snap != nil {
		snap.Release()
	} else {
		index.releaseCurrentStorage()
	}
	index.index = nil
	index.keyIndex = indexdata.FieldStorage{}
	index.keyIndexLoaded = false
	index.nilIndex = nil
	index.lenIndex = nil
	index.lenZeroComplement = nil
	index.lenIndexLoaded = false
	index.measure = nil
	index.universe = posting.List{}
}

func (index *Index) ConfigureAsyncMaterializedPredSnapshotOps(ops qexec.AsyncMaterializedPredSnapshotOps) {
	index.exec.ConfigureAsyncMaterializedPredSnapshotOps(ops)
}

func (index *Index) ConfigureWrite(cfg *wexec.Config) {
	cfg.Indexed = true
	cfg.Unique = wexec.UniqueContext{
		Schema:  index.schema,
		Current: index.CurrentSnapshot,
	}
	cfg.SnapshotOps = wexec.SnapshotOps{
		Current:     index.CurrentSnapshot,
		Schema:      index.schema,
		CacheConfig: index.SnapshotCacheConfig(),
		PatchFields: index.schema.Patch.Fields,
		StrKeyIndex: index.keyMode == qexec.KeyModeString,
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

func (index *Index) ScanKeysOnSnapshot(snap *snapshot.View, seek uint64, fn func(uint64) (bool, error)) error {
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

func (index *Index) ScanStringKeysOnSnapshot(snap *snapshot.View, seek string, fn func(string) (bool, error)) error {
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

func (index *Index) CountOnSnapshot(snap *snapshot.View, exprs ...qx.Expr) (uint64, error) {
	prepared, err := qagg.PrepareCount(index.exec, exprs...)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()

	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)

	return qagg.Count(view, prepared, true)
}

func (index *Index) AggregateOnSnapshot(snap *snapshot.View, q *qx.QX) (qagg.Result, error) {
	prepared, err := qagg.Prepare(q, index.schema, index.exec)
	if err != nil {
		return qagg.Result{}, err
	}
	defer prepared.Release()

	view := index.exec.AcquireView(snap)
	defer index.exec.ReleaseView(view)

	return qagg.Execute(view, snap, prepared)
}

func (index *Index) QueryIDsOnSnapshot(snap *snapshot.View, q *qx.QX, tryEmpty bool) ([]uint64, error) {
	prepared, shape, err := index.prepareQuery(q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()

	return index.QueryShapeOnSnapshot(snap, &shape, tryEmpty)
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

func (index *Index) QueryShapeOnSnapshot(snap *snapshot.View, q *qir.Shape, tryEmpty bool) ([]uint64, error) {
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
		Current:           index.CurrentSnapshot(),
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
		prev := index.CurrentSnapshot()
		index.installStorageSnapshot(seq, st)
		if prev != nil {
			prev.Release()
		}
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

	if index.CurrentSnapshot() == nil {
		index.releaseCurrentStorage()
	}
	index.lenIndexLoaded = result.LenLoaded
	index.keyIndexLoaded = result.KeyIndexLoaded

	st := result.Storage
	result.Storage = snapshot.Storage{}
	index.installStorageSnapshot(currentSeq, st)
	installed = true

	return LoadResult{
		SkipFields:        result.SkipFields,
		SkipMeasureFields: result.SkipMeasureFields,
		PlannerStats:      result.PlannerStats,
	}, nil
}

func (index *Index) StoreIndexSnapshot(file string, seq uint64, uid [persist.UIDLen]byte, snap *snapshot.View) error {
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

func (index *Index) InstallCurrentSnapshot(bolt *bbolt.DB, bucket []byte) error {
	if index.CurrentSnapshot() != nil {
		return nil
	}
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return err
	}
	snap := snapshot.NewView(seq, nil, index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
		Index:             index.index,
		KeyIndex:          index.keyIndex,
		NilIndex:          index.nilIndex,
		LenIndex:          index.lenIndex,
		LenZeroComplement: index.lenZeroComplement,
		Measure:           index.measure,
		Universe:          index.universe,
	})
	index.installSnapshot(snap)
	return nil
}

func (index *Index) installStorageSnapshot(seq uint64, st snapshot.Storage) {
	snap := snapshot.NewView(seq, index.CurrentSnapshot(), index.schema, index.SnapshotCacheConfig(), st)
	index.installSnapshot(snap)
}

func (index *Index) installSnapshot(s *snapshot.View) {
	index.index = s.Index
	index.keyIndex = s.KeyIndex
	index.nilIndex = s.NilIndex
	index.lenIndex = s.LenIndex
	index.lenZeroComplement = s.LenZeroComplement
	index.measure = s.Measure
	index.universe = s.Universe
	index.current.Store(s)
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
	snap := snapshot.NewView(seq, index.CurrentSnapshot(), index.schema, index.SnapshotCacheConfig(), snapshot.Storage{
		Index:             nextIndex,
		KeyIndex:          indexdata.FieldStorage{},
		NilIndex:          nextNilIndex,
		LenIndex:          nextLenIndex,
		LenZeroComplement: nextLenZeroComplement,
		Measure:           nextMeasure,
		Universe:          nextUniverse,
	})
	return StagedSnapshot{view: snap}
}

func (index *Index) InstallCommittedSnapshot(snap *snapshot.View) error {
	index.installSnapshot(snap)
	if index.keyMode == qexec.KeyModeString {
		index.keyIndexLoaded = true
	}
	return nil
}

func (index *Index) PublishLoadedPlannerStats(s *rbistats.PlannerSnapshot) {
	index.exec.PublishLoadedPlannerStats(s, index.CurrentSnapshot())
}

func (index *Index) RefreshPlannerStatsOnSnapshot(snap *snapshot.View, unavailable func() error) error {
	index.exec.Analyzer.Lock()
	defer index.exec.Analyzer.Unlock()

	if err := unavailable(); err != nil {
		return err
	}
	index.exec.RefreshPlannerStatsOnSnapshot(snap)
	return nil
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

func (index *Index) IndexStatsOnSnapshot(snap *snapshot.View) rbistats.Index {
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
