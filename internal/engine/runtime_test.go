package engine

import (
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"go.etcd.io/bbolt"
)

type runtimeTestRec struct {
	Name   string   `db:"name" rbi:"index"`
	Tags   []string `db:"tags" rbi:"index"`
	Score  uint64   `db:"score" rbi:"measure"`
	Active bool     `db:"active" rbi:"index"`
}

type runtimeTestNoIndexRec struct {
	Name string
}

var runtimeTestCodec = func() *schema.Schema {
	rt, err := schema.Compile(reflect.TypeFor[runtimeTestRec](), schema.Config{})
	if err != nil {
		panic(err)
	}
	return rt
}()

func openRuntimeTestBolt(t *testing.T, bucket []byte) *bbolt.DB {
	t.Helper()

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "runtime.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err = db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucket)
		return e
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return db
}

func putRuntimeTestRec(t *testing.T, db *bbolt.DB, bucket []byte, id uint64, rec runtimeTestRec) {
	t.Helper()

	data := encodeRuntimeTestRec(&rec)
	if err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(id, &key), data)
	}); err != nil {
		t.Fatalf("put record: %v", err)
	}
}

func putRuntimeTestStringRec(t *testing.T, db *bbolt.DB, bucket []byte, mapBucket []byte, key string, rec runtimeTestRec) uint64 {
	t.Helper()

	data := encodeRuntimeTestRec(&rec)
	var idx uint64
	if err := db.Update(func(tx *bbolt.Tx) error {
		m := tx.Bucket(mapBucket)
		var err error
		idx, err = m.NextSequence()
		if err != nil {
			return err
		}
		var mapKey [8]byte
		if err = m.Put(keycodec.U64BytesWithBuf(idx, &mapKey), keycodec.StringBytes(key)); err != nil {
			return err
		}
		value := keycodec.AppendU64Bytes(nil, idx)
		value = append(value, data...)
		return tx.Bucket(bucket).Put(keycodec.StringBytes(key), value)
	}); err != nil {
		t.Fatalf("put string record: %v", err)
	}
	return idx
}

func setRuntimeTestSequence(t *testing.T, db *bbolt.DB, bucket []byte, seq uint64) {
	t.Helper()

	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).SetSequence(seq)
	}); err != nil {
		t.Fatalf("set sequence: %v", err)
	}
}

func createRuntimeStringMap(t *testing.T, db *bbolt.DB, bucket []byte) []byte {
	t.Helper()

	mapBucket := append(append([]byte(nil), bucket...), ".rbimap"...)
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(mapBucket)
		return err
	}); err != nil {
		t.Fatalf("create string map: %v", err)
	}
	return mapBucket
}

func runtimeTestBucketState(t *testing.T, db *bbolt.DB, bucket []byte, strKey bool) (uint64, bool, keycodec.DataKey) {
	t.Helper()

	var seq uint64
	var hasLast bool
	var last keycodec.DataKey
	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return errors.New("bucket does not exist")
		}
		seq = b.Sequence()
		key, _ := b.Cursor().Last()
		if key != nil {
			hasLast = true
			last = keycodec.DataKeyFromBytes(key, strKey)
		}
		return nil
	}); err != nil {
		t.Fatalf("bucket state: %v", err)
	}
	return seq, hasLast, last
}

func newRuntimeTestRuntime(t *testing.T, strKey bool) *Index {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(runtimeTestRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	r, err := NewIndex(Config{Schema: rt, StrKey: strKey})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	if r == nil {
		t.Fatal("engine.New returned nil runtime")
	}
	return r
}

func decodeRuntimeTestRec(data []byte) (unsafe.Pointer, error) {
	rec := new(runtimeTestRec)
	if err := runtimeTestCodec.Codec.Decode(data, unsafe.Pointer(rec)); err != nil {
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func encodeRuntimeTestRec(rec *runtimeTestRec) []byte {
	payload, err := runtimeTestCodec.Codec.Encode(unsafe.Pointer(rec), nil)
	if err != nil {
		panic(err)
	}
	return slices.Clone(payload)
}

func releaseRuntimeTestRec(ptr unsafe.Pointer) {
	*(*runtimeTestRec)(ptr) = runtimeTestRec{}
}

func TestRuntimeNumericKeyOnlySchemaDoesNotCreateIndex(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(runtimeTestNoIndexRec{}), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	r, err := NewIndex(Config{Schema: rt, StrKey: false, StrKeyIndex: true})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if r != nil {
		t.Fatal("numeric key-only schema must not create indexed runtime")
	}
}

func TestRuntimeStringKeyIndexOnlyCreatesRuntimeAndResolvesKey(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(runtimeTestNoIndexRec{}), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	r, err := NewIndex(Config{Schema: rt, StrKey: true, StrKeyIndex: true})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if r == nil {
		t.Fatal("NewIndex returned nil for string key-index-only runtime")
	}
	if len(r.schema.Indexed) != 0 || len(r.schema.Fields) != 0 {
		t.Fatalf("string key runtime must not add schema fields: indexed=%d fields=%d", len(r.schema.Indexed), len(r.schema.Fields))
	}

	prepared, shape, err := r.prepareQuery(qx.Query(qx.EQ(schema.ReservedKeyFieldName, "k")).Sort(schema.ReservedKeyFieldName, qx.ASC))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()
	if shape.Expr.FieldOrdinal < 0 {
		t.Fatalf("$key expr ordinal=%d", shape.Expr.FieldOrdinal)
	}
	if !shape.HasOrder || shape.Order.FieldOrdinal != shape.Expr.FieldOrdinal {
		t.Fatalf("$key order shape=%+v", shape)
	}
	if r.exec.FieldNameByOrdinal(shape.Expr.FieldOrdinal) != schema.ReservedKeyFieldName {
		t.Fatalf("FieldNameByOrdinal($key)=%q", r.exec.FieldNameByOrdinal(shape.Expr.FieldOrdinal))
	}
}

func runtimeTestStringKeyStorage(keys []string, ids []uint64) indexdata.FieldStorage {
	var builder indexdata.SortedUniqueStringFieldStorageBuilder
	builder.Init(len(keys))
	for i := range keys {
		builder.AppendBytes(keycodec.StringBytes(keys[i]), ids[i])
	}
	return builder.Finish()
}

func newRuntimeStringKeyIndexRuntime(t *testing.T) *Index {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(runtimeTestNoIndexRec{}), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	r, err := NewIndex(Config{Schema: rt, StrKey: true, StrKeyIndex: true})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	return r
}

func runtimeTestQueryKeys(t *testing.T, r *Index, q *qx.QX) []uint64 {
	t.Helper()

	snap := r.CurrentSnapshot()
	var got []uint64
	if err := r.QueryKeysOnSnapshot(snap, q, func(ids []uint64) error {
		got = append(got, ids...)
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	return got
}

func (index *Index) QueryKeysOnSnapshot(snap *snapshot.View, q *qx.QX, fn func([]uint64) error) error {
	ids, err := index.QueryIDsOnSnapshot(snap, q, false)
	if err != nil {
		return err
	}
	return fn(ids)
}

func TestRuntimeScanStringKeysUsesKeyIndexSnapshot(t *testing.T) {
	r := newRuntimeStringKeyIndexRuntime(t)

	r.installStorageSnapshot(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"a", "b", "c"}, []uint64{11, 12, 13}),
		Universe: posting.BuildFromSorted([]uint64{
			11, 12, 13,
		}),
	})

	snap := r.CurrentSnapshot()
	var got []string
	var saved string
	if err := r.ScanStringKeysOnSnapshot(snap, "b", func(key string) (bool, error) {
		got = append(got, key)
		if key == "b" {
			saved = key
		}
		return true, nil
	}); err != nil {
		t.Fatalf("ScanStringKeys: %v", err)
	}
	if !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("ScanStringKeys keys=%v want [b c]", got)
	}

	got = got[:0]
	if err := r.ScanStringKeysOnSnapshot(snap, "a", func(key string) (bool, error) {
		got = append(got, key)
		return false, nil
	}); err != nil {
		t.Fatalf("ScanStringKeys stop: %v", err)
	}
	if !slices.Equal(got, []string{"a"}) {
		t.Fatalf("ScanStringKeys stop keys=%v want [a]", got)
	}

	scanErr := errors.New("scan error")
	err := r.ScanStringKeysOnSnapshot(snap, "a", func(string) (bool, error) {
		return true, scanErr
	})
	if !errors.Is(err, scanErr) {
		t.Fatalf("ScanStringKeys error=%v want %v", err, scanErr)
	}

	r.installStorageSnapshot(4, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"z"}, []uint64{99}),
		Universe: posting.BuildFromSorted([]uint64{
			99,
		}),
	})
	if saved != "b" {
		t.Fatalf("callback key after snapshot replacement=%q want b", saved)
	}
}

func TestRuntimeStringKeyPredicatesUseKeyIndex(t *testing.T) {
	r := newRuntimeStringKeyIndexRuntime(t)
	r.installStorageSnapshot(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"alpha", "beta", "bravo", "carrot"}, []uint64{40, 10, 30, 20}),
		Universe: posting.BuildFromSorted([]uint64{
			10, 20, 30, 40,
		}),
	})

	stats := r.IndexStatsOnSnapshot(r.CurrentSnapshot())
	if stats.StringKeyIndex == nil {
		t.Fatal("StringKeyIndex stats nil")
	}
	if stats.StringKeyIndex.KeyBytes != 20 || stats.StringKeyIndex.Cardinality != 4 {
		t.Fatalf("StringKeyIndex stats keyBytes=%d card=%d want 20/4", stats.StringKeyIndex.KeyBytes, stats.StringKeyIndex.Cardinality)
	}
	if _, ok := stats.FieldTotalCardinality[schema.ReservedKeyFieldName]; ok {
		t.Fatalf("IndexStats field maps must not contain %q", schema.ReservedKeyFieldName)
	}

	cases := []struct {
		name string
		q    *qx.QX
		want []uint64
	}{
		{name: "eq", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, "beta")), want: []uint64{10}},
		{name: "in", q: qx.Query(qx.IN(schema.ReservedKeyFieldName, []string{"carrot", "alpha", "missing"})), want: []uint64{20, 40}},
		{name: "gte", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, "bravo")), want: []uint64{20, 30}},
		{name: "prefix", q: qx.Query(qx.PREFIX(schema.ReservedKeyFieldName, "br")), want: []uint64{30}},
		{name: "suffix", q: qx.Query(qx.SUFFIX(schema.ReservedKeyFieldName, "ta")), want: []uint64{10}},
		{name: "contains", q: qx.Query(qx.CONTAINS(schema.ReservedKeyFieldName, "arr")), want: []uint64{20}},
	}
	for _, tc := range cases {
		got := runtimeTestQueryKeys(t, r, tc.q)
		slices.Sort(got)
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s QueryKeys=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestRuntimeStringKeyOrderUsesKeyIndex(t *testing.T) {
	r := newRuntimeStringKeyIndexRuntime(t)
	r.installStorageSnapshot(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"alpha", "beta", "bravo", "carrot"}, []uint64{40, 10, 30, 20}),
		Universe: posting.BuildFromSorted([]uint64{
			10, 20, 30, 40,
		}),
	})

	got := runtimeTestQueryKeys(t, r, qx.Query().Sort(schema.ReservedKeyFieldName, qx.ASC))
	if !slices.Equal(got, []uint64{40, 10, 30, 20}) {
		t.Fatalf("ASC QueryKeys=%v want [40 10 30 20]", got)
	}
	got = runtimeTestQueryKeys(t, r, qx.Query().Sort(schema.ReservedKeyFieldName, qx.DESC))
	if !slices.Equal(got, []uint64{20, 30, 10, 40}) {
		t.Fatalf("DESC QueryKeys=%v want [20 30 10 40]", got)
	}
	got = runtimeTestQueryKeys(t, r, qx.Query(qx.GTE(schema.ReservedKeyFieldName, "beta")).Sort(schema.ReservedKeyFieldName, qx.ASC).Offset(1).Limit(2))
	if !slices.Equal(got, []uint64{30, 20}) {
		t.Fatalf("bounded ordered QueryKeys=%v want [30 20]", got)
	}
}

func TestRuntimeStringKeyRejectsNonStringValues(t *testing.T) {
	r := newRuntimeStringKeyIndexRuntime(t)
	r.installStorageSnapshot(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"alpha"}, []uint64{1}),
		Universe: posting.BuildFromSorted([]uint64{1}),
	})

	cases := []*qx.QX{
		qx.Query(qx.EQ(schema.ReservedKeyFieldName, 1)),
		qx.Query(qx.EQ(schema.ReservedKeyFieldName, nil)),
		qx.Query(qx.IN(schema.ReservedKeyFieldName, []int{1})),
		qx.Query(qx.IN(schema.ReservedKeyFieldName, []any{nil})),
		qx.Query(qx.PREFIX(schema.ReservedKeyFieldName, 1)),
		qx.Query(qx.CONTAINS(schema.ReservedKeyFieldName, true)),
	}
	for i, q := range cases {
		err := r.QueryKeysOnSnapshot(r.CurrentSnapshot(), q, func([]uint64) error { return nil })
		if err == nil {
			t.Fatalf("case %d QueryKeys succeeded, want error", i)
		}
	}
}

func buildRuntimeTestIndex(t *testing.T, r *Index, db *bbolt.DB, bucket []byte) {
	t.Helper()

	result, err := r.BuildIndex(db, bucket, nil, nil, nil, decodeRuntimeTestRec, releaseRuntimeTestRec)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	if !result.Stats {
		t.Fatal("BuildIndex did not report stats")
	}
}

func TestRuntimeInstallCurrentSnapshotDoesNotRepublishExistingCurrent(t *testing.T) {
	bucket := []byte("runtime_install_existing_current")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)
	current := r.CurrentSnapshot()
	if current == nil {
		t.Fatal("BuildIndex did not install current snapshot")
	}

	if err := r.InstallCurrentSnapshot(db, bucket); err != nil {
		t.Fatalf("InstallCurrentSnapshot: %v", err)
	}
	if got := r.CurrentSnapshot(); got != current {
		t.Fatalf("InstallCurrentSnapshot replaced existing current snapshot: got=%p want=%p", got, current)
	}
}

func TestRuntimeInitialSnapshotCacheHasRuntimeContext(t *testing.T) {
	bucket := []byte("runtime_initial_snapshot_cache_context")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	rt, err := schema.Compile(reflect.TypeOf(runtimeTestRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	var dirty atomic.Bool
	var epoch atomic.Uint64
	epoch.Store(7)
	r, err := NewIndex(Config{
		Schema:                          rt,
		MaterializedPredCacheMaxEntries: 1,
		RuntimeCachesDirtyOwner:         &dirty,
		RuntimeCachesRetireEpoch:        &epoch,
	})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	buildRuntimeTestIndex(t, r, db, bucket)
	snap := r.CurrentSnapshot()
	if snap == nil {
		t.Fatal("initial build did not install current snapshot")
	}
	defer r.ReleaseUnpublishedSnapshot()

	snap.StoreMaterializedPredKey(qcache.MaterializedPredKeyFromOpaque("a"), (posting.List{}).BuildAdded(1))
	snap.StoreMaterializedPredKey(qcache.MaterializedPredKeyFromOpaque("b"), (posting.List{}).BuildAdded(2))
	if !dirty.Load() {
		t.Fatal("initial snapshot runtime cache eviction did not mark dirty owner")
	}
	retained := snap.TakeRetiredRuntimeCachesBefore(7)
	if !retained.Empty() {
		retained.Release()
		t.Fatal("retired runtime cache payload was detached before its retire epoch became safe")
	}
	retained.Release()
	if !snap.RuntimeCachesDirty() {
		t.Fatal("runtime cache dirty flag was cleared while retired payload was still epoch-protected")
	}
	retired := snap.TakeRetiredRuntimeCachesBefore(8)
	if retired.Empty() {
		t.Fatal("retired runtime cache payload was not detached after safe epoch advanced")
	}
	retired.Release()
}

func TestRuntimeReleaseUnpublishedSnapshotReleasesCurrentView(t *testing.T) {
	bucket := []byte("runtime_release_unpublished_current")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)
	snap := r.CurrentSnapshot()
	if snap == nil || snap.Index == nil || snap.NilIndex == nil || snap.LenIndex == nil || snap.Measure == nil {
		t.Fatalf("current snapshot not installed: %v", snap)
	}

	r.ReleaseUnpublishedSnapshot()
	if got := r.CurrentSnapshot(); got != nil {
		t.Fatalf("current snapshot after release=%v want nil", got)
	}
	if snap.Index != nil || snap.NilIndex != nil || snap.LenIndex != nil || snap.Measure != nil {
		t.Fatalf("released snapshot kept storage: index=%v nil=%v len=%v measure=%v", snap.Index, snap.NilIndex, snap.LenIndex, snap.Measure)
	}
}

func TestRuntimeReleaseUnpublishedSnapshotReleasesInitialStorage(t *testing.T) {
	r := newRuntimeTestRuntime(t, false)
	if r.index == nil || r.nilIndex == nil || r.lenIndex == nil || r.measure == nil {
		t.Fatalf("initial storage missing: index=%v nil=%v len=%v measure=%v", r.index, r.nilIndex, r.lenIndex, r.measure)
	}

	r.ReleaseUnpublishedSnapshot()
	if r.index != nil || r.nilIndex != nil || r.lenIndex != nil || r.lenZeroComplement != nil || r.measure != nil {
		t.Fatalf("initial storage after release: index=%v nil=%v len=%v lenZero=%v measure=%v", r.index, r.nilIndex, r.lenIndex, r.lenZeroComplement, r.measure)
	}
}

func TestRuntimeBuildQueryCountAggregatePlannerAndStats(t *testing.T) {
	bucket := []byte("runtime_dispatch")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Tags: []string{"go"}, Score: 10, Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob", Tags: []string{"db"}, Score: 20})
	putRuntimeTestRec(t, db, bucket, 3, runtimeTestRec{Name: "alice", Tags: []string{"go", "db"}, Score: 7, Active: true})
	setRuntimeTestSequence(t, db, bucket, 3)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)
	snap := r.CurrentSnapshot()

	var keys []uint64
	if err := r.QueryKeysOnSnapshot(snap, qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 3}) {
		t.Fatalf("QueryKeys IDs=%v want [1 3]", keys)
	}

	count, err := r.CountOnSnapshot(snap, qx.EQ("active", true))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Count=%d want 2", count)
	}

	agg, err := r.AggregateOnSnapshot(snap, qx.Query(qx.EQ("active", true)).Metrics(qx.ROWCOUNT().AS("rows")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(agg.Rows) != 1 || len(agg.Rows[0]) != 1 {
		t.Fatalf("Aggregate rows=%#v", agg.Rows)
	}
	rows, ok := agg.Rows[0][0].Uint()
	if !ok || rows != 2 {
		t.Fatalf("Aggregate rowcount=%d ok=%v want 2/true", rows, ok)
	}

	ids, err := r.QueryIDsOnSnapshot(snap, qx.Query(qx.EQ("name", "alice")), true)
	if err != nil {
		t.Fatalf("QueryIDsOnSnapshot: %v", err)
	}
	if !slices.Equal(ids, []uint64{1, 3}) {
		t.Fatalf("QueryIDsOnSnapshot IDs=%v want [1 3]", ids)
	}
	if err = db.View(func(tx *bbolt.Tx) error {
		var key [8]byte
		raw := tx.Bucket(bucket).Get(keycodec.U64BytesWithBuf(ids[0], &key))
		if raw == nil {
			t.Fatal("Query callback did not receive sequence-aligned bolt tx")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	seq, hasLast, last := runtimeTestBucketState(t, db, bucket, false)
	if seq != 3 || snap.UniverseCardinality() != 3 || !hasLast || keycodec.UserKeyFromDataKey[uint64](last, false) != 3 {
		t.Fatalf("bucket/snapshot state seq=%d card=%d hasLast=%v last=%v", seq, snap.UniverseCardinality(), hasLast, last)
	}
	indexStats := r.IndexStatsOnSnapshot(snap)
	if indexStats.FieldTotalCardinality["name"] != 3 || indexStats.FieldTotalCardinality["active"] != 3 {
		t.Fatalf("IndexStats.FieldTotalCardinality=%v", indexStats.FieldTotalCardinality)
	}
	if indexStats.StringKeyIndex != nil {
		t.Fatalf("IndexStats.StringKeyIndex=%+v, want nil", indexStats.StringKeyIndex)
	}

	if err = r.RefreshPlannerStatsOnSnapshot(snap, func() error { return nil }); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}
	plannerStats := r.PlannerStats()
	if plannerStats.UniverseCardinality != 3 || plannerStats.FieldCount == 0 {
		t.Fatalf("PlannerStats=%+v", plannerStats)
	}
}

func TestRuntimeStringQueryKeysReturnIDs(t *testing.T) {
	bucket := []byte("runtime_string")
	db := openRuntimeTestBolt(t, bucket)
	mapBucket := createRuntimeStringMap(t, db, bucket)
	putRuntimeTestStringRec(t, db, bucket, mapBucket, "b", runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestStringRec(t, db, bucket, mapBucket, "a", runtimeTestRec{Name: "bob"})
	putRuntimeTestStringRec(t, db, bucket, mapBucket, "c", runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 3)

	r := newRuntimeTestRuntime(t, true)
	result, err := r.BuildIndex(db, bucket, mapBucket, nil, nil, decodeRuntimeTestRec, releaseRuntimeTestRec)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	if !result.Stats {
		t.Fatal("BuildIndex did not report stats")
	}

	var got []uint64
	snap := r.CurrentSnapshot()
	if err := r.QueryKeysOnSnapshot(snap, qx.Query(qx.EQ("name", "alice")), func(ids []uint64) error {
		got = ids
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(got, []uint64{1, 3}) {
		t.Fatalf("QueryKeys IDs=%v want [1 3]", got)
	}

	seq, hasLast, last := runtimeTestBucketState(t, db, bucket, true)
	if seq != 3 || snap.UniverseCardinality() != 3 || !hasLast || keycodec.UserKeyFromDataKey[string](last, true) != "c" {
		t.Fatalf("string bucket/snapshot state seq=%d card=%d hasLast=%v last=%v", seq, snap.UniverseCardinality(), hasLast, last)
	}
}

func TestRuntimeStageTruncatePublishesEmptySnapshot(t *testing.T) {
	bucket := []byte("runtime_truncate")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)

	staged := r.StageTruncate(4)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	if got := r.CurrentSnapshot(); got == nil || got.Seq != 2 {
		t.Fatalf("current snapshot before publish=%v", got)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(bucket); err != nil {
			return err
		}
		b, err := tx.CreateBucket(bucket)
		if err != nil {
			return err
		}
		return b.SetSequence(4)
	}); err != nil {
		t.Fatalf("truncate bucket: %v", err)
	}

	err := r.InstallCommittedSnapshot(staged.view)
	if err != nil {
		t.Fatalf("InstallCommittedStaged: %v", err)
	}
	if got := r.CurrentSnapshot(); got == nil || got.Seq != 4 || got.UniverseCardinality() != 0 {
		t.Fatalf("current snapshot after truncate=%v", got)
	}

	var keys []uint64
	current := r.CurrentSnapshot()
	if err := r.QueryKeysOnSnapshot(current, qx.Query(), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after truncate: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("QueryKeys after truncate=%v want empty", keys)
	}
	seq, hasLast, _ := runtimeTestBucketState(t, db, bucket, false)
	if seq != 4 || current.UniverseCardinality() != 0 || hasLast {
		t.Fatalf("bucket/snapshot state after truncate seq=%d card=%d hasLast=%v", seq, current.UniverseCardinality(), hasLast)
	}
}

func TestRuntimePinnedCurrentSurvivesPublishedTruncate(t *testing.T) {
	bucket := []byte("runtime_pinned_current_truncate")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)

	prepared, shape, err := r.prepareQuery(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	old := r.CurrentSnapshot()
	if old == nil || old.Seq != 2 {
		t.Fatalf("old snapshot=%v", old)
	}

	staged := r.StageTruncate(4)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	if err = r.InstallCommittedSnapshot(staged.view); err != nil {
		t.Fatalf("InstallCommittedStaged: %v", err)
	}
	if got := r.CurrentSnapshot(); got == nil || got.Seq != 4 || got.UniverseCardinality() != 0 {
		t.Fatalf("current snapshot after truncate=%v", got)
	}

	keys, err := r.QueryShapeOnSnapshot(old, &shape, false)
	if err != nil {
		t.Fatalf("query old snapshot: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("old snapshot query IDs=%v want [1]", keys)
	}

	var currentKeys []uint64
	if err = r.QueryKeysOnSnapshot(r.CurrentSnapshot(), qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		currentKeys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys current after truncate: %v", err)
	}
	if len(currentKeys) != 0 {
		t.Fatalf("current snapshot query IDs=%v want empty", currentKeys)
	}
}

func TestRuntimePinnedCurrentSurvivesLoadIndex(t *testing.T) {
	bucket := []byte("runtime_pinned_current_load")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)

	file := filepath.Join(t.TempDir(), "runtime.rbi")
	old := r.CurrentSnapshot()
	if old == nil || old.Seq != 2 {
		t.Fatalf("old snapshot=%v", old)
	}
	if err := r.StoreIndexSnapshot(file, old.Seq, [persist.UIDLen]byte{}, old); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}

	prepared, shape, err := r.prepareQuery(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	if _, err = r.LoadIndex(file, db.Path(), bucket, old.Seq, [persist.UIDLen]byte{}); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	if got := r.CurrentSnapshot(); got == nil || got == old || got.Seq != 2 {
		t.Fatalf("current snapshot after LoadIndex=%v old=%v", got, old)
	}

	keys, err := r.QueryShapeOnSnapshot(old, &shape, false)
	if err != nil {
		t.Fatalf("query old snapshot: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("old snapshot query IDs=%v want [1]", keys)
	}
}

func TestRuntimeBuildReleasesLoadedSnapshotWhenRebuilding(t *testing.T) {
	bucket := []byte("runtime_loaded_snapshot_rebuild_release")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Tags: []string{"go"}, Score: 10, Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob", Tags: []string{"db"}, Score: 20})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)
	snap := r.CurrentSnapshot()

	file := filepath.Join(t.TempDir(), "runtime.rbi")
	if err := r.StoreIndexSnapshot(file, snap.Seq, [persist.UIDLen]byte{}, snap); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}

	loaded := newRuntimeTestRuntime(t, false)
	if _, err := loaded.LoadIndex(file, db.Path(), bucket, snap.Seq, [persist.UIDLen]byte{}); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	loadedSnap := loaded.CurrentSnapshot()
	if loadedSnap == nil || loadedSnap.Index == nil {
		t.Fatalf("loaded snapshot=%v", loadedSnap)
	}

	if _, err := loaded.BuildIndex(
		db,
		bucket,
		nil,
		map[string]struct{}{"name": {}},
		nil,
		decodeRuntimeTestRec,
		releaseRuntimeTestRec,
	); err != nil {
		t.Fatalf("BuildIndex partial: %v", err)
	}
	if got := loaded.CurrentSnapshot(); got == nil || got == loadedSnap || got.Seq != loadedSnap.Seq {
		t.Fatalf("current after rebuild=%v loaded=%v", got, loadedSnap)
	}
	if loadedSnap.Index != nil || loadedSnap.NilIndex != nil || loadedSnap.LenIndex != nil || loadedSnap.Measure != nil {
		t.Fatalf("loaded snapshot storage was not released: index=%v nil=%v len=%v measure=%v", loadedSnap.Index, loadedSnap.NilIndex, loadedSnap.LenIndex, loadedSnap.Measure)
	}

	var keys []uint64
	if err := loaded.QueryKeysOnSnapshot(loaded.CurrentSnapshot(), qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys rebuilt current: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("rebuilt current IDs=%v want [1]", keys)
	}
}

func TestRuntimeStageTruncateKeepsCurrentSnapshotQueryable(t *testing.T) {
	bucket := []byte("runtime_drop_staged")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)

	staged := r.StageTruncate(2)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}

	if got := r.CurrentSnapshot(); got == nil || got == staged.view || got.Seq != 1 || got.UniverseCardinality() != 1 {
		t.Fatalf("current snapshot after StageTruncate=%v", got)
	}

	var keys []uint64
	if err := r.QueryKeysOnSnapshot(r.CurrentSnapshot(), qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after StageTruncate: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("QueryKeys after StageTruncate IDs=%v want [1]", keys)
	}
}

func TestRuntimeFailedBuildIndexKeepsCurrentSnapshotQueryable(t *testing.T) {
	bucket := []byte("runtime_failed_rebuild_keeps_current")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)

	if err := db.Update(func(tx *bbolt.Tx) error {
		var key [8]byte
		return tx.Bucket(bucket).Put(keycodec.U64BytesWithBuf(2, &key), []byte{0xff})
	}); err != nil {
		t.Fatalf("put corrupt record: %v", err)
	}
	setRuntimeTestSequence(t, db, bucket, 2)

	if _, err := r.BuildIndex(db, bucket, nil, nil, nil, decodeRuntimeTestRec, releaseRuntimeTestRec); err == nil {
		t.Fatal("BuildIndex succeeded with corrupt record")
	}
	if got := r.CurrentSnapshot(); got == nil || got.Seq != 1 || got.UniverseCardinality() != 1 {
		t.Fatalf("current snapshot after failed BuildIndex=%v", got)
	}

	var keys []uint64
	if err := r.QueryKeysOnSnapshot(r.CurrentSnapshot(), qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after failed BuildIndex: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("QueryKeys after failed BuildIndex IDs=%v want [1]", keys)
	}
}

func TestRuntimeConfigureWriteWiresSnapshotOps(t *testing.T) {
	r := newRuntimeTestRuntime(t, false)
	var cfg wexec.Config

	r.ConfigureWrite(&cfg)
	if !cfg.Indexed {
		t.Fatal("ConfigureWrite did not enable indexed writes")
	}
	if cfg.Unique.Schema != r.schema || cfg.Unique.Current == nil {
		t.Fatalf("Unique wiring=%+v", cfg.Unique)
	}
	if cfg.SnapshotOps.Current == nil || cfg.SnapshotOps.Schema != r.schema {
		t.Fatalf("SnapshotOps wiring=%+v", cfg.SnapshotOps)
	}
	if cfg.SnapshotOps.CacheConfig != r.SnapshotCacheConfig() {
		t.Fatalf("SnapshotOps CacheConfig=%+v want %+v", cfg.SnapshotOps.CacheConfig, r.SnapshotCacheConfig())
	}
	if cfg.Unique.Current() != r.CurrentSnapshot() {
		t.Fatal("Unique.Current is not wired to runtime snapshot manager")
	}
}

func TestRuntimeCommittedInstallPanicPropagates(t *testing.T) {
	r := newRuntimeTestRuntime(t, false)
	staged := r.StageTruncate(7)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	staged.view.Release()

	defer func() {
		if v := recover(); v == nil {
			t.Fatal("InstallCommittedSnapshot did not propagate panic")
		}
		if got := r.CurrentSnapshot(); got != nil {
			t.Fatalf("current snapshot=%v want nil", got)
		}
	}()

	_ = r.InstallCommittedSnapshot(nil)
}

func TestRuntimeStoreLoadRoundTrip(t *testing.T) {
	bucket := []byte("runtime_store_load")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Score: 10, Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob", Score: 20})
	setRuntimeTestSequence(t, db, bucket, 5)

	r := newRuntimeTestRuntime(t, false)
	buildRuntimeTestIndex(t, r, db, bucket)
	snap := r.CurrentSnapshot()
	_ = r.RefreshPlannerStatsOnSnapshot(snap, func() error { return nil })

	file := filepath.Join(t.TempDir(), "runtime.rbi")
	if err := r.StoreIndexSnapshot(file, snap.Seq, [persist.UIDLen]byte{}, snap); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}

	loaded := newRuntimeTestRuntime(t, false)
	seq, err := currentBucketSequence(db, bucket)
	if err != nil {
		t.Fatalf("currentBucketSequence: %v", err)
	}
	result, err := loaded.LoadIndex(file, db.Path(), bucket, seq, [persist.UIDLen]byte{})
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	loaded.PublishLoadedPlannerStats(result.PlannerStats)

	var keys []uint64
	loadedSnap := loaded.CurrentSnapshot()
	if err := loaded.QueryKeysOnSnapshot(loadedSnap, qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("loaded QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("loaded QueryKeys IDs=%v want [1]", keys)
	}
	count, err := loaded.CountOnSnapshot(loadedSnap, qx.EQ("active", true))
	if err != nil {
		t.Fatalf("loaded Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("loaded Count=%d want 1", count)
	}
	plannerStats := loaded.PlannerStats()
	if plannerStats.UniverseCardinality != 2 || plannerStats.FieldCount == 0 {
		t.Fatalf("loaded PlannerStats=%+v", plannerStats)
	}
}
