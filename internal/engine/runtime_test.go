package engine

import (
	"errors"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vmihailenco/msgpack/v5"
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

	data, err := msgpack.Marshal(&rec)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	if err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(id, &key), data)
	}); err != nil {
		t.Fatalf("put record: %v", err)
	}
}

func putRuntimeTestStringRec(t *testing.T, db *bbolt.DB, bucket []byte, mapBucket []byte, key string, rec runtimeTestRec) uint64 {
	t.Helper()

	data, err := msgpack.Marshal(&rec)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	var idx uint64
	if err = db.Update(func(tx *bbolt.Tx) error {
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

func newRuntimeTestRuntime(t *testing.T, strKey bool, snapshotStats bool) *Index {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(runtimeTestRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	r, err := NewIndex(Config{Schema: rt, StrKey: strKey, SnapshotStats: snapshotStats})
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
	if err := msgpack.Unmarshal(data, rec); err != nil {
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func releaseRuntimeTestRec(ptr unsafe.Pointer) {
	*(*runtimeTestRec)(ptr) = runtimeTestRec{}
}

func TestRuntimeStringKeyIndexOnlyCreatesRuntimeAndResolvesKey(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(runtimeTestNoIndexRec{}), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	numeric, err := NewIndex(Config{Schema: rt, StrKey: false, StrKeyIndex: true})
	if err != nil {
		t.Fatalf("numeric NewIndex: %v", err)
	}
	if numeric != nil {
		t.Fatal("numeric StringKeyIndex must not activate key-index-only runtime")
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

	var got []uint64
	if err := r.QueryKeys(q, func(ids []uint64) error {
		got = append(got, ids...)
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	return got
}

func TestRuntimeScanStringKeysUsesKeyIndexSnapshot(t *testing.T) {
	r := newRuntimeStringKeyIndexRuntime(t)

	r.publishStorageSnapshotNoLock(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"a", "b", "c"}, []uint64{11, 12, 13}),
		Universe: posting.BuildFromSorted([]uint64{
			11, 12, 13,
		}),
	})

	var got []string
	var saved string
	if err := r.ScanStringKeys("b", func(key string) (bool, error) {
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
	if err := r.ScanStringKeys("a", func(key string) (bool, error) {
		got = append(got, key)
		return false, nil
	}); err != nil {
		t.Fatalf("ScanStringKeys stop: %v", err)
	}
	if !slices.Equal(got, []string{"a"}) {
		t.Fatalf("ScanStringKeys stop keys=%v want [a]", got)
	}

	scanErr := errors.New("scan error")
	err := r.ScanStringKeys("a", func(string) (bool, error) {
		return true, scanErr
	})
	if !errors.Is(err, scanErr) {
		t.Fatalf("ScanStringKeys error=%v want %v", err, scanErr)
	}

	r.publishStorageSnapshotNoLock(4, snapshot.Storage{
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
	r.publishStorageSnapshotNoLock(3, snapshot.Storage{
		KeyIndex: runtimeTestStringKeyStorage([]string{"alpha", "beta", "bravo", "carrot"}, []uint64{40, 10, 30, 20}),
		Universe: posting.BuildFromSorted([]uint64{
			10, 20, 30, 40,
		}),
	})

	stats := r.IndexStats()
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
	r.publishStorageSnapshotNoLock(3, snapshot.Storage{
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
	r.publishStorageSnapshotNoLock(3, snapshot.Storage{
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
		err := r.QueryKeys(q, func([]uint64) error { return nil })
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

func TestRuntimeBuildQueryCountAggregatePlannerAndStats(t *testing.T) {
	bucket := []byte("runtime_dispatch")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Tags: []string{"go"}, Score: 10, Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob", Tags: []string{"db"}, Score: 20})
	putRuntimeTestRec(t, db, bucket, 3, runtimeTestRec{Name: "alice", Tags: []string{"go", "db"}, Score: 7, Active: true})
	setRuntimeTestSequence(t, db, bucket, 3)

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)

	var keys []uint64
	if err := r.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 3}) {
		t.Fatalf("QueryKeys IDs=%v want [1 3]", keys)
	}

	count, err := r.Count(qx.EQ("active", true))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Count=%d want 2", count)
	}

	agg, err := r.Aggregate(qx.Query(qx.EQ("active", true)).Metrics(qx.ROWCOUNT().AS("rows")))
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

	called := false
	unavailableErr := errors.New("unavailable")
	err = r.Query(qx.Query(qx.EQ("name", "alice")), db, bucket, func() error {
		return unavailableErr
	}, func(tx *bbolt.Tx, ids []uint64) error {
		called = true
		if !slices.Equal(ids, []uint64{1, 3}) {
			t.Fatalf("Query callback IDs=%v want [1 3]", ids)
		}
		var key [8]byte
		raw := tx.Bucket(bucket).Get(keycodec.U64BytesWithBuf(ids[0], &key))
		if raw == nil {
			t.Fatal("Query callback did not receive sequence-aligned bolt tx")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !called {
		t.Fatal("Query callback was not called")
	}

	dbStats, err := r.DBStats(db, bucket, nil)
	if err != nil {
		t.Fatalf("DBStats: %v", err)
	}
	if dbStats.Sequence != 3 || dbStats.KeyCount != 3 || !dbStats.HasLast || keycodec.UserKeyFromDataKey[uint64](dbStats.LastKey, false) != 3 {
		t.Fatalf("DBStats=%+v", dbStats)
	}
	snapshotStats := r.SnapshotStats()
	if snapshotStats.Sequence != 3 || snapshotStats.UniverseCard != 3 || snapshotStats.RegistrySize != 1 {
		t.Fatalf("SnapshotStats=%+v", snapshotStats)
	}
	indexStats := r.IndexStats()
	if indexStats.FieldTotalCardinality["name"] != 3 || indexStats.FieldTotalCardinality["active"] != 3 {
		t.Fatalf("IndexStats.FieldTotalCardinality=%v", indexStats.FieldTotalCardinality)
	}
	if indexStats.StringKeyIndex != nil {
		t.Fatalf("IndexStats.StringKeyIndex=%+v, want nil", indexStats.StringKeyIndex)
	}

	if err = r.RefreshPlannerStats(func() error { return nil }); err != nil {
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

	r := newRuntimeTestRuntime(t, true, true)
	result, err := r.BuildIndex(db, bucket, mapBucket, nil, nil, decodeRuntimeTestRec, releaseRuntimeTestRec)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	if !result.Stats {
		t.Fatal("BuildIndex did not report stats")
	}

	var got []uint64
	if err := r.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(ids []uint64) error {
		got = ids
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(got, []uint64{1, 3}) {
		t.Fatalf("QueryKeys IDs=%v want [1 3]", got)
	}

	st, err := r.DBStats(db, bucket, nil)
	if err != nil {
		t.Fatalf("DBStats: %v", err)
	}
	if st.Sequence != 3 || st.KeyCount != 3 || !st.HasLast || keycodec.UserKeyFromDataKey[string](st.LastKey, true) != "c" {
		t.Fatalf("string DBStats=%+v", st)
	}
}

func TestRuntimeStageTruncatePublishesEmptySnapshot(t *testing.T) {
	bucket := []byte("runtime_truncate")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)

	staged := r.StageTruncate(4)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	if got := r.snapshot.Current(); got == nil || got.Seq != 2 {
		t.Fatalf("current snapshot before publish=%v", got)
	}
	if snap, seq, ref := r.snapshot.PinCurrent(); snap == nil || seq != 2 || ref == nil {
		t.Fatalf("PinCurrent before publish snap=%v seq=%d ref=%v", snap, seq, ref)
	} else {
		r.snapshot.Unpin(seq, ref)
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

	var broken atomic.Bool
	err := r.PublishCommittedStaged(&broken, log.New(io.Discard, "", 0), 4, "truncate", staged)
	if err != nil {
		t.Fatalf("PublishCommittedStaged: %v", err)
	}
	if got := r.snapshot.Current(); got == nil || got.Seq != 4 || got.UniverseCardinality() != 0 {
		t.Fatalf("current snapshot after truncate=%v", got)
	}

	var keys []uint64
	if err := r.QueryKeys(qx.Query(), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after truncate: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("QueryKeys after truncate=%v want empty", keys)
	}
	stats, err := r.DBStats(db, bucket, nil)
	if err != nil {
		t.Fatalf("DBStats after truncate: %v", err)
	}
	if stats.Sequence != 4 || stats.KeyCount != 0 || stats.HasLast {
		t.Fatalf("DBStats after truncate=%+v", stats)
	}
}

func TestRuntimePinnedCurrentSurvivesPublishedTruncate(t *testing.T) {
	bucket := []byte("runtime_pinned_current_truncate")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)

	prepared, shape, err := r.prepareQuery(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	old, seq, ref := r.snapshot.PinCurrent()
	if old == nil || seq != 2 || ref == nil {
		t.Fatalf("PinCurrent old snap=%v seq=%d ref=%v", old, seq, ref)
	}
	defer r.snapshot.Unpin(seq, ref)

	staged := r.StageTruncate(4)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	var broken atomic.Bool
	if err = r.PublishCommittedStaged(&broken, log.New(io.Discard, "", 0), 4, "truncate", staged); err != nil {
		t.Fatalf("PublishCommittedStaged: %v", err)
	}
	if got := r.snapshot.Current(); got == nil || got.Seq != 4 || got.UniverseCardinality() != 0 {
		t.Fatalf("current snapshot after truncate=%v", got)
	}

	keys, err := r.queryKeysOnSnapshot(old, &shape, false)
	if err != nil {
		t.Fatalf("query old snapshot: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("old snapshot query IDs=%v want [1]", keys)
	}

	var currentKeys []uint64
	if err = r.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
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

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)

	file := filepath.Join(t.TempDir(), "runtime.rbi")
	if err := r.StoreIndex(file, db, bucket); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}

	prepared, shape, err := r.prepareQuery(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	old, seq, ref := r.snapshot.PinCurrent()
	if old == nil || seq != 2 || ref == nil {
		t.Fatalf("PinCurrent old snap=%v seq=%d ref=%v", old, seq, ref)
	}
	defer r.snapshot.Unpin(seq, ref)

	if _, err = r.LoadIndex(file, db.Path(), bucket, seq); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	if got := r.snapshot.Current(); got == nil || got == old || got.Seq != 2 {
		t.Fatalf("current snapshot after LoadIndex=%v old=%v", got, old)
	}

	keys, err := r.queryKeysOnSnapshot(old, &shape, false)
	if err != nil {
		t.Fatalf("query old snapshot: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("old snapshot query IDs=%v want [1]", keys)
	}
}

func TestRuntimeDBStatsMissingSnapshotReturnsUnavailable(t *testing.T) {
	bucket := []byte("runtime_stats_missing_snapshot")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice"})
	setRuntimeTestSequence(t, db, bucket, 2)

	r := newRuntimeTestRuntime(t, false, true)
	errUnavailable := errors.New("unavailable")
	_, err := r.DBStats(db, bucket, func() error {
		return errUnavailable
	})
	if !errors.Is(err, errUnavailable) {
		t.Fatalf("DBStats err=%v want %v", err, errUnavailable)
	}
}

func TestRuntimeDropStagedKeepsCurrentSnapshotQueryable(t *testing.T) {
	bucket := []byte("runtime_drop_staged")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)

	staged := r.StageTruncate(2)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	r.DropStaged(2)

	if snap, ref, ok := r.snapshot.PinBySeq(2); ok {
		r.snapshot.Unpin(2, ref)
		t.Fatalf("staged snapshot survived DropStaged: %v", snap)
	}
	if got := r.snapshot.Current(); got == nil || got.Seq != 1 || got.UniverseCardinality() != 1 {
		t.Fatalf("current snapshot after DropStaged=%v", got)
	}

	var keys []uint64
	if err := r.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after DropStaged: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("QueryKeys after DropStaged IDs=%v want [1]", keys)
	}
}

func TestRuntimeFailedBuildIndexKeepsCurrentSnapshotQueryable(t *testing.T) {
	bucket := []byte("runtime_failed_rebuild_keeps_current")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Active: true})
	setRuntimeTestSequence(t, db, bucket, 1)

	r := newRuntimeTestRuntime(t, false, true)
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
	if got := r.snapshot.Current(); got == nil || got.Seq != 1 || got.UniverseCardinality() != 1 {
		t.Fatalf("current snapshot after failed BuildIndex=%v", got)
	}

	var keys []uint64
	if err := r.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("QueryKeys after failed BuildIndex: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("QueryKeys after failed BuildIndex IDs=%v want [1]", keys)
	}
}

func TestRuntimeConfigureWriteWiresSnapshotPublish(t *testing.T) {
	r := newRuntimeTestRuntime(t, false, true)
	var cfg wexec.Config
	var broken atomic.Bool

	r.ConfigureWrite(&cfg, &broken, log.New(io.Discard, "", 0))
	if !cfg.Indexed {
		t.Fatal("ConfigureWrite did not enable indexed writes")
	}
	if cfg.Unique.Schema != r.schema || cfg.Unique.Current == nil {
		t.Fatalf("Unique wiring=%+v", cfg.Unique)
	}
	if cfg.SnapshotOps.Manager != r.snapshot || cfg.SnapshotOps.Schema != r.schema {
		t.Fatalf("SnapshotOps wiring=%+v", cfg.SnapshotOps)
	}
	if cfg.SnapshotOps.CacheConfig != r.SnapshotCacheConfig() {
		t.Fatalf("SnapshotOps CacheConfig=%+v want %+v", cfg.SnapshotOps.CacheConfig, r.SnapshotCacheConfig())
	}
	if cfg.PublishCommitted == nil {
		t.Fatal("ConfigureWrite did not install publish callback")
	}

	staged := r.StageTruncate(11)
	if err := cfg.PublishCommitted(11, "set", staged.view); err != nil {
		t.Fatalf("PublishCommitted callback: %v", err)
	}
	if got := r.snapshot.Current(); got == nil || got.Seq != 11 {
		t.Fatalf("current snapshot after callback=%v", got)
	}
	if cfg.Unique.Current() != r.snapshot.Current() {
		t.Fatal("Unique.Current is not wired to runtime snapshot manager")
	}
}

func TestRuntimeCommittedPublishPanicBreaksAndDropsStaged(t *testing.T) {
	r := newRuntimeTestRuntime(t, false, true)
	staged := r.StageTruncate(7)
	if staged.view == nil {
		t.Fatal("StageTruncate returned empty staged snapshot")
	}
	if snap, ref, ok := r.snapshot.PinBySeq(7); !ok || snap != staged.view || ref == nil {
		t.Fatalf("staged snapshot was not pinnable before panic")
	} else {
		r.snapshot.Unpin(7, ref)
	}

	var broken atomic.Bool
	err := func() (err error) {
		defer r.recoverCommittedPublishPanic(&broken, log.New(io.Discard, "", 0), 7, "truncate", &err)
		panic("failpoint: publish truncate")
	}()
	if !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("recover err=%v want broken", err)
	}
	if !broken.Load() {
		t.Fatal("broken flag was not set")
	}
	if snap, ref, ok := r.snapshot.PinBySeq(7); ok {
		r.snapshot.Unpin(7, ref)
		t.Fatalf("staged snapshot survived recovery: %v", snap)
	}
	if got := r.snapshot.Current(); got != nil {
		t.Fatalf("current snapshot=%v want nil", got)
	}
}

func TestRuntimeAnalyzeStopSignalConcurrentWithStop(t *testing.T) {
	r := newRuntimeTestRuntime(t, false, false)

	for i := 0; i < 1000; i++ {
		stop := make(chan struct{})
		done := make(chan struct{})
		r.exec.Analyzer.Lock()
		r.exec.Analyzer.Stop = stop
		r.exec.Analyzer.Done = done
		r.exec.Analyzer.Unlock()

		var doneWG sync.WaitGroup
		doneWG.Add(1)
		go func() {
			defer doneWG.Done()
			<-stop
			close(done)
		}()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.stopAnalyzerSignal()
		}()
		go func() {
			defer wg.Done()
			r.StopAnalyzeLoop()
		}()
		wg.Wait()
		doneWG.Wait()

		r.exec.Analyzer.Lock()
		if r.exec.Analyzer.Stop != nil || r.exec.Analyzer.Done != nil {
			t.Fatalf("iteration %d analyzer state stop=%v done=%v", i, r.exec.Analyzer.Stop, r.exec.Analyzer.Done)
		}
		r.exec.Analyzer.Unlock()
	}
}

func TestRuntimeStoreLoadRoundTrip(t *testing.T) {
	bucket := []byte("runtime_store_load")
	db := openRuntimeTestBolt(t, bucket)
	putRuntimeTestRec(t, db, bucket, 1, runtimeTestRec{Name: "alice", Score: 10, Active: true})
	putRuntimeTestRec(t, db, bucket, 2, runtimeTestRec{Name: "bob", Score: 20})
	setRuntimeTestSequence(t, db, bucket, 5)

	r := newRuntimeTestRuntime(t, false, true)
	buildRuntimeTestIndex(t, r, db, bucket)
	r.RefreshPlannerStatsLocked()

	file := filepath.Join(t.TempDir(), "runtime.rbi")
	if err := r.StoreIndex(file, db, bucket); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}

	loaded := newRuntimeTestRuntime(t, false, true)
	seq, err := currentBucketSequence(db, bucket)
	if err != nil {
		t.Fatalf("currentBucketSequence: %v", err)
	}
	result, err := loaded.LoadIndex(file, db.Path(), bucket, seq)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	loaded.PublishLoadedPlannerStats(result.PlannerStats)

	var keys []uint64
	if err := loaded.QueryKeys(qx.Query(qx.EQ("name", "alice")), func(k []uint64) error {
		keys = k
		return nil
	}); err != nil {
		t.Fatalf("loaded QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("loaded QueryKeys IDs=%v want [1]", keys)
	}
	count, err := loaded.Count(qx.EQ("active", true))
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
