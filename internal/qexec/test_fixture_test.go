package qexec

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const (
	testMatPredCacheMaxEntries = 24
	testMatPredCacheMaxCard    = 64 << 10
	testNumericRangeBucketSize = 512
	testNumericRangeMinKeys    = 8192
	testNumericRangeMinSpan    = 1024
	testRandStreamMix          = 0x9e3779b97f4a7c15
	nilIndexEntryKey           = indexdata.NilIndexEntryKey
)

type Options struct {
	AnalyzeInterval time.Duration

	TraceSink        func(TraceEvent)
	TraceSampleEvery int

	SnapshotMaterializedPredCacheMaxEntries     int
	SnapshotMaterializedPredCacheMaxCardinality int

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int
}

type Field struct {
	Name  string
	Value any
}

type Meta struct {
	Country string `db:"country" rbi:"index"`
}

type TestFixtureMeta = Meta

type testRec struct {
	Meta

	Name     string   `db:"name"      rbi:"index"`
	Email    string   `db:"email"     rbi:"index"`
	Age      int      `db:"age"       rbi:"index"`
	Score    float64  `db:"score"     rbi:"index"`
	Active   bool     `db:"active"    rbi:"index"`
	Tags     []string `db:"tags"      rbi:"index"`
	FullName string   `db:"full_name" rbi:"index"`
	Opt      *string  `db:"opt"       rbi:"index"`
}

type Rec = testRec

type PtrIntRec struct {
	Name   string `db:"name" rbi:"index"`
	Rank   *int   `db:"rank" rbi:"index"`
	Active bool   `db:"active" rbi:"index"`
}

type testOptions struct {
	TraceSink        func(TraceEvent)
	TraceSampleEvery int

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	MatPredCacheMaxEntries int
	MatPredCacheMaxCard    uint64
}

type testDB struct {
	rt   *schema.Schema
	exec *Runtime
	snap *snapshot.View
	seq  uint64
	cfg  snapshot.CacheConfig
}

type DB[K ~string | ~uint64, V any] struct {
	engine  *queryEngine
	options Options
	values  map[uint64]*V

	path   string
	closed bool
}

type queryEngine struct {
	snapshot *snapshot.Registry
	schema   *schema.Schema
	exec     *Runtime
	cfg      snapshot.CacheConfig
	seq      uint64
}

type testRawDB struct{}

func (testRawDB) Close() error { return nil }

func intPtr(v int) *int {
	return &v
}

func openTempDBUint64(t *testing.T, options ...Options) (*DB[uint64, Rec], string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test_uint64.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path, firstOptions(options))
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func openTempDBUint64PtrInt(t *testing.T, options ...Options) (*DB[uint64, PtrIntRec], string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test_uint64_ptr_int.db")
	db, raw := openBoltAndNew[uint64, PtrIntRec](t, path, firstOptions(options))
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func openBoltAndNew[K ~string | ~uint64, V any](tb testing.TB, path string, options Options) (*DB[K, V], *testRawDB) {
	tb.Helper()
	db := newFixtureDB[K, V](tb, path, options)
	return db, &testRawDB{}
}

func firstOptions(options []Options) Options {
	if len(options) == 0 {
		return Options{}
	}
	return options[0]
}

func newFixtureDB[K ~string | ~uint64, V any](tb testing.TB, path string, options Options) *DB[K, V] {
	tb.Helper()
	options.setDefaults()

	var v V
	rt, err := schema.Compile(reflect.TypeOf(v), schema.Config{})
	if err != nil {
		tb.Fatalf("schema.Compile: %v", err)
	}

	exec := NewRuntime(Config{
		Schema:                         rt,
		AnalyzeInterval:                options.AnalyzeInterval,
		NumericRangeBucketSize:         options.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: options.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:  options.NumericRangeBucketMinSpanKeys,
		TraceSink:                      options.TraceSink,
		TraceSampleEvery:               options.TraceSampleEvery,
	})

	qe := &queryEngine{
		snapshot: snapshot.NewRegistry(false),
		schema:   rt,
		exec:     exec,
		cfg: snapshot.CacheConfig{
			MatPredMaxEntries: options.SnapshotMaterializedPredCacheMaxEntries,
			MatPredMaxCard:    uint64(options.SnapshotMaterializedPredCacheMaxCardinality),
		},
	}
	db := &DB[K, V]{
		engine:  qe,
		options: options,
		values:  make(map[uint64]*V),
		path:    path,
	}
	return db
}

func (o *Options) setDefaults() {
	if o.TraceSink != nil && o.TraceSampleEvery == 0 {
		o.TraceSampleEvery = 1
	}
	if o.SnapshotMaterializedPredCacheMaxEntries == 0 {
		o.SnapshotMaterializedPredCacheMaxEntries = testMatPredCacheMaxEntries
	}
	if o.SnapshotMaterializedPredCacheMaxCardinality == 0 {
		o.SnapshotMaterializedPredCacheMaxCardinality = testMatPredCacheMaxCard
	}
	if o.NumericRangeBucketSize == 0 {
		o.NumericRangeBucketSize = testNumericRangeBucketSize
	}
	if o.NumericRangeBucketMinFieldKeys == 0 {
		o.NumericRangeBucketMinFieldKeys = testNumericRangeMinKeys
	}
	if o.NumericRangeBucketMinSpanKeys == 0 {
		o.NumericRangeBucketMinSpanKeys = testNumericRangeMinSpan
	}
}

func (db *DB[K, V]) Set(id K, newVal *V) error {
	idx := fixtureUint64Key(id)
	var old unsafe.Pointer
	if prev := db.values[idx]; prev != nil {
		old = unsafe.Pointer(prev)
	}
	next := new(V)
	*next = *newVal
	db.values[idx] = next
	db.publish([]snapshot.BatchEntry{{ID: idx, Old: old, New: unsafe.Pointer(next)}})
	return nil
}

func (db *DB[K, V]) BatchSet(ids []K, vals []*V) error {
	entries := make([]snapshot.BatchEntry, len(ids))
	for i := range ids {
		idx := fixtureUint64Key(ids[i])
		var old unsafe.Pointer
		if prev := db.values[idx]; prev != nil {
			old = unsafe.Pointer(prev)
		}
		next := new(V)
		*next = *vals[i]
		db.values[idx] = next
		entries[i] = snapshot.BatchEntry{ID: idx, Old: old, New: unsafe.Pointer(next)}
	}
	db.publish(entries)
	return nil
}

func (db *DB[K, V]) Patch(id K, fields []Field) error {
	idx := fixtureUint64Key(id)
	prev := db.values[idx]
	next := new(V)
	if prev != nil {
		*next = *prev
	}
	for i := range fields {
		fi, ok := fixturePatchField(reflect.ValueOf(next).Elem(), fields[i].Name)
		if ok {
			fi.Set(reflect.ValueOf(fields[i].Value).Convert(fi.Type()))
		}
	}
	var old unsafe.Pointer
	if prev != nil {
		old = unsafe.Pointer(prev)
	}
	db.values[idx] = next
	db.publish([]snapshot.BatchEntry{{ID: idx, Old: old, New: unsafe.Pointer(next)}})
	return nil
}

func (db *DB[K, V]) Delete(id K) error {
	idx := fixtureUint64Key(id)
	prev := db.values[idx]
	if prev == nil {
		return nil
	}
	delete(db.values, idx)
	db.publish([]snapshot.BatchEntry{{ID: idx, Old: unsafe.Pointer(prev)}})
	return nil
}

func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	ids, err := db.engine.currentQueryViewForTests().Query(&viewQ, true)
	if err != nil {
		return nil, err
	}
	out := make([]K, len(ids))
	for i := range ids {
		out[i] = fixtureKeyFromUint64[K](ids[i])
	}
	return out, nil
}

func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	keys, err := db.QueryKeys(q)
	if err != nil {
		return nil, err
	}
	out := make([]*V, 0, len(keys))
	for i := range keys {
		if v := db.values[fixtureUint64Key(keys[i])]; v != nil {
			out = append(out, v)
		}
	}
	return out, nil
}

func (db *DB[K, V]) BatchGet(ids ...K) ([]*V, error) {
	out := make([]*V, len(ids))
	for i := range ids {
		out[i] = db.values[fixtureUint64Key(ids[i])]
	}
	return out, nil
}

func (db *DB[K, V]) Count(exprs ...qx.Expr) (uint64, error) {
	prepared, err := qir.PrepareCountExprsResolved(db.engine.schema.IndexedByName, exprs...)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()
	ids, err := db.engine.currentQueryViewForTests().Filter(prepared)
	if err != nil {
		return 0, err
	}
	defer ids.Release()
	return ids.Cardinality(), nil
}

func (db *DB[K, V]) ReleaseRecords(v ...*V) {}

func (db *DB[K, V]) DisableSync() {}

func (db *DB[K, V]) EnableSync() {}

func (db *DB[K, V]) Close() error {
	if db.closed {
		return nil
	}
	db.closed = true
	return nil
}

func (db *DB[K, V]) publish(entries []snapshot.BatchEntry) {
	db.engine.seq++
	next := snapshot.Build(db.engine.seq, db.engine.snapshot.Current(), db.engine.schema, db.engine.cfg, nil, nil, entries)
	db.engine.snapshot.Publish(next)
}

func (db *DB[K, V]) clearCurrentSnapshotCachesForTesting() {
	if snap := db.engine.snapshot.Current(); snap != nil {
		snap.ClearRuntimeCaches()
	}
}

func fixtureUint64Key[K ~string | ~uint64](id K) uint64 {
	return uint64(any(id).(uint64))
}

func fixtureKeyFromUint64[K ~string | ~uint64](id uint64) K {
	return any(id).(K)
}

func fixturePatchField(v reflect.Value, name string) (reflect.Value, bool) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		if sf.Anonymous {
			if fv, ok := fixturePatchField(v.Field(i), name); ok {
				return fv, true
			}
		}
		if sf.Name == name || sf.Tag.Get("db") == name || sf.Tag.Get("json") == name {
			return v.Field(i), true
		}
	}
	return reflect.Value{}, false
}

func seedGeneratedUint64Data(t *testing.T, db *DB[uint64, Rec], n int, gen func(i int) *Rec) {
	t.Helper()
	ids := make([]uint64, n)
	vals := make([]*Rec, n)
	for i := 1; i <= n; i++ {
		ids[i-1] = uint64(i)
		vals[i-1] = gen(i)
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
}

func expectedKeysUint64(t testing.TB, db *DB[uint64, Rec], q *qx.QX) ([]uint64, error) {
	t.Helper()
	return db.QueryKeys(q)
}

func collectAndLeavesForTest(e qx.Expr) ([]qx.Expr, bool) {
	switch {
	case e.Is(qx.KindOP, qx.OpNOT):
		if len(e.Args) != 1 {
			return nil, false
		}
		child := e.Args[0]
		if child.Is(qx.KindOP, qx.OpAND) || child.Is(qx.KindOP, qx.OpOR) || child.Is(qx.KindOP, qx.OpNOT) || child.Kind == qx.KindNONE {
			return nil, false
		}
		if child.Kind != qx.KindOP {
			return nil, false
		}
		return []qx.Expr{e}, true
	case e.Is(qx.KindOP, qx.OpAND):
		if len(e.Args) == 0 {
			return nil, false
		}
		out := make([]qx.Expr, 0, len(e.Args))
		for i := range e.Args {
			leaves, ok := collectAndLeavesForTest(e.Args[i])
			if !ok {
				return nil, false
			}
			out = append(out, leaves...)
		}
		return out, true
	case e.Kind == qx.KindNONE:
		return []qx.Expr{e}, true
	case e.Kind == qx.KindOP:
		if e.Is(qx.KindOP, qx.OpOR) {
			return nil, false
		}
		return []qx.Expr{e}, true
	default:
		return nil, false
	}
}

func mustExtractAndLeaves(t testing.TB, e qx.Expr) []qx.Expr {
	t.Helper()
	leaves, ok := collectAndLeavesForTest(e)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves failed: ok=%v len=%d", ok, len(leaves))
	}
	return leaves
}

func queryIDsEqual(q *qx.QX, a, b []uint64) bool {
	if q == nil || len(q.Order) > 0 || slices.Equal(a, b) {
		return slices.Equal(a, b)
	}
	sa := append([]uint64(nil), a...)
	sb := append([]uint64(nil), b...)
	slices.Sort(sa)
	slices.Sort(sb)
	return slices.Equal(sa, sb)
}

func assertQueryIDsEqual(t *testing.T, q *qx.QX, a, b []uint64) {
	t.Helper()
	if testQueryNoOrderPage(q) {
		t.Fatalf("no-order page results must be checked with assertNoOrderPage: q=%+v\nA=%v\nB=%v", q, a, b)
	}
	if queryIDsEqual(q, a, b) {
		return
	}
	t.Fatalf("query ids mismatch:\nA=%v\nB=%v", a, b)
}

func assertSameSlice(t *testing.T, got, want []uint64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected ids:\ngot=%v\nwant=%v", got, want)
	}
}

func assertNoOrderPage(t testing.TB, q *qx.QX, got, full []uint64, label string) {
	t.Helper()
	if err := testValidateNoOrderPage(q, got, full); err != nil {
		t.Fatalf("%s no-order page mismatch: %v\ngot=%v\nfull=%v", label, err, got, full)
	}
}

func assertPostingConsumerSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	slices.Sort(want)
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch:\ngot=%v\nwant=%v", gotIDs, want)
	}
}

func seedData(t *testing.T, db *DB[uint64, Rec], n int) []uint64 {
	t.Helper()
	r := newRand(1)
	ids := make([]uint64, 0, n)
	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*Rec, 0, batchSize)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve"}
	tagsPool := [][]string{
		{"go", "db"},
		{"java"},
		{"rust", "go"},
		{"ops"},
		{"go", "go", "db"},
		{},
	}
	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}
	for i := 1; i <= n; i++ {
		var opt *string
		if i%4 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			opt = &s
		}
		rec := &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     names[r.IntN(len(names))],
			Age:      18 + r.IntN(50),
			Score:    math.Round((r.Float64()*100.0)*100) / 100,
			Active:   r.IntN(2) == 0,
			Tags:     append([]string(nil), tagsPool[r.IntN(len(tagsPool))]...),
			FullName: "FN-" + fmt.Sprintf("%02d", i),
			Opt:      opt,
		}
		id := uint64(i)
		ids = append(ids, id)
		batchIDs = append(batchIDs, id)
		batchVals = append(batchVals, rec)
		if len(batchIDs) == cap(batchIDs) {
			flush()
		}
	}
	flush()
	return ids
}

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^testRandStreamMix))
}

func cardinalityByExprBitmap(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.engine.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.ids.Release()
	return db.engine.currentQueryViewForTests().postingResultCardinality(b)
}

func orderWindowForTest(q *qx.QX) (int, bool) {
	return testOrderWindow(q)
}

func newTestDB(t testing.TB, opts testOptions) *testDB {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeFor[testRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	if opts.NumericRangeBucketSize == 0 {
		opts.NumericRangeBucketSize = testNumericRangeBucketSize
	}
	if opts.NumericRangeBucketMinFieldKeys == 0 {
		opts.NumericRangeBucketMinFieldKeys = testNumericRangeMinKeys
	}
	if opts.NumericRangeBucketMinSpanKeys == 0 {
		opts.NumericRangeBucketMinSpanKeys = testNumericRangeMinSpan
	}
	if opts.TraceSink != nil && opts.TraceSampleEvery == 0 {
		opts.TraceSampleEvery = 1
	}
	if opts.MatPredCacheMaxEntries == 0 {
		opts.MatPredCacheMaxEntries = testMatPredCacheMaxEntries
	}
	if opts.MatPredCacheMaxCard == 0 {
		opts.MatPredCacheMaxCard = testMatPredCacheMaxCard
	}

	exec := NewRuntime(Config{
		Schema:                         rt,
		NumericRangeBucketSize:         opts.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: opts.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:  opts.NumericRangeBucketMinSpanKeys,
		TraceSink:                      opts.TraceSink,
		TraceSampleEvery:               opts.TraceSampleEvery,
	})

	return &testDB{
		rt:   rt,
		exec: exec,
		cfg: snapshot.CacheConfig{
			MatPredMaxEntries: opts.MatPredCacheMaxEntries,
			MatPredMaxCard:    opts.MatPredCacheMaxCard,
		},
	}
}

func (db *testDB) view() *View {
	view := newView(db.snap, db.exec)
	return &view
}

func (db *testDB) clearCurrentSnapshotCaches() {
	db.snap.ClearRuntimeCaches()
}

func (db *testDB) seedData(t testing.TB, n int) []uint64 {
	t.Helper()

	r := rand.New(rand.NewPCG(1, 1^testRandStreamMix))
	ids := make([]uint64, 0, n)
	vals := make([]testRec, n)
	entries := make([]snapshot.BatchEntry, n)

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve"}
	tagsPool := [][]string{
		{"go", "db"},
		{"java"},
		{"rust", "go"},
		{"ops"},
		{"go", "go", "db"},
		{},
	}

	for i := 1; i <= n; i++ {
		var opt *string
		if i%4 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			opt = &s
		}
		vals[i-1] = testRec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     names[r.IntN(len(names))],
			Age:      18 + r.IntN(50),
			Score:    math.Round((r.Float64()*100.0)*100) / 100,
			Active:   r.IntN(2) == 0,
			Tags:     append([]string(nil), tagsPool[r.IntN(len(tagsPool))]...),
			FullName: "FN-" + fmt.Sprintf("%02d", i),
			Opt:      opt,
		}
		id := uint64(i)
		ids = append(ids, id)
		entries[i-1] = snapshot.BatchEntry{ID: id, New: unsafe.Pointer(&vals[i-1])}
	}

	db.seq++
	db.snap = snapshot.Build(db.seq, db.snap, db.rt, db.cfg, nil, nil, entries)
	return ids
}

func (db *testDB) seedGeneratedData(t testing.TB, n int, gen func(uint64) testRec) {
	t.Helper()
	vals := make([]testRec, n)
	entries := make([]snapshot.BatchEntry, n)
	for i := 1; i <= n; i++ {
		id := uint64(i)
		vals[i-1] = gen(id)
		entries[i-1] = snapshot.BatchEntry{ID: id, New: unsafe.Pointer(&vals[i-1])}
	}
	db.seq++
	db.snap = snapshot.Build(db.seq, db.snap, db.rt, db.cfg, nil, nil, entries)
}

func (db *testDB) prepareQuery(q *qx.QX) (*qir.Query, qir.Shape, error) {
	prepared, err := qir.PrepareQuery(q, db.rt.IndexedByName)
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func (db *testDB) query(q *qx.QX) ([]uint64, error) {
	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	return db.view().Query(&viewQ, false)
}

func (db *testDB) beginTrace(q qir.Shape) *Trace {
	orderField := ""
	if q.HasOrder {
		orderField = db.exec.FieldNameByOrdinal(q.Order.FieldOrdinal)
	}
	return db.exec.BeginTrace(q, orderField)
}

func (qe *queryEngine) currentQueryViewForTests() *View {
	if qe == nil || qe.snapshot == nil {
		return &View{snap: &snapshot.View{}}
	}
	if snap := qe.snapshot.Current(); snap != nil {
		return qe.queryViewForSnapshotForTests(snap)
	}
	return qe.queryViewForSnapshotForTests(&snapshot.View{})
}

func (qe *queryEngine) queryViewForSnapshotForTests(snap *snapshot.View) *View {
	if snap == nil {
		snap = &snapshot.View{}
	}
	view := newView(snap, qe.exec)
	return &view
}

func (qe *queryEngine) beginTraceForTests(q qir.Shape) *Trace {
	orderField := ""
	if q.HasOrder {
		orderField = qe.fieldNameByOrdinal(q.Order.FieldOrdinal)
	}
	return qe.exec.BeginTrace(q, orderField)
}

func (qe *queryEngine) fieldNameByOrdinal(ordinal int) string {
	return qe.exec.FieldNameByOrdinal(ordinal)
}

type testQIRResolver map[string]int

func (r testQIRResolver) ResolveField(name string) (int, bool) {
	if ordinal, ok := r[name]; ok {
		return ordinal, true
	}
	ordinal := len(r)
	r[name] = ordinal
	return ordinal, true
}

func prepareTestQuery(qe *queryEngine, q *qx.QX) (*qir.Query, qir.Shape, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareQuery(q, testQIRResolver{})
	} else {
		prepared, err = qir.PrepareQuery(q, qe.schema.IndexedByName)
	}
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func prepareTestExpr(qe *queryEngine, expr qx.Expr) (*qir.Query, qir.Expr, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareCountExprsResolved(testQIRResolver{}, expr)
	} else {
		prepared, err = qir.PrepareCountExprsResolved(qe.schema.IndexedByName, expr)
	}
	if err != nil {
		return nil, qir.Expr{}, err
	}
	return prepared, prepared.Expr, nil
}

func prepareTestExprs(qe *queryEngine, exprs []qx.Expr) ([]*qir.Query, []qir.Expr, error) {
	prepared := make([]*qir.Query, 0, len(exprs))
	out := make([]qir.Expr, 0, len(exprs))
	for i := range exprs {
		p, expr, err := prepareTestExpr(qe, exprs[i])
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return nil, nil, err
		}
		prepared = append(prepared, p)
		out = append(out, detachTestQIRExpr(expr))
	}
	return prepared, out, nil
}

func releasePreparedQueriesForTest(prepared []*qir.Query) {
	for i := range prepared {
		prepared[i].Release()
	}
}

func detachTestQIRExpr(expr qir.Expr) qir.Expr {
	if len(expr.Operands) == 0 {
		return expr
	}
	out := expr
	out.Operands = make([]qir.Expr, len(expr.Operands))
	for i := range expr.Operands {
		out.Operands[i] = detachTestQIRExpr(expr.Operands[i])
	}
	return out
}

func mustTestQIRExpr(t testing.TB, expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(nil, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func mustTestQIRExprForDB[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(db.engine, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func testExprFieldName(qe *queryEngine, expr qir.Expr) string {
	if qe == nil {
		return ""
	}
	return qe.fieldNameByOrdinal(expr.FieldOrdinal)
}

func compileScalarOpForTest(op qx.Op) qir.Op {
	switch op {
	case qx.OpEQ:
		return qir.OpEQ
	case qx.OpGT:
		return qir.OpGT
	case qx.OpGTE:
		return qir.OpGTE
	case qx.OpLT:
		return qir.OpLT
	case qx.OpLTE:
		return qir.OpLTE
	case qx.OpIN:
		return qir.OpIN
	case qx.OpHASANY:
		return qir.OpHASANY
	case qx.OpHASALL:
		return qir.OpHASALL
	case qx.OpPREFIX:
		return qir.OpPREFIX
	case qx.OpSUFFIX:
		return qir.OpSUFFIX
	case qx.OpCONTAINS:
		return qir.OpCONTAINS
	default:
		panic(fmt.Sprintf("unsupported qx op in test helper: %q", op))
	}
}

func (qe *queryEngine) evalExpr(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().evalExpr(expr)
}

func (qe *queryEngine) tryLimitRoutes(q *qx.QX, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return tryLimitRoutesForTest(qe.currentQueryViewForTests(), &viewQ, trace)
}

func tryLimitRoutesForTest(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	if viewQ.HasOrder && viewQ.Order.Kind != qir.OrderKindBasic {
		return nil, false, nil
	}
	if !viewQ.HasOrder && viewQ.Limit != 0 {
		if out, ok, plan, err := view.executeNoOrderLimit(viewQ, trace); ok {
			if trace != nil {
				trace.SetPlan(plan)
			}
			return out, true, err
		}
	}
	if viewQ.HasOrder && viewQ.Order.Kind == qir.OrderKindBasic && viewQ.Limit != 0 {
		out, ok, plan, err := view.executeOrderedLimit(viewQ, trace)
		if ok && trace != nil && plan != "" {
			trace.SetPlan(plan)
		}
		if ok {
			return out, true, err
		}
	}
	return nil, false, nil
}

func (qe *queryEngine) tryCandidateLimitRoute(q *qx.QX, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	view := qe.currentQueryViewForTests()
	if viewQ.HasOrder {
		out, ok, plan, err := view.executeOrderedLimit(&viewQ, trace)
		if ok && plan == PlanCandidateOrder {
			if trace != nil {
				trace.SetPlan(plan)
			}
			return out, true, err
		}
		return nil, false, err
	}
	out, ok, plan, err := view.executeNoOrderLimit(&viewQ, trace)
	if ok && plan == PlanCandidateNoOrder {
		if trace != nil {
			trace.SetPlan(plan)
		}
		return out, true, err
	}
	return nil, false, err
}

func (qe *queryEngine) executeOrderedLimit(q *qx.QX, trace *Trace) ([]uint64, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()
	out, ok, plan, err := qe.currentQueryViewForTests().executeOrderedLimit(&viewQ, trace)
	if ok && trace != nil && plan != "" {
		trace.SetPlan(plan)
	}
	return out, ok, err
}

func (qe *queryEngine) execSelectedNoOrderBounds(q *qx.QX, field string, bounds indexdata.Bounds, rest []qx.Expr, trace *Trace) ([]uint64, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()
	preparedRest, compiledRest, err := prepareTestExprs(qe, rest)
	if err != nil {
		return nil, false, err
	}
	defer releasePreparedQueriesForTest(preparedRest)
	return qe.currentQueryViewForTests().execSelectedNoOrderBounds(&viewQ, field, bounds, compiledRest, trace)
}

func (qe *queryEngine) execSelectedNoOrderDirectRange(q *qx.QX, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execSelectedNoOrderDirectRange(&viewQ, trace)
}

func (qe *queryEngine) execSelectedNoOrderDirectPrefix(q *qx.QX, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execSelectedNoOrderDirectPrefix(&viewQ, trace)
}

func (qe *queryEngine) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, orderedOffset uint64, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)
	preds, ok := qe.currentQueryViewForTests().buildPredicatesOrderedWithMode(compiledLeaves, orderField, allowMaterialize, orderedWindow, orderedOffset, coverOrderRange, allowOrderedEagerMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *Trace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOrderedBasicReader(&viewQ, preds, trace)
}

func (qe *queryEngine) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, trace *Trace) []uint64 {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOrderedBasicFallback(&viewQ, preds, active, ov, br, trace)
}

func (qe *queryEngine) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranches(compiledOps)
}

func (qe *queryEngine) buildORBranchesOrdered(ops []qx.Expr, orderField string, orderedWindow int) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, 0)
}

func (qe *queryEngine) buildORBranchesOrderedWithOffset(ops []qx.Expr, orderField string, orderedWindow int, orderedOffset uint64) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, orderedOffset)
}

func (qe *queryEngine) execPlanORNoOrderAdaptiveCore(q *qx.QX, branches plannerORBranches, trace *Trace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanORNoOrderAdaptiveCore(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanORNoOrderBaselineCore(q *qx.QX, branches plannerORBranches, trace *Trace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanORNoOrderBaselineCore(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *Trace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderBasic(&viewQ, branches, nil, trace, nil)
}

func (qe *queryEngine) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderMergeFallback(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *Trace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderKWay(&viewQ, branches, nil, trace)
}

func (qe *queryEngine) materializedPredCacheKey(e qx.Expr) qcache.MaterializedPredKey {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return qcache.MaterializedPredKey{}
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().materializedPredKey(expr)
}

func (qe *queryEngine) buildPredRangeCandidateWithMode(e qx.Expr, fm *schema.Field, ov indexdata.FieldIndexView, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().buildPredRangeCandidateWithMode(expr, fm, ov, allowMaterialize)
}

func (qe *queryEngine) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().buildPredicateWithMode(expr, allowMaterialize)
}

func (qe *queryEngine) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (indexdata.Bounds, []bool, bool, bool) {
	prepared := make([]*qir.Query, 0, n)
	compiled := make([]qir.Expr, 0, n)
	for i := 0; i < n; i++ {
		p, expr, err := prepareTestExpr(qe, exprAt(i))
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return indexdata.Bounds{}, nil, false, false
		}
		prepared = append(prepared, p)
		compiled = append(compiled, expr)
	}
	defer releasePreparedQueriesForTest(prepared)
	rb, covered, has, ok := qe.currentQueryViewForTests().collectOrderRangeBounds(field, n, func(i int) qir.Expr {
		return compiled[i]
	})
	return rb, copyBoolBufAndRelease(covered), has, ok
}

func (qe *queryEngine) extractOrderRangeCoverageIndexView(field string, preds []predicate, ov indexdata.FieldIndexView) (indexdata.FieldIndexRange, []bool, bool) {
	br, covered, ok := qe.currentQueryViewForTests().extractOrderRangeCoverageIndexViewReader(field, preds, ov)
	return br, copyBoolBufAndRelease(covered), ok
}

func copyBoolBufAndRelease(buf []bool) []bool {
	if buf == nil {
		return nil
	}
	out := append([]bool(nil), buf...)
	pooled.ReleaseBoolSlice(buf)
	return out
}

func (qe *queryEngine) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().shouldUseOROrderKWayRuntimeFallback(&viewQ, branches, needWindow)
}

func (qe *queryEngine) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)
	preds, ok := qe.currentQueryViewForTests().buildPredicates(compiledLeaves)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)
	preds, ok := qe.currentQueryViewForTests().buildPredicatesOrdered(compiledLeaves, orderField)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) shouldPreferExecutionNoOrderPrefix(q *qx.QX, leaves []qx.Expr) bool {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer preparedQ.Release()
	preparedLeaves, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(preparedLeaves)
	return qe.currentQueryViewForTests().shouldPreferExecutionNoOrderPrefix(&viewQ, compiledLeaves)
}

func (qe *queryEngine) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	dir := qx.ASC
	if o.Desc {
		dir = qx.DESC
	}
	order, err := qir.PrepareQuery(qx.Query().SortBy(o.By, dir), qe.schema.IndexedByName)
	if err != nil {
		return false
	}
	defer order.Release()
	orderView := qir.NewShape(order)
	if !orderView.HasOrder {
		return false
	}
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().shouldUseCandidateOrder(orderView.Order, compiledLeaves)
}

func detachPredicateSetForTests(preds predicateSet) []predicate {
	out := make([]predicate, len(preds.owner))
	for i := 0; i < len(out); i++ {
		out[i] = preds.owner[i]
	}
	if preds.owner != nil {
		for i := 0; i < len(preds.owner); i++ {
			preds.owner[i] = predicate{}
		}
		predicateSlicePool.Put(preds.owner)
	}
	return out
}

func (qe *queryEngine) tryFilterCardinalityORByPredicates(expr qx.Expr, trace *Trace) (uint64, bool, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return 0, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().TryFilterCardinalityORByPredicates(compiled, trace)
}

func (qe *queryEngine) exprValueToIdxScalar(expr qx.Expr) (string, bool, bool, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return "", false, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().exprValueToIdxScalar(compiled)
}

func (qe *queryEngine) extractNoOrderBounds(leaves []qx.Expr) (string, indexdata.Bounds, bool, error) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return "", indexdata.Bounds{}, false, err
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().extractNoOrderBounds(compiledLeaves)
}

func (qe *queryEngine) evalSimple(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().evalSimple(expr)
}

func snapshotExtLoadMaterializedPred(s *snapshot.View, key qcache.MaterializedPredKey) (posting.List, bool) {
	return s.LoadMaterializedPredKey(key)
}

func snapshotExtStoreMaterializedPred(s *snapshot.View, key qcache.MaterializedPredKey, ids posting.List) {
	s.StoreMaterializedPredKey(key, ids)
}

func (db *testDB) assertKeysMatchReference(t testing.TB, label string, q *qx.QX, got []uint64) {
	t.Helper()
	if err := testValidateNoDuplicateKeys(label, got); err != nil {
		t.Fatal(err)
	}
	if testQueryNoOrderPage(q) {
		fullQ := qx.Clone(q)
		fullQ.Order = nil
		fullQ.Window.Offset = 0
		fullQ.Window.Limit = 0
		full, err := db.query(fullQ)
		if err != nil {
			t.Fatalf("reference query(%+v): %v", fullQ, err)
		}
		if err := testValidateNoOrderPage(q, got, full); err != nil {
			t.Fatalf("%s no-order page mismatch: %v\ngot=%v\nfull=%v", label, err, got, full)
		}
		return
	}
	want, err := db.query(q)
	if err != nil {
		t.Fatalf("reference query(%+v): %v", q, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s mismatch:\nq=%+v\ngot=%v\nwant=%v", label, q, got, want)
	}
}

func testQueryNoOrderPage(q *qx.QX) bool {
	return q != nil && len(q.Order) == 0 && (q.Window.Offset > 0 || q.Window.Limit > 0)
}

func testValidateNoDuplicateKeys(label string, keys []uint64) error {
	seen := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s returned duplicate key %v", label, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func testValidateNoOrderPage(q *qx.QX, got []uint64, full []uint64) error {
	offset := int(q.Window.Offset)
	if offset > len(full) {
		offset = len(full)
	}
	wantLen := len(full) - offset
	if q.Window.Limit > 0 && int(q.Window.Limit) < wantLen {
		wantLen = int(q.Window.Limit)
	}
	if len(got) != wantLen {
		return fmt.Errorf("result length %d want %d full=%d offset=%d limit=%d", len(got), wantLen, len(full), q.Window.Offset, q.Window.Limit)
	}
	fullSet := make(map[uint64]struct{}, len(full))
	for _, key := range full {
		fullSet[key] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(got))
	for _, key := range got {
		if _, ok := fullSet[key]; !ok {
			return fmt.Errorf("key %v is outside full result set", key)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate key %v", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type traceContractRecorder struct {
	mu     sync.Mutex
	events []TraceEvent
}

func (r *traceContractRecorder) sink(ev TraceEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *traceContractRecorder) mark() int {
	r.mu.Lock()
	n := len(r.events)
	r.mu.Unlock()
	return n
}

func (r *traceContractRecorder) lastSince(t testing.TB, mark int) TraceEvent {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) <= mark {
		t.Fatalf("expected trace event after mark=%d, total=%d", mark, len(r.events))
	}
	return r.events[len(r.events)-1]
}

func traceContractAssertQueryResultTrace(t testing.TB, ev TraceEvent, rowsReturned uint64) {
	t.Helper()
	if ev.Timestamp.IsZero() {
		t.Fatalf("expected trace timestamp, trace=%+v", ev)
	}
	if ev.Duration <= 0 {
		t.Fatalf("expected positive trace duration, trace=%+v", ev)
	}
	if ev.Plan == "" {
		t.Fatalf("expected non-empty trace plan, trace=%+v", ev)
	}
	if ev.Error != "" {
		t.Fatalf("unexpected trace error=%q, trace=%+v", ev.Error, ev)
	}
	if ev.RowsReturned != rowsReturned {
		t.Fatalf("rows returned mismatch: trace=%d want=%d", ev.RowsReturned, rowsReturned)
	}
	if ev.RowsExamined < ev.RowsReturned {
		t.Fatalf("expected RowsExamined >= RowsReturned for query trace, trace=%+v", ev)
	}
}

func testOrderWindow(q *qx.QX) (int, bool) {
	if q == nil || q.Window.Limit == 0 {
		return 0, false
	}
	need := q.Window.Offset + q.Window.Limit
	if need < q.Window.Offset || need > uint64(math.MaxInt) {
		return 0, false
	}
	return int(need), true
}

func cloneQuery(q *qx.QX) *qx.QX {
	return qx.Clone(q)
}
