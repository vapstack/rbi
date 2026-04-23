package rbi

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
	"go.etcd.io/bbolt"
)

type Meta struct {
	Country string `db:"country" dbi:"default"`
}

type Rec struct {
	Meta

	Name   string   `db:"name"   dbi:"default"`
	Email  string   `db:"email"  dbi:"default"`
	Age    int      `db:"age"    dbi:"default"`
	Score  float64  `db:"score"  dbi:"default"`
	Active bool     `db:"active" dbi:"default"`
	Tags   []string `db:"tags"   dbi:"default"`

	FullName string  `db:"full_name" json:"fullName" dbi:"default"`
	Opt      *string `db:"opt"                       dbi:"default"`
}

func openTempDBUint64(t *testing.T, options ...Options) (*DB[uint64, Rec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_uint64.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func openTempDBString(t *testing.T, options ...Options) (*DB[string, Rec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_string.db")
	db, raw := openBoltAndNew[string, Rec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func openBoltAndNew[K ~string | ~uint64, V any](tb testing.TB, path string, options ...Options) (*DB[K, V], *bbolt.DB) {
	tb.Helper()
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		tb.Fatalf("bbolt.Open: %v", err)
	}
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	opts = testOptions(opts)
	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true
	db, err := New[K, V](raw, opts)
	if err != nil {
		_ = raw.Close()
		tb.Fatalf("New: %v", err)
	}
	return db, raw
}

func TestQuery_TryQueryEmptyOnSnapshot_SimpleScalarLeaf(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 8; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u%d", i),
			Age:    18 + i,
			Active: i%2 == 0,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	qv := db.currentQueryViewForTests()

	prepared, viewQ, err := prepareTestQuery(db, qx.Query(qx.GT("age", 100)))
	if err != nil {
		t.Fatalf("prepareTestQuery(no-match): %v", err)
	}
	empty, err := qv.tryQueryEmptyOnSnapshot(&viewQ)
	prepared.Release()
	if err != nil {
		t.Fatalf("tryQueryEmptyOnSnapshot(no-match): %v", err)
	}
	if !empty {
		t.Fatalf("expected GT(age,100) to be proven empty without tx")
	}

	prepared, viewQ, err = prepareTestQuery(db, qx.Query(qx.GTE("age", 20)))
	if err != nil {
		t.Fatalf("prepareTestQuery(hit): %v", err)
	}
	empty, err = qv.tryQueryEmptyOnSnapshot(&viewQ)
	prepared.Release()
	if err != nil {
		t.Fatalf("tryQueryEmptyOnSnapshot(hit): %v", err)
	}
	if empty {
		t.Fatalf("did not expect GTE(age,20) hit query to be proven empty")
	}
}

func seedData(t *testing.T, db *DB[uint64, Rec], n int) []uint64 {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

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
			Tags:     append([]string(nil), tagsPool[r.IntN(len(tagsPool))]...), // clone slice
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

func seedGeneratedUint64Data(t *testing.T, db *DB[uint64, Rec], n int, gen func(i int) *Rec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*Rec, 0, batchSize)

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
		batchIDs = append(batchIDs, uint64(i))
		batchVals = append(batchVals, gen(i))
		if len(batchIDs) == cap(batchIDs) {
			flush()
		}
	}
	flush()
}

func mustExtractAndLeaves(t testing.TB, e qx.Expr) []qx.Expr {
	t.Helper()
	leaves, ok := collectAndLeavesForTest(e)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves failed: ok=%v len=%d", ok, len(leaves))
	}
	return leaves
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

func orderWindowForTest(q *qx.QX) (int, bool) {
	if q == nil || q.Window.Limit == 0 {
		return 0, false
	}
	need := q.Window.Offset + q.Window.Limit
	if need < q.Window.Offset {
		return 0, false
	}
	if need > uint64(math.MaxInt) {
		return 0, false
	}
	return int(need), true
}

func testOrderBasic(field string, desc bool) qx.Order {
	return qx.Order{By: qx.REF(field), Desc: desc}
}

func testOrderByArrayPos(field string, priority []string, desc bool) qx.Order {
	return qx.Order{By: qx.POS(field, priority), Desc: desc}
}

func testOrderByArrayCount(field string, desc bool) qx.Order {
	return qx.Order{By: qx.LEN(field), Desc: desc}
}

func queryTestOrderField(q *qx.QX) string {
	if q == nil || len(q.Order) == 0 {
		return ""
	}
	by := q.Order[0].By
	if by.Kind == qx.KindREF {
		return by.Name
	}
	if by.Kind == qx.KindOP && len(by.Args) > 0 && by.Args[0].Kind == qx.KindREF {
		return by.Args[0].Name
	}
	return ""
}

func queryTestOrderIsArrayPosOnField(q *qx.QX, field string) bool {
	if q == nil || len(q.Order) == 0 {
		return false
	}
	by := q.Order[0].By
	return by.Is(qx.KindOP, qx.OpPOS) && len(by.Args) == 2 && by.Args[0].Kind == qx.KindREF && by.Args[0].Name == field
}

func queryTestOrderLabel(q *qx.QX) string {
	if q == nil || len(q.Order) == 0 {
		return "none"
	}
	by := q.Order[0].By
	if by.Kind == qx.KindREF {
		return "basic:" + by.Name
	}
	if by.Kind == qx.KindOP {
		return by.Name
	}
	return by.Kind
}

func queryTestOrderValues(q *qx.QX) []string {
	if q == nil || len(q.Order) == 0 {
		return nil
	}
	by := q.Order[0].By
	if by.Kind != qx.KindOP || len(by.Args) != 2 {
		return nil
	}
	vals, _ := by.Args[1].Value.([]string)
	return vals
}

func TestExtractAndLeavesRejectsNegatedAndGroup(t *testing.T) {
	e := qx.AND(
		qx.NOT(qx.AND(
			qx.EQ("email", "a@example.com"),
			qx.EQ("age", 42),
		)),
		qx.EQ("active", true),
	)

	leaves, ok := extractAndLeaves(mustTestQIRExpr(t, e))
	if ok || leaves != nil {
		t.Fatalf("expected negated AND group to be rejected, got ok=%v leaves=%v", ok, leaves)
	}
}

func TestCollectAndLeavesFixedRejectsNegatedAndGroup(t *testing.T) {
	e := qx.AND(
		qx.NOT(qx.AND(
			qx.EQ("email", "a@example.com"),
			qx.EQ("age", 42),
		)),
		qx.EQ("active", true),
	)

	var buf [4]qir.Expr
	leaves, ok := collectAndLeavesFixed(mustTestQIRExpr(t, e), buf[:0])
	if ok || leaves != nil {
		t.Fatalf("expected negated AND group to be rejected, got ok=%v leaves=%v", ok, leaves)
	}
}

func containsAll(haystack []string, needles []string) bool {
	if len(needles) == 0 {
		return false // in logic context usually empty filter matches nothing or needs definition
	}
	set := make(map[string]int, len(haystack))
	for _, v := range haystack {
		set[v]++
	}
	for _, n := range needles {
		if set[n] == 0 {
			return false
		}
	}
	return true
}

func containsAny(haystack []string, needles []string) bool {
	if len(needles) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(haystack))
	for _, v := range haystack {
		set[v] = struct{}{}
	}
	for _, n := range needles {
		if _, ok := set[n]; ok {
			return true
		}
	}
	return false
}

func distinctCountStrings(s []string) int {
	if len(s) == 0 {
		return 0
	}
	m := make(map[string]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return len(m)
}

func fieldValue(rec *Rec, field string) any {
	switch field {
	case "country":
		return rec.Country
	case "name":
		return rec.Name
	case "email":
		return rec.Email
	case "age":
		return rec.Age
	case "score":
		return rec.Score
	case "active":
		return rec.Active
	case "tags":
		return rec.Tags
	case "full_name":
		return rec.FullName
	case "fullName":
		return rec.FullName
	case "opt":
		if rec.Opt == nil {
			return nil
		}
		return *rec.Opt
	default:
		return nil
	}
}

type testExprFieldResolver struct{}

func (testExprFieldResolver) ResolveField(name string) (int, bool) {
	switch name {
	case "country":
		return 0, true
	case "name":
		return 1, true
	case "email":
		return 2, true
	case "age":
		return 3, true
	case "score":
		return 4, true
	case "active":
		return 5, true
	case "tags":
		return 6, true
	case "full_name", "fullName":
		return 7, true
	case "opt":
		return 8, true
	default:
		return 0, false
	}
}

func testExprFieldNameByOrdinal(ordinal int) string {
	switch ordinal {
	case 0:
		return "country"
	case 1:
		return "name"
	case 2:
		return "email"
	case 3:
		return "age"
	case 4:
		return "score"
	case 5:
		return "active"
	case 6:
		return "tags"
	case 7:
		return "full_name"
	case 8:
		return "opt"
	default:
		return ""
	}
}

func fieldValueByOrdinal(rec *Rec, ordinal int) any {
	return fieldValue(rec, testExprFieldNameByOrdinal(ordinal))
}

func asString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case *string:
		if x == nil {
			return "", true
		}
		return *x, true
	default:
		return "", false
	}
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	default:
		return 0, false
	}
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case uint64:
		return int64(x), true
	case float64:
		if x != math.Trunc(x) {
			return 0, false
		}
		return int64(x), true
	default:
		return 0, false
	}
}

func compareOrderedFieldValues(va, vb any, desc bool) int {
	switch {
	case va == nil && vb == nil:
		return 0
	case va == nil:
		return 1
	case vb == nil:
		return -1
	}

	switch xa := va.(type) {
	case int:
		xb, ok := vb.(int)
		if !ok {
			return 0
		}
		switch {
		case xa < xb:
			if desc {
				return 1
			}
			return -1
		case xa > xb:
			if desc {
				return -1
			}
			return 1
		default:
			return 0
		}
	case float64:
		xb, ok := vb.(float64)
		if !ok {
			return 0
		}
		switch cmp := compareFloat64QuerySemantics(xa, xb); {
		case cmp < 0:
			if desc {
				return 1
			}
			return -1
		case cmp > 0:
			if desc {
				return -1
			}
			return 1
		default:
			return 0
		}
	case string:
		xb, ok := vb.(string)
		if !ok {
			return 0
		}
		switch {
		case xa < xb:
			if desc {
				return 1
			}
			return -1
		case xa > xb:
			if desc {
				return -1
			}
			return 1
		default:
			return 0
		}
	case bool:
		xb, ok := vb.(bool)
		if !ok {
			return 0
		}
		switch {
		case !xa && xb:
			if desc {
				return 1
			}
			return -1
		case xa && !xb:
			if desc {
				return -1
			}
			return 1
		default:
			return 0
		}
	default:
		return 0
	}
}

func queryValueEqualsScalar(value any, want any) (bool, error) {
	switch w := want.(type) {
	case nil:
		return value == nil, nil
	case int:
		i, ok := asInt(value)
		return ok && i == int64(w), nil
	case float64:
		f, ok := asFloat(value)
		return ok && compareFloat64QuerySemantics(f, w) == 0, nil
	case bool:
		b, ok := value.(bool)
		return ok && b == w, nil
	case string:
		s, ok := asString(value)
		return ok && s == w, nil
	default:
		return false, fmt.Errorf("test harness: unsupported scalar query value %T", want)
	}
}

func querySliceContainsScalar(values any, want any) (bool, error) {
	if values == nil {
		return false, nil
	}
	v := reflect.ValueOf(values)
	v, isNil := unwrapExprValue(v)
	if isNil {
		return want == nil, nil
	}
	if v.Kind() != reflect.Slice {
		return false, fmt.Errorf("test harness: IN expects a slice, got %T", values)
	}

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		raw := any(nil)
		if elem.IsValid() && elem.CanInterface() {
			raw = elem.Interface()
		}
		elem, elemNil := unwrapExprValue(elem)
		if elemNil {
			if want == nil {
				return true, nil
			}
			continue
		}
		if elem.IsValid() && elem.CanInterface() {
			raw = elem.Interface()
		}
		match, err := queryValueEqualsScalar(raw, want)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// evalExprBool tries to implement the query logic to serve as a reference implementation.
func evalExprBool(rec *Rec, e qx.Expr) (bool, error) {
	prepared, err := qir.PrepareCountExprResolved(testExprFieldResolver{}, e)
	if err != nil {
		return false, err
	}
	defer prepared.Release()
	return evalPreparedExprBool(rec, prepared.Expr)
}

func evalPreparedExprBool(rec *Rec, e qir.Expr) (bool, error) {
	if e.Op == qir.OpNOOP {
		if e.Not {
			return false, nil
		}
		return true, nil
	}

	if e.Op == qir.OpAND || e.Op == qir.OpOR {
		if len(e.Operands) == 0 {
			return false, ErrInvalidQuery
		}
		if e.Op == qir.OpAND {
			out := true
			for _, ch := range e.Operands {
				b, err := evalPreparedExprBool(rec, ch)
				if err != nil {
					return false, err
				}
				out = out && b
				if !out {
					break
				}
			}
			if e.Not {
				out = !out
			}
			return out, nil
		}
		// OpOR
		out := false
		for _, ch := range e.Operands {
			b, err := evalPreparedExprBool(rec, ch)
			if err != nil {
				return false, err
			}
			out = out || b
			if out {
				break
			}
		}
		if e.Not {
			out = !out
		}
		return out, nil
	}

	fieldName := testExprFieldNameByOrdinal(e.FieldOrdinal)
	fv := fieldValueByOrdinal(rec, e.FieldOrdinal)

	var out bool

	switch e.Op {
	case qir.OpEQ:
		if e.Value == nil {
			out = fv == nil
			break
		}

		switch v := fv.(type) {
		case nil:
			out = false
		case int:
			i, ok2 := asInt(e.Value)
			out = ok2 && int64(v) == i
		case float64:
			f, ok2 := asFloat(e.Value)
			out = ok2 && compareFloat64QuerySemantics(v, f) == 0
		case bool:
			b, ok2 := e.Value.(bool)
			out = ok2 && v == b
		case string:
			s, ok2 := e.Value.(string)
			out = ok2 && v == s
		case []string:
			return false, fmt.Errorf("test harness: EQ not defined for slice field %q", fieldName)
		default:
			return false, fmt.Errorf("test harness: unsupported EQ field type %T", fv)
		}

	case qir.OpIN:
		var err error
		out, err = querySliceContainsScalar(e.Value, fv)
		if err != nil {
			return false, err
		}

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE:
		if fv == nil || e.Value == nil {
			out = false
			break
		}
		switch v := fv.(type) {
		case int:
			i, ok2 := asInt(e.Value)
			if !ok2 {
				return false, fmt.Errorf("test harness: bad numeric value for %v", e.Op)
			}
			switch e.Op {
			case qir.OpGT:
				out = int64(v) > i
			case qir.OpGTE:
				out = int64(v) >= i
			case qir.OpLT:
				out = int64(v) < i
			case qir.OpLTE:
				out = int64(v) <= i
			}
		case float64:
			f, ok2 := asFloat(e.Value)
			if !ok2 {
				return false, fmt.Errorf("test harness: bad float value for %v", e.Op)
			}
			cmp := compareFloat64QuerySemantics(v, f)
			switch e.Op {
			case qir.OpGT:
				out = cmp > 0
			case qir.OpGTE:
				out = cmp >= 0
			case qir.OpLT:
				out = cmp < 0
			case qir.OpLTE:
				out = cmp <= 0
			}
		case string:
			s, ok2 := asString(e.Value)
			if !ok2 {
				return false, fmt.Errorf("test harness: bad string value for %v", e.Op)
			}
			switch e.Op {
			case qir.OpGT:
				out = v > s
			case qir.OpGTE:
				out = v >= s
			case qir.OpLT:
				out = v < s
			case qir.OpLTE:
				out = v <= s
			}
		default:
			return false, fmt.Errorf("test harness: unsupported range field type %T", fv)
		}

	case qir.OpPREFIX:
		if fv == nil || e.Value == nil {
			out = false
			break
		}
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: PREFIX only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: PREFIX expects string")
		}
		out = strings.HasPrefix(s, p)

	case qir.OpSUFFIX:
		if fv == nil || e.Value == nil {
			out = false
			break
		}
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: SUFFIX only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: SUFFIX expects string")
		}
		out = strings.HasSuffix(s, p)

	case qir.OpCONTAINS:
		if fv == nil || e.Value == nil {
			out = false
			break
		}
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: CONTAINS only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: CONTAINS expects string")
		}
		out = strings.Contains(s, p)

	case qir.OpHASALL, qir.OpHASANY:
		arr, ok2 := fv.([]string)
		if !ok2 {
			return false, fmt.Errorf("test harness: %v only for []string in this test", e.Op)
		}
		if e.Value == nil {
			switch e.Op {
			case qir.OpHASALL:
				return false, fmt.Errorf("HAS: no values provided")
			case qir.OpHASANY:
				out = false
			}
			break
		}
		needles, ok3 := e.Value.([]string)
		if !ok3 {
			return false, fmt.Errorf("test harness: %v expects []string", e.Op)
		}
		switch e.Op {
		case qir.OpHASALL:
			out = containsAll(arr, needles)
		case qir.OpHASANY:
			out = containsAny(arr, needles)
		}

	default:
		return false, fmt.Errorf("test harness: unsupported op %v", e.Op)
	}

	if e.Not {
		out = !out
	}
	return out, nil
}

// expectedKeysUint64 scans the DB linearly and applies logic to produce the expected result set
func expectedKeysUint64(t *testing.T, db *DB[uint64, Rec], q *qx.QX) ([]uint64, error) {
	t.Helper()

	type row struct {
		id  uint64
		rec *Rec
	}

	var rows []row

	err := db.SeqScan(0, func(id uint64, v *Rec) (bool, error) {
		ok, e := evalExprBool(v, q.Filter)
		if e != nil {
			return false, e
		}
		if ok {
			rows = append(rows, row{id: id, rec: v})
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if len(q.Order) == 0 {
		sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	} else {
		prepared, viewQ, err := prepareTestQuery(db, q)
		if err != nil {
			return nil, err
		}
		defer prepared.Release()
		o := viewQ.Order
		f := testOrderFieldName(db, o)
		switch o.Kind {
		case qir.OrderKindBasic:
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)
				cmp := compareOrderedFieldValues(va, vb, o.Desc)
				if cmp == 0 {
					return a.id < b.id
				}
				return cmp < 0
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case qir.OrderKindArrayPos:
			want, _ := o.Data.([]string)
			if len(want) == 0 {
				sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
				break
			}
			priority := want
			if o.Desc {
				priority = append([]string(nil), want...)
				// reverse priority for desc
				for i, j := 0, len(priority)-1; i < j; i, j = i+1, j-1 {
					priority[i], priority[j] = priority[j], priority[i]
				}
			}
			rank := func(r row) int {
				raw := fieldValue(r.rec, f)
				set := make(map[string]struct{})
				switch v := raw.(type) {
				case []string:
					for _, s := range v {
						set[s] = struct{}{}
					}
				case string:
					set[v] = struct{}{}
				}
				for i, v := range priority {
					if _, ok := set[v]; ok {
						return i
					}
				}
				return len(priority)
			}
			sort.Slice(rows, func(i, j int) bool {
				ri := rank(rows[i])
				rj := rank(rows[j])
				if ri == rj {
					return rows[i].id < rows[j].id
				}
				return ri < rj
			})

		case qir.OrderKindArrayCount:
			sort.Slice(rows, func(i, j int) bool {
				ai, _ := fieldValue(rows[i].rec, f).([]string)
				aj, _ := fieldValue(rows[j].rec, f).([]string)
				ci := distinctCountStrings(ai)
				cj := distinctCountStrings(aj)
				if ci == cj {
					return rows[i].id < rows[j].id
				}
				if o.Desc {
					return ci > cj
				}
				return ci < cj
			})

		default:
			return nil, fmt.Errorf("test harness: unknown order kind %v", o.Kind)
		}
	}

	off := int(q.Window.Offset)
	if off > len(rows) {
		return nil, nil
	}
	rows = rows[off:]
	if q.Window.Limit != 0 && int(q.Window.Limit) < len(rows) {
		rows = rows[:int(q.Window.Limit)]
	}

	out := make([]uint64, len(rows))
	for i := range rows {
		out[i] = rows[i].id
	}
	return out, nil
}

// expectedKeysString scans the DB linearly and applies query semantics for
// string-key databases. Ordering/tie-breaks follow internal idx order.
func expectedKeysString(t testing.TB, db *DB[string, Rec], q *qx.QX) ([]string, error) {
	t.Helper()

	type row struct {
		id  string
		idx uint64
		rec *Rec
	}

	var rows []row
	err := db.SeqScan("", func(id string, v *Rec) (bool, error) {
		ok, e := evalExprBool(v, q.Filter)
		if e != nil {
			return false, e
		}
		if ok {
			rows = append(rows, row{
				id:  id,
				idx: db.idxFromID(id),
				rec: v,
			})
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if len(q.Order) == 0 {
		sort.Slice(rows, func(i, j int) bool { return rows[i].idx < rows[j].idx })
	} else {
		prepared, viewQ, err := prepareTestQuery(db, q)
		if err != nil {
			return nil, err
		}
		defer prepared.Release()
		o := viewQ.Order
		f := testOrderFieldName(db, o)
		switch o.Kind {
		case qir.OrderKindBasic:
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)
				cmp := compareOrderedFieldValues(va, vb, o.Desc)
				if cmp == 0 {
					return a.idx < b.idx
				}
				return cmp < 0
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case qir.OrderKindArrayPos:
			want, _ := o.Data.([]string)
			if len(want) == 0 {
				sort.Slice(rows, func(i, j int) bool { return rows[i].idx < rows[j].idx })
				break
			}
			priority := want
			if o.Desc {
				priority = append([]string(nil), want...)
				for i, j := 0, len(priority)-1; i < j; i, j = i+1, j-1 {
					priority[i], priority[j] = priority[j], priority[i]
				}
			}
			rank := func(r row) int {
				raw := fieldValue(r.rec, f)
				set := make(map[string]struct{})
				switch v := raw.(type) {
				case []string:
					for _, s := range v {
						set[s] = struct{}{}
					}
				case string:
					set[v] = struct{}{}
				}
				for i, v := range priority {
					if _, ok := set[v]; ok {
						return i
					}
				}
				return len(priority)
			}
			sort.Slice(rows, func(i, j int) bool {
				ri := rank(rows[i])
				rj := rank(rows[j])
				if ri == rj {
					return rows[i].idx < rows[j].idx
				}
				return ri < rj
			})

		case qir.OrderKindArrayCount:
			sort.Slice(rows, func(i, j int) bool {
				ai, _ := fieldValue(rows[i].rec, f).([]string)
				aj, _ := fieldValue(rows[j].rec, f).([]string)
				ci := distinctCountStrings(ai)
				cj := distinctCountStrings(aj)
				if ci == cj {
					return rows[i].idx < rows[j].idx
				}
				if o.Desc {
					return ci > cj
				}
				return ci < cj
			})

		default:
			return nil, fmt.Errorf("test harness: unknown order kind %v", o.Kind)
		}
	}

	off := int(q.Window.Offset)
	if off > len(rows) {
		return nil, nil
	}
	rows = rows[off:]
	if q.Window.Limit != 0 && int(q.Window.Limit) < len(rows) {
		rows = rows[:int(q.Window.Limit)]
	}

	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].id
	}
	return out, nil
}

func assertSameSlice(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got=%d want=%d\ngot=%v\nwant=%v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("mismatch at %d: got=%v want=%v\ngot=%v\nwant=%v", i, got[i], want[i], got, want)
		}
	}
}

func assertNoDuplicateStringIDs(t testing.TB, label string, ids []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("%s duplicate id in result: %q (result=%v)", label, id, ids)
		}
		seen[id] = struct{}{}
	}
}

func assertNoOrderWindowSubsetString(t testing.TB, q *qx.QX, got, full []string, label string) {
	t.Helper()
	assertNoDuplicateStringIDs(t, label, got)

	allow := make(map[string]struct{}, len(full))
	for _, id := range full {
		allow[id] = struct{}{}
	}
	for _, id := range got {
		if _, ok := allow[id]; !ok {
			t.Fatalf("%s: id=%q is outside full no-order result set", label, id)
		}
	}

	maxLen := len(full)
	if q.Window.Offset >= uint64(len(full)) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = len(full) - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	if len(got) > maxLen {
		t.Fatalf("%s: no-order window overflow got=%d max=%d", label, len(got), maxLen)
	}
}

func TestQuery_RouteEquivalence_StringKeys_Randomized(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}
	r := newRand(20260302)

	for i := 1; i <= 2_000; i++ {
		tagN := 1 + r.IntN(3)
		tags := make([]string, 0, tagN)
		for len(tags) < tagN {
			tags = append(tags, tagPool[r.IntN(len(tagPool))])
		}
		name := names[r.IntN(len(names))]
		rec := &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%04d@example.test", name, i),
			Age:      18 + r.IntN(60),
			Score:    float64(r.IntN(10_000))/100 + r.Float64()*0.001,
			Active:   r.IntN(2) == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}
		if i%11 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			rec.Opt = &s
		}
		if err := db.Set(fmt.Sprintf("id-%05d", i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}
	randomLeaf := func() qx.Expr {
		switch r.IntN(12) {
		case 0:
			return qx.EQ("active", r.IntN(2) == 0)
		case 1:
			return qx.IN("country", pickVals(countries, 2))
		case 2:
			return qx.NOTIN("country", pickVals(countries, 2))
		case 3:
			return qx.GTE("age", 18+r.IntN(35))
		case 4:
			return qx.LTE("age", 25+r.IntN(45))
		case 5:
			return qx.HASANY("tags", pickVals(tagPool, 2))
		case 6:
			return qx.HASALL("tags", pickVals(tagPool, 2))
		case 7:
			return qx.HASNONE("tags", pickVals(tagPool, 2))
		case 8:
			return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", 1+r.IntN(3)))
		case 9:
			return qx.SUFFIX("country", "land")
		case 10:
			return qx.CONTAINS("country", "land")
		default:
			return qx.GT("score", float64(20+r.IntN(70)))
		}
	}
	randomExpr := func() qx.Expr {
		if r.IntN(100) < 35 {
			return qx.OR(
				qx.AND(randomLeaf(), randomLeaf()),
				qx.AND(randomLeaf(), randomLeaf()),
			)
		}
		if r.IntN(100) < 60 {
			return qx.AND(randomLeaf(), randomLeaf(), randomLeaf())
		}
		return randomLeaf()
	}
	randomOrder := func() []qx.Order {
		switch r.IntN(5) {
		case 0, 1:
			return []qx.Order{testOrderBasic("score", r.IntN(2) == 0)}
		case 2:
			return []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
		case 3:
			return []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
		default:
			if r.IntN(100) < 50 {
				return []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
			}
			return []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
		}
	}

	for step := 0; step < 180; step++ {
		q := &qx.QX{Filter: randomExpr()}
		mode := r.IntN(100)
		switch {
		case mode < 45:
			q.Order = randomOrder()
			q.Window.Offset = uint64(r.IntN(120))
			q.Window.Limit = uint64(20 + r.IntN(120))
		case mode < 65:
			q.Window.Offset = uint64(r.IntN(180))
			q.Window.Limit = uint64(10 + r.IntN(120))
		case mode < 80:
			q.Order = randomOrder()
		}

		gotKeys, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("step=%d QueryKeys(%+v): %v", step, q, err)
		}
		wantPage, err := expectedKeysString(t, db, q)
		if err != nil {
			t.Fatalf("step=%d expectedKeysString(page %+v): %v", step, q, err)
		}
		if len(q.Order) == 0 && (q.Window.Offset > 0 || q.Window.Limit > 0) {
			fullQ := cloneQuery(q)
			fullQ.Window.Offset = 0
			fullQ.Window.Limit = 0
			wantFull, err := expectedKeysString(t, db, fullQ)
			if err != nil {
				t.Fatalf("step=%d expectedKeysString(full %+v): %v", step, q, err)
			}
			assertNoOrderWindowSubsetString(t, q, gotKeys, wantFull, fmt.Sprintf("step=%d QueryKeys", step))
		} else if !slices.Equal(gotKeys, wantPage) {
			t.Fatalf("step=%d page mismatch q=%+v\ngot=%v\nwant=%v", step, q, gotKeys, wantPage)
		}

		gotItems, err := db.Query(q)
		if err != nil {
			t.Fatalf("step=%d Query(%+v): %v", step, q, err)
		}
		wantItems, err := db.BatchGet(gotKeys...)
		if err != nil {
			t.Fatalf("step=%d BatchGet(got): %v", step, err)
		}
		if len(gotItems) != len(wantItems) {
			t.Fatalf("step=%d items len mismatch: got=%d want=%d q=%+v", step, len(gotItems), len(wantItems), q)
		}
		for i := range wantItems {
			if gotItems[i] == nil || wantItems[i] == nil {
				t.Fatalf("step=%d nil item mismatch at i=%d q=%+v got=%#v want=%#v", step, i, q, gotItems[i], wantItems[i])
			}
			if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
				t.Fatalf("step=%d item mismatch at i=%d q=%+v got=%#v want=%#v", step, i, q, gotItems[i], wantItems[i])
			}
		}

		countQ := cloneQuery(q)
		countQ.Order = nil
		countQ.Window.Offset = 0
		countQ.Window.Limit = 0
		wantAll, err := expectedKeysString(t, db, countQ)
		if err != nil {
			t.Fatalf("step=%d expectedKeysString(all %+v): %v", step, q, err)
		}
		cnt, err := db.Count(q.Filter)
		if err != nil {
			t.Fatalf("step=%d Count(%+v): %v", step, q, err)
		}
		if cnt != uint64(len(wantAll)) {
			t.Fatalf("step=%d count mismatch q=%+v got=%d want=%d", step, q, cnt, len(wantAll))
		}
	}
}

func sortedStringIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

func queryStringIDsEqual(q *qx.QX, a, b []string) bool {
	if q == nil {
		return slices.Equal(a, b)
	}
	if len(q.Order) > 0 {
		return slices.Equal(a, b)
	}
	return slices.Equal(sortedStringIDs(a), sortedStringIDs(b))
}

type preparedRouteEqUint64 interface {
	rootDB() *DB[uint64, Rec]
}

type preparedRouteEqString interface {
	rootDB() *DB[string, Rec]
}

func preparedRouteViewUint64(src preparedRouteEqUint64) *queryView[uint64, Rec] {
	switch v := any(src).(type) {
	case *DB[uint64, Rec]:
		return v.currentQueryViewForTests()
	case *queryView[uint64, Rec]:
		return v
	default:
		panic("unexpected uint64 prepared route source")
	}
}

func preparedRouteViewString(src preparedRouteEqString) *queryView[string, Rec] {
	switch v := any(src).(type) {
	case *DB[string, Rec]:
		return v.currentQueryViewForTests()
	case *queryView[string, Rec]:
		return v
	default:
		panic("unexpected string prepared route source")
	}
}

func assertPreparedRouteEquivalenceString(
	t testing.TB,
	db preparedRouteEqString,
	q *qx.QX,
) (nq *qx.QX, ref []string, usedExecution bool, usedPlanner bool) {
	t.Helper()

	view := preparedRouteViewString(db)
	nq = normalizeQueryForTest(q)
	prepared, viewQ, err := prepareTestQuery(db.rootDB(), nq)
	if err != nil {
		t.Fatalf("prepareQuery(%+v): %v", nq, err)
	}
	defer prepared.Release()

	if err := view.checkUsedQuery(&viewQ); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	ref, err = view.execPreparedQuery(&viewQ)
	if err != nil {
		t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	assertNoDuplicateStringIDs(t, "prepared", ref)

	strictEqual := len(nq.Order) > 0 || (nq.Window.Limit == 0 && nq.Window.Offset == 0)
	var noOrderFull []string
	if !strictEqual {
		fullQ := cloneQuery(nq)
		fullQ.Window.Offset = 0
		fullQ.Window.Limit = 0

		fullPrepared, fullViewQ, ferr := prepareTestQuery(db.rootDB(), fullQ)
		if ferr != nil {
			t.Fatalf("prepareQuery(full %+v): %v", fullQ, ferr)
		}
		noOrderFull, err = view.execPreparedQuery(&fullViewQ)
		fullPrepared.Release()
		if err != nil {
			t.Fatalf("execPreparedQuery(full %+v): %v", fullQ, err)
		}
		assertNoOrderWindowSubsetString(t, nq, ref, noOrderFull, "prepared")
	}

	execOut, ok, err := view.tryExecutionPlan(&viewQ, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateStringIDs(t, "execution", execOut)
		if strictEqual {
			if !queryStringIDsEqual(nq, ref, execOut) {
				want, werr := expectedKeysString(t, db.rootDB(), q)
				if werr != nil {
					t.Fatalf(
						"execution route mismatch:\nq=%+v\nnq=%+v\nref=%v\nexecution=%v\nseqscan_err=%v",
						q, nq, ref, execOut, werr,
					)
				}
				t.Fatalf(
					"execution route mismatch:\nq=%+v\nnq=%+v\nref=%v\nexecution=%v\nseqscan=%v",
					q, nq, ref, execOut, want,
				)
			}
		} else {
			assertNoOrderWindowSubsetString(t, nq, execOut, noOrderFull, "execution")
		}
		usedExecution = true
	}

	planOut, ok, err := view.tryPlan(&viewQ, nil)
	if err != nil {
		t.Fatalf("tryPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateStringIDs(t, "planner", planOut)
		if strictEqual {
			if !queryStringIDsEqual(nq, ref, planOut) {
				want, werr := expectedKeysString(t, db.rootDB(), q)
				if werr != nil {
					t.Fatalf(
						"planner route mismatch:\nq=%+v\nnq=%+v\nref=%v\nplanner=%v\nseqscan_err=%v",
						q, nq, ref, planOut, werr,
					)
				}
				t.Fatalf(
					"planner route mismatch:\nq=%+v\nnq=%+v\nref=%v\nplanner=%v\nseqscan=%v",
					q, nq, ref, planOut, want,
				)
			}
		} else {
			assertNoOrderWindowSubsetString(t, nq, planOut, noOrderFull, "planner")
		}
		usedPlanner = true
	}

	return nq, ref, usedExecution, usedPlanner
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_StringKeys(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})
	r := newRand(20260303)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}

	for i := 1; i <= 6_000; i++ {
		tagN := 1 + r.IntN(3)
		tags := make([]string, 0, tagN)
		for len(tags) < tagN {
			tags = append(tags, tagPool[r.IntN(len(tagPool))])
		}
		name := names[r.IntN(len(names))]
		rec := &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%05d@example.test", name, i),
			Age:      18 + r.IntN(60),
			Score:    float64(r.IntN(10_000))/100 + r.Float64()*0.001,
			Active:   r.IntN(2) == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}
		if err := db.Set(fmt.Sprintf("id-%05d", i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	queries := []*qx.QX{
		qx.Query(
			qx.EQ("active", true),
			qx.GTE("age", 22),
			qx.LT("age", 50),
		).Sort("age", qx.ASC).Limit(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).Sort("full_name", qx.ASC).Limit(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Limit(140),
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.HASANY("tags", []string{"go", "ops"}),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.GTE("age", 25),
				),
			),
		).Sort("score", qx.DESC).Offset(40).Limit(90),
		queryOrderSortByArrayCount(qx.Query(
			qx.GTE("age", 20),
		), "tags", qx.DESC).Offset(7).Limit(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Limit(85),
		qx.Query(
			qx.HASALL("tags", []string{"go", "db"}),
			qx.HASANY("tags", []string{"go", "ops"}),
		).Sort("score", qx.DESC).Offset(30).Limit(70),
	}

	var sawExec bool
	var sawPlan bool
	for i := range queries {
		q := queries[i]

		nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalenceString(t, db, q)
		sawExec = sawExec || usedExec
		sawPlan = sawPlan || usedPlan

		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("q%d QueryKeys: %v", i, err)
		}
		want, err := expectedKeysString(t, db, q)
		if err != nil {
			t.Fatalf("q%d expectedKeysString: %v", i, err)
		}
		if len(q.Order) == 0 && (q.Window.Limit > 0 || q.Window.Offset > 0) {
			fullQ := cloneQuery(q)
			fullQ.Window.Offset = 0
			fullQ.Window.Limit = 0
			fullWant, err := expectedKeysString(t, db, fullQ)
			if err != nil {
				t.Fatalf("q%d expectedKeysString(full): %v", i, err)
			}
			assertNoOrderWindowSubsetString(t, q, got, fullWant, fmt.Sprintf("q%d QueryKeys", i))
			assertNoOrderWindowSubsetString(t, nq, ref, fullWant, fmt.Sprintf("q%d prepared", i))
		} else {
			if !queryStringIDsEqual(q, got, want) {
				t.Fatalf("q%d QueryKeys mismatch:\n got=%v\nwant=%v", i, got, want)
			}
			if !queryStringIDsEqual(nq, ref, want) {
				t.Fatalf("q%d prepared mismatch:\n got=%v\nwant=%v", i, ref, want)
			}
		}
	}

	if !sawExec {
		t.Fatalf("expected at least one execution fast-path route")
	}
	if !sawPlan {
		t.Fatalf("expected at least one planner fast-path route")
	}
}

func TestQueryCorrectnessAgainstSeqScan_Uint64Keys(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 80)

	queries := []*qx.QX{
		qx.Query(qx.EQ("name", "alice")),
		qx.Query(qx.PREFIX("name", "al")),
		qx.Query(qx.SUFFIX("country", "land")),
		qx.Query(qx.CONTAINS("country", "land")),

		qx.Query(qx.GT("age", 30)),
		qx.Query(qx.GTE("age", 30)),
		qx.Query(qx.LT("age", 25)),
		qx.Query(qx.LTE("age", 25)),
		qx.Query(qx.IN("age", []int{18, 19, 20})),

		qx.Query(qx.HASALL("tags", []string{"go", "db"})),
		qx.Query(qx.HASANY("tags", []string{"go", "java"})),
		qx.Query(qx.HASNONE("tags", []string{"rust"})),

		qx.Query(
			qx.OR(
				qx.AND(
					qx.GT("age", 30),
					qx.EQ("active", true),
				),
				qx.PREFIX("name", "al"),
			),
		),

		qx.Query(qx.NOT(qx.CONTAINS("country", "land"))),
	}

	queries = append(queries,
		qx.Query(qx.GT("age", 20)).Sort("age", qx.ASC).Offset(5).Limit(10),
		queryOrderSortByArrayCount(qx.Query(), "tags", qx.DESC),
		queryOrderSortByArrayPos(qx.Query(), "tags", []string{"go", "java"}, qx.ASC),
	)

	for i, q := range queries {
		q := q
		t.Run(fmt.Sprintf("q%d_%v", i, plannerExtExprOp(q.Filter)), func(t *testing.T) {
			gotKeys, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}

			wantPageKeys, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("expectedKeysUint64(page): %v", err)
			}
			assertSameSlice(t, gotKeys, wantPageKeys)

			gotItems, err := db.Query(q)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(gotItems) != len(wantPageKeys) {
				t.Fatalf("Query len mismatch: got=%d want=%d", len(gotItems), len(wantPageKeys))
			}

			qNoPage := *q
			qNoPage.Window.Offset = 0
			qNoPage.Window.Limit = 0

			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("expectedKeysUint64(all): %v", err)
			}

			cnt, err := db.Count(q.Filter)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if cnt != uint64(len(wantAllKeys)) {
				t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(wantAllKeys))
			}
		})
	}
}

func TestQuerySetEquivalence_StringKeys(t *testing.T) {
	db, path := openTempDBString(t)

	for i := 1; i <= 20; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Age:      i,
			Score:    float64(i),
			Active:   i%2 == 0,
			Tags:     []string{"go"},
			FullName: "X",
		}
		if err := db.Set(id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(qx.GT("age", 10))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	wantSet := map[string]struct{}{}
	err = db.SeqScan("", func(id string, v *Rec) (bool, error) {
		ok, e := evalExprBool(v, q.Filter)
		if e != nil {
			return false, e
		}
		if ok {
			wantSet[id] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	gotSet := map[string]struct{}{}
	for _, id := range got {
		gotSet[id] = struct{}{}
	}

	if len(gotSet) != len(wantSet) {
		t.Fatalf("set size mismatch: got=%d want=%d (db=%s)", len(gotSet), len(wantSet), path)
	}
	for k := range wantSet {
		if _, ok := gotSet[k]; !ok {
			t.Fatalf("missing id %q in result set", k)
		}
	}
}

func TestScanKeysUint64_SeekOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 5; i++ {
		r := &Rec{Name: fmt.Sprintf("n%d", i), Age: i}
		if err := db.Set(uint64(i), r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []uint64
	err := db.ScanKeys(3, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := []uint64{3, 4, 5}
	if !slices.Equal(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestScanKeys_String_SeekLowerBound(t *testing.T) {
	db, _ := openTempDBString(t)

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{Name: "x", Age: i}
		if err := db.Set(id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	seek := "id-03"
	got := make(map[string]struct{})
	err := db.ScanKeys(seek, func(id string) (bool, error) {
		if id < seek {
			t.Fatalf("unexpected id below seek: %v", id)
		}
		got[id] = struct{}{}
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := map[string]struct{}{
		"id-03": {},
		"id-04": {},
		"id-05": {},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d: %v", len(want), len(got), got)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("missing key: %v (got=%v)", id, got)
		}
	}
}

func TestSort_OrderStability_WithDupValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 10; i++ {
		if err := db.Set(uint64(i), &Rec{Age: 20}); err != nil {
			t.Fatal(err)
		}
	}

	q := qx.Query(qx.EQ("age", 20)).Sort("age", qx.ASC)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(ids)-1; i++ {
		if ids[i] > ids[i+1] {
			t.Errorf("unstable sort for dup values at index %d: %d > %d", i, ids[i], ids[i+1])
		}
	}
}

func TestQuery_NegativeNoOrder_ExcludesCorrectly_WithPaging(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	q := qx.Query(
		qx.NOT(qx.EQ("name", "alice")),
	).Offset(10).Limit(40)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_OrderVariants_AcrossSnapshots_MatchSeqScanModel(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 320)

	// Force a clean snapshot before mutations to exercise the initial order paths.
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(base): %v", err)
	}

	runChecks := func(label string, queries []*qx.QX) {
		t.Helper()
		for i, q := range queries {
			q := q
			t.Run(fmt.Sprintf("%s_q%d_%s", label, i, queryTestOrderLabel(q)), func(t *testing.T) {
				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				want, err := expectedKeysUint64(t, db, q)
				if err != nil {
					t.Fatalf("expectedKeysUint64: %v", err)
				}
				assertSameSlice(t, got, want)
			})
		}
	}

	baseQueries := []*qx.QX{
		// Base basic-order fast path (skip=0, limit<=256) and non-fast variant.
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.ASC).Limit(64),
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC).Offset(7).Limit(41),

		// Base array order over slice field.
		queryOrderSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", []string{"go", "java", "ops"}, qx.ASC).Offset(3).Limit(33),
		queryOrderSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", true))), "tags", qx.DESC).Offset(2).Limit(37),

		// ArrayPos over scalar field to cover scalar ordering variants.
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.ASC).Limit(60),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("base", baseQueries)

	// Mutate basic/slice/scalar order fields and verify subsequent snapshots.
	if err := db.Patch(1, []Field{{Name: "age", Value: 99}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "tags", Value: []string{"ops", "go"}}}); err != nil {
		t.Fatalf("Patch(tags): %v", err)
	}
	if err := db.Patch(3, []Field{{Name: "country", Value: "NL"}}); err != nil {
		t.Fatalf("Patch(country): %v", err)
	}
	if err := db.Set(1001, &Rec{
		Meta:     Meta{Country: "DE"},
		Name:     "mutated-user",
		Email:    "mutated-user@example.com",
		Age:      37,
		Score:    77.7,
		Active:   true,
		Tags:     []string{"java", "ops"},
		FullName: "mutated-user",
	}); err != nil {
		t.Fatalf("Set(mutated): %v", err)
	}
	if err := db.Delete(4); err != nil {
		t.Fatalf("Delete(mutated): %v", err)
	}

	mutatedQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.ASC).Limit(64),
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC).Offset(7).Limit(41),
		queryOrderSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", []string{"go", "java", "ops"}, qx.DESC).Offset(3).Limit(33),
		queryOrderSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", true))), "tags", qx.ASC).Offset(2).Limit(37),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.ASC).Limit(60),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("mutated", mutatedQueries)
}

func TestQuery_ArrayOrder_RandomMutations_MatchSeqScan(t *testing.T) {
	cases := []struct {
		name  string
		seed  int64
		steps int
	}{
		{name: "seed_20260224", seed: 20260224, steps: 160},
		{name: "seed_20260301", seed: 20260301, steps: 120},
		{name: "seed_20260317", seed: 20260317, steps: 120},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t)
			_ = seedData(t, db, 420)

			r := newRand(tc.seed)
			countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US", "JP"}
			tagPool := []string{"go", "db", "ops", "rust", "java", "infra", "ml"}
			names := []string{"alice", "bob", "carol", "dave", "eve", "nik"}

			randomTags := func() []string {
				n := 1 + r.IntN(4)
				out := make([]string, 0, n)
				for i := 0; i < n; i++ {
					out = append(out, tagPool[r.IntN(len(tagPool))])
				}
				return out
			}
			randomRec := func(id uint64) *Rec {
				name := names[r.IntN(len(names))]
				return &Rec{
					Meta:     Meta{Country: countries[r.IntN(len(countries))]},
					Name:     name,
					Email:    fmt.Sprintf("%s-%d@example.test", name, id),
					Age:      18 + r.IntN(60),
					Score:    float64(r.IntN(1000))/10.0 + r.Float64()*0.001,
					Active:   r.IntN(2) == 0,
					Tags:     randomTags(),
					FullName: fmt.Sprintf("FN-%05d", id),
				}
			}
			pickVals := func(src []string, n int) []string {
				out := make([]string, 0, n)
				for i := 0; i < n; i++ {
					out = append(out, src[r.IntN(len(src))])
				}
				return out
			}

			randomLeaf := func() qx.Expr {
				switch r.IntN(8) {
				case 0:
					return qx.EQ("active", r.IntN(2) == 0)
				case 1:
					return qx.IN("country", pickVals(countries, 2))
				case 2:
					return qx.NOTIN("country", pickVals(countries, 2))
				case 3:
					return qx.GTE("age", 18+r.IntN(35))
				case 4:
					return qx.LTE("age", 25+r.IntN(40))
				case 5:
					return qx.HASANY("tags", pickVals(tagPool, 2))
				case 6:
					return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", r.IntN(3)))
				default:
					return qx.GT("score", float64(r.IntN(80)))
				}
			}
			randomExpr := func() qx.Expr {
				a := randomLeaf()
				if r.IntN(100) < 45 {
					b := randomLeaf()
					if r.IntN(2) == 0 {
						return qx.AND(a, b)
					}
					return qx.OR(a, b)
				}
				return a
			}
			randomQuery := func() *qx.QX {
				q := &qx.QX{Filter: randomExpr()}
				switch r.IntN(4) {
				case 0:
					q.Order = []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
				case 1:
					q.Order = []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
				case 2:
					q.Order = []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
				default:
					q.Order = []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
				}

				switch r.IntN(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
					q.Window.Offset = 0
					q.Window.Limit = 0
				case 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
					q.Window.Offset = uint64(350 + r.IntN(300))
					q.Window.Limit = uint64(25 + r.IntN(60))
				default:
					q.Window.Offset = uint64(r.IntN(45))
					q.Window.Limit = uint64(1 + r.IntN(120))
				}
				return q
			}
			randomPatch := func() []Field {
				switch r.IntN(5) {
				case 0:
					return []Field{{Name: "age", Value: 18 + r.IntN(60)}}
				case 1:
					return []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
				case 2:
					return []Field{{Name: "tags", Value: randomTags()}}
				case 3:
					return []Field{{Name: "active", Value: r.IntN(2) == 0}}
				default:
					return []Field{{Name: "score", Value: float64(r.IntN(1000))/10.0 + r.Float64()*0.001}}
				}
			}

			var opsLog []string

			for step := 0; step < tc.steps; step++ {
				if step > 0 && step%27 == 0 {
					if err := db.RebuildIndex(); err != nil {
						t.Fatalf("RebuildIndex(seed=%d,step=%d): %v", tc.seed, step, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d rebuild", step))
				}

				id := uint64(1 + r.IntN(560))
				switch x := r.IntN(100); {
				case x < 45:
					rec := randomRec(id)
					if err := db.Set(id, rec); err != nil {
						t.Fatalf("Set(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d set id=%d tags=%v country=%s age=%d active=%v", step, id, rec.Tags, rec.Country, rec.Age, rec.Active))
				case x < 80:
					patch := randomPatch()
					if err := db.Patch(id, patch); err != nil {
						t.Fatalf("Patch(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d patch id=%d patch=%v", step, id, patch))
				default:
					if err := db.Delete(id); err != nil {
						t.Fatalf("Delete(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d delete id=%d", step, id))
				}

				for qi := 0; qi < 3; qi++ {
					q := randomQuery()
					got, err := db.QueryKeys(q)
					if err != nil {
						t.Fatalf("QueryKeys(seed=%d,step=%d,qi=%d): %v\nq=%+v", tc.seed, step, qi, err, q)
					}
					want, err := expectedKeysUint64(t, db, q)
					if err != nil {
						t.Fatalf("expectedKeysUint64(seed=%d,step=%d,qi=%d): %v\nq=%+v", tc.seed, step, qi, err, q)
					}
					if !slices.Equal(got, want) {
						first := -1
						maxCmp := len(got)
						if len(want) < maxCmp {
							maxCmp = len(want)
						}
						for i := 0; i < maxCmp; i++ {
							if got[i] != want[i] {
								first = i
								break
							}
						}
						if first == -1 && len(got) != len(want) {
							first = maxCmp
						}

						extra := ""
						if first >= 0 && first < len(got) && first < len(want) && len(q.Order) > 0 {
							if queryTestOrderIsArrayPosOnField(q, "tags") {
								gid := got[first]
								wid := want[first]
								gv, _ := db.Get(gid)
								wv, _ := db.Get(wid)
								vals := queryTestOrderValues(q)
								var bits []string
								for _, tag := range vals {
									ids := db.fieldLookupPostingRetained("tags", tag)
									gHas := ids.Contains(gid)
									wHas := ids.Contains(wid)
									bits = append(bits, fmt.Sprintf("%s:g=%v,w=%v", tag, gHas, wHas))
								}
								extra = fmt.Sprintf(
									"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v\nwantRec=%#v\ntagMembership={%s}",
									first,
									gid,
									wid,
									gv,
									wv,
									strings.Join(bits, ", "),
								)
							} else if queryTestOrderIsArrayPosOnField(q, "country") {
								gid := got[first]
								wid := want[first]
								gv, _ := db.Get(gid)
								wv, _ := db.Get(wid)
								vals := queryTestOrderValues(q)
								var bits []string
								for _, country := range vals {
									ids := db.fieldLookupPostingRetained("country", country)
									gHas := ids.Contains(gid)
									wHas := ids.Contains(wid)
									bits = append(bits, fmt.Sprintf("%s:g=%v,w=%v", country, gHas, wHas))
								}
								extra = fmt.Sprintf(
									"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v\nwantRec=%#v\ncountryMembership={%s}",
									first,
									gid,
									wid,
									gv,
									wv,
									strings.Join(bits, ", "),
								)
							}
						}
						t.Fatalf(
							"query mismatch seed=%d step=%d qi=%d q=%+v\ngot=%v\nwant=%v%s\nops:\n%s",
							tc.seed,
							step,
							qi,
							q,
							got,
							want,
							extra,
							strings.Join(opsLog, "\n"),
						)
					}
				}
			}
		})
	}
}

func TestQuery_RandomMixedMultiWrites_MatchSeqScanModel(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 240)

	r := newRand(20260326)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra", "ml"}
	names := []string{"alice", "bob", "carol", "dave", "eve", "nik"}

	randomTags := func() []string {
		n := 1 + r.IntN(4)
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, tagPool[r.IntN(len(tagPool))])
		}
		return out
	}
	randomRec := func(id uint64) *Rec {
		name := names[r.IntN(len(names))]
		return &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%d-%d@example.test", name, id, r.IntN(10_000)),
			Age:      18 + r.IntN(62),
			Score:    float64(r.IntN(1000))/10.0 + r.Float64()*0.0001,
			Active:   r.IntN(2) == 0,
			Tags:     randomTags(),
			FullName: fmt.Sprintf("FN-%05d", id),
		}
	}
	randomPatch := func() []Field {
		switch r.IntN(6) {
		case 0:
			return []Field{{Name: "age", Value: 18 + r.IntN(62)}}
		case 1:
			return []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
		case 2:
			return []Field{{Name: "tags", Value: randomTags()}}
		case 3:
			return []Field{{Name: "active", Value: r.IntN(2) == 0}}
		case 4:
			return []Field{{Name: "score", Value: float64(r.IntN(1000)) / 10.0}}
		default:
			return []Field{{Name: "name", Value: names[r.IntN(len(names))]}}
		}
	}
	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}

	randomLeaf := func() qx.Expr {
		switch r.IntN(8) {
		case 0:
			return qx.EQ("active", r.IntN(2) == 0)
		case 1:
			return qx.IN("country", pickVals(countries, 2))
		case 2:
			return qx.NOTIN("country", pickVals(countries, 2))
		case 3:
			return qx.GTE("age", 18+r.IntN(40))
		case 4:
			return qx.LTE("age", 20+r.IntN(45))
		case 5:
			return qx.HASANY("tags", pickVals(tagPool, 2))
		case 6:
			return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", r.IntN(3)))
		default:
			return qx.GT("score", float64(r.IntN(85)))
		}
	}
	randomExpr := func() qx.Expr {
		a := randomLeaf()
		if r.IntN(100) < 45 {
			b := randomLeaf()
			if r.IntN(2) == 0 {
				return qx.AND(a, b)
			}
			return qx.OR(a, b)
		}
		return a
	}
	randomQuery := func() *qx.QX {
		q := &qx.QX{Filter: randomExpr()}
		switch r.IntN(4) {
		case 0:
			q.Order = []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
		case 1:
			q.Order = []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
		case 2:
			q.Order = []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
		default:
			q.Order = []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
		}
		q.Window.Offset = uint64(r.IntN(40))
		if r.IntN(100) < 10 {
			q.Window.Limit = 0
		} else {
			q.Window.Limit = uint64(1 + r.IntN(120))
		}
		return q
	}

	opsLog := make([]string, 0, 256)

	for step := 0; step < 180; step++ {
		id := uint64(1 + r.IntN(360))
		switch x := r.IntN(100); {
		case x < 22:
			rec := randomRec(id)
			if err := db.Set(id, rec); err != nil {
				t.Fatalf("step=%d Set(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d set id=%d tags=%v country=%s age=%d active=%v", step, id, rec.Tags, rec.Country, rec.Age, rec.Active))
		case x < 44:
			n := 2 + r.IntN(5)
			ids := make([]uint64, n)
			vals := make([]*Rec, n)
			pairs := make([]string, n)
			for i := 0; i < n; i++ {
				cid := uint64(1 + r.IntN(360))
				ids[i] = cid
				vals[i] = randomRec(cid)
				pairs[i] = fmt.Sprintf("%d:%v", cid, vals[i].Tags)
			}
			if err := db.BatchSet(ids, vals); err != nil {
				t.Fatalf("step=%d BatchSet(ids=%v): %v", step, ids, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d set_many %s", step, strings.Join(pairs, " | ")))
		case x < 58:
			patch := randomPatch()
			if err := db.Patch(id, patch); err != nil {
				t.Fatalf("step=%d Patch(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d patch id=%d patch=%v", step, id, patch))
		case x < 74:
			n := 2 + r.IntN(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(360))
			}
			patch := randomPatch()
			if err := db.BatchPatch(ids, patch); err != nil {
				t.Fatalf("step=%d BatchPatch(ids=%v,patch=%v): %v", step, ids, patch, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d patch_many ids=%v patch=%v", step, ids, patch))
		case x < 88:
			if err := db.Delete(id); err != nil {
				t.Fatalf("step=%d Delete(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d delete id=%d", step, id))
		default:
			n := 1 + r.IntN(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(360))
			}
			if err := db.BatchDelete(ids); err != nil {
				t.Fatalf("step=%d BatchDelete(ids=%v): %v", step, ids, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d delete_many ids=%v", step, ids))
		}

		for qi := 0; qi < 3; qi++ {
			q := randomQuery()

			gotKeys, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("step=%d qi=%d QueryKeys: %v q=%+v", step, qi, err, q)
			}
			wantKeys, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("step=%d qi=%d expectedKeys: %v q=%+v", step, qi, err, q)
			}
			if !slices.Equal(gotKeys, wantKeys) {
				first := -1
				maxCmp := len(gotKeys)
				if len(wantKeys) < maxCmp {
					maxCmp = len(wantKeys)
				}
				for i := 0; i < maxCmp; i++ {
					if gotKeys[i] != wantKeys[i] {
						first = i
						break
					}
				}
				if first == -1 && len(gotKeys) != len(wantKeys) {
					first = maxCmp
				}

				extra := ""
				if first >= 0 && first < len(gotKeys) && first < len(wantKeys) {
					gid := gotKeys[first]
					wid := wantKeys[first]
					gv, _ := db.Get(gid)
					wv, _ := db.Get(wid)
					if queryTestOrderIsArrayPosOnField(q, "country") {
						vals := queryTestOrderValues(q)
						var gm, wm []string
						for _, country := range vals {
							ids := db.fieldLookupPostingRetained("country", country)
							gHas := ids.Contains(gid)
							wHas := ids.Contains(wid)
							gm = append(gm, fmt.Sprintf("%s=%v", country, gHas))
							wm = append(wm, fmt.Sprintf("%s=%v", country, wHas))
						}
						countryDebug := func(country string) string {
							qSimple := qx.Query(qx.AND(
								qx.EQ("active", true),
								qx.EQ("country", country),
							))
							gotIDs, gerr := db.QueryKeys(qSimple)
							wantIDs, werr := expectedKeysUint64(t, db, qSimple)
							if gerr != nil || werr != nil {
								return fmt.Sprintf("%s: gotErr=%v wantErr=%v", country, gerr, werr)
							}
							if len(gotIDs) > 8 {
								gotIDs = gotIDs[:8]
							}
							if len(wantIDs) > 8 {
								wantIDs = wantIDs[:8]
							}
							return fmt.Sprintf("%s: got=%v want=%v", country, gotIDs, wantIDs)
						}
						extra = fmt.Sprintf(
							"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v countryMembership={%s}\nwantRec=%#v countryMembership={%s}\ncountryDebug:\n%s\n%s",
							first,
							gid,
							wid,
							gv,
							strings.Join(gm, ","),
							wv,
							strings.Join(wm, ","),
							countryDebug("NL"),
							countryDebug("Finland"),
						)
					} else {
						var gl, wl int
						if gv != nil {
							gl = distinctCountStrings(gv.Tags)
						}
						if wv != nil {
							wl = distinctCountStrings(wv.Tags)
						}
						var gm, wm []string
						for _, tag := range []string{"rust", "java", "infra", "go", "db"} {
							ids := db.fieldLookupPostingRetained("tags", tag)
							gHas := ids.Contains(gid)
							wHas := ids.Contains(wid)
							gm = append(gm, fmt.Sprintf("%s=%v", tag, gHas))
							wm = append(wm, fmt.Sprintf("%s=%v", tag, wHas))
						}
						extra = fmt.Sprintf(
							"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v gotDistinctTags=%d tagMembership={%s}\nwantRec=%#v wantDistinctTags=%d tagMembership={%s}",
							first, gid, wid, gv, gl, strings.Join(gm, ","), wv, wl, strings.Join(wm, ","),
						)
					}
				}
				t.Fatalf("step=%d qi=%d keys mismatch q=%+v got=%v want=%v%s\nops:\n%s", step, qi, q, gotKeys, wantKeys, extra, strings.Join(opsLog, "\n"))
			}

			gotItems, err := db.Query(q)
			if err != nil {
				t.Fatalf("step=%d qi=%d Query: %v q=%+v", step, qi, err, q)
			}
			wantItems, err := db.BatchGet(wantKeys...)
			if err != nil {
				t.Fatalf("step=%d qi=%d BatchGet(wantKeys): %v", step, qi, err)
			}
			if len(gotItems) != len(wantItems) {
				t.Fatalf("step=%d qi=%d items len mismatch q=%+v got=%d want=%d", step, qi, q, len(gotItems), len(wantItems))
			}
			for i := range wantItems {
				if gotItems[i] == nil || wantItems[i] == nil {
					t.Fatalf("step=%d qi=%d nil item at i=%d q=%+v got=%#v want=%#v", step, qi, i, q, gotItems[i], wantItems[i])
				}
				if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
					t.Fatalf("step=%d qi=%d item mismatch at i=%d q=%+v got=%#v want=%#v", step, qi, i, q, gotItems[i], wantItems[i])
				}
			}

			qNoPage := *q
			qNoPage.Window.Offset = 0
			qNoPage.Window.Limit = 0
			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("step=%d qi=%d expectedKeys(all): %v q=%+v", step, qi, err, q)
			}
			cnt, err := db.Count(q.Filter)
			if err != nil {
				t.Fatalf("step=%d qi=%d Count: %v q=%+v", step, qi, err, q)
			}
			if cnt != uint64(len(wantAllKeys)) {
				t.Fatalf("step=%d qi=%d count mismatch q=%+v got=%d want=%d", step, qi, q, cnt, len(wantAllKeys))
			}
		}
	}
}

func TestQuery_ByArrayPos_Scalar_DuplicatePriority_BaseAndOverlay(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 260)

	check := func(label string, q *qx.QX) {
		t.Helper()
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("%s QueryKeys: %v", label, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("%s expectedKeysUint64: %v", label, err)
		}
		assertSameSlice(t, got, want)
		seen := make(map[uint64]struct{}, len(got))
		for _, id := range got {
			if _, ok := seen[id]; ok {
				t.Fatalf("%s duplicate id in result: %d (got=%v)", label, id, got)
			}
			seen[id] = struct{}{}
		}
	}

	baseQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.ASC).Offset(3).Limit(120)
	check("base", baseQ)
	baseDescQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.DESC).Offset(3).Limit(120)
	check("base-desc", baseDescQ)

	if err := db.Patch(5, []Field{{Name: "country", Value: "NL"}}); err != nil {
		t.Fatalf("Patch(5 country): %v", err)
	}
	if err := db.Patch(6, []Field{{Name: "country", Value: "PL"}}); err != nil {
		t.Fatalf("Patch(6 country): %v", err)
	}
	if err := db.Set(1001, &Rec{
		Meta:     Meta{Country: "NL"},
		Name:     "dup-priority",
		Email:    "dup-priority@example.test",
		Age:      39,
		Score:    77.7,
		Active:   false,
		Tags:     []string{"go"},
		FullName: "FN-1001",
	}); err != nil {
		t.Fatalf("Set(1001): %v", err)
	}

	overlayQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.ASC).Offset(3).Limit(120)
	check("overlay", overlayQ)
	overlayDescQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.DESC).Offset(3).Limit(120)
	check("overlay-desc", overlayDescQ)
}

func runQueryKeysChecked(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	return ids
}

func assertQueryIDsEqual(t *testing.T, q *qx.QX, a, b []uint64) {
	t.Helper()
	if queryIDsEqual(q, a, b) {
		return
	}
	if len(q.Order) > 0 {
		t.Fatalf("ordered results mismatch:\nA=%v\nB=%v", a, b)
		return
	}
	sa := sortedIDs(a)
	sb := sortedIDs(b)
	t.Fatalf("unordered set mismatch:\nA=%v\nB=%v", sa, sb)
}

func queryIDsEqual(q *qx.QX, a, b []uint64) bool {
	if q == nil {
		return slices.Equal(a, b)
	}
	if len(q.Order) > 0 {
		return slices.Equal(a, b)
	}

	sa := sortedIDs(a)
	sb := sortedIDs(b)
	return slices.Equal(sa, sb)
}

func assertNoDuplicateIDs(t testing.TB, label string, ids []uint64) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("%s duplicate id in result: %d (result=%v)", label, id, ids)
		}
		seen[id] = struct{}{}
	}
}

func assertNoOrderWindowSubset(t testing.TB, q *qx.QX, got, full []uint64, label string) {
	t.Helper()
	assertNoDuplicateIDs(t, label, got)

	allow := make(map[uint64]struct{}, len(full))
	for _, id := range full {
		allow[id] = struct{}{}
	}
	for _, id := range got {
		if _, ok := allow[id]; !ok {
			t.Fatalf("%s: id=%d is outside full no-order result set", label, id)
		}
	}

	maxLen := len(full)
	if q.Window.Offset >= uint64(len(full)) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = len(full) - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	if len(got) > maxLen {
		t.Fatalf("%s: no-order window overflow got=%d max=%d", label, len(got), maxLen)
	}
}

func assertPreparedRouteEquivalence(t testing.TB, db preparedRouteEqUint64, q *qx.QX) (nq *qx.QX, ref []uint64, usedExecution bool, usedPlanner bool) {
	t.Helper()

	view := preparedRouteViewUint64(db)
	nq = normalizeQueryForTest(q)
	prepared, viewQ, err := prepareTestQuery(db.rootDB(), nq)
	if err != nil {
		t.Fatalf("prepareQuery(%+v): %v", nq, err)
	}
	defer prepared.Release()

	if err := view.checkUsedQuery(&viewQ); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	ref, err = view.execPreparedQuery(&viewQ)
	if err != nil {
		t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	assertNoDuplicateIDs(t, "prepared", ref)

	strictEqual := len(nq.Order) > 0 || (nq.Window.Limit == 0 && nq.Window.Offset == 0)
	var noOrderFull []uint64
	if !strictEqual {
		fullQ := cloneQuery(nq)
		fullQ.Window.Offset = 0
		fullQ.Window.Limit = 0

		fullPrepared, fullViewQ, ferr := prepareTestQuery(db.rootDB(), fullQ)
		if ferr != nil {
			t.Fatalf("prepareQuery(full %+v): %v", fullQ, ferr)
		}
		noOrderFull, err = view.execPreparedQuery(&fullViewQ)
		fullPrepared.Release()
		if err != nil {
			t.Fatalf("execPreparedQuery(full %+v): %v", fullQ, err)
		}
		assertNoOrderWindowSubset(t, nq, ref, noOrderFull, "prepared")
	}

	execOut, ok, err := view.tryExecutionPlan(&viewQ, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateIDs(t, "execution", execOut)
		if strictEqual {
			if !queryIDsEqual(nq, ref, execOut) {
				if tt, ok := t.(*testing.T); ok {
					want, werr := expectedKeysUint64(tt, db.rootDB(), q)
					if werr != nil {
						t.Fatalf("execution route mismatch:\nq=%+v\nnq=%+v\nref=%v\nexecution=%v\nseqscan_err=%v", q, nq, ref, execOut, werr)
					}
					t.Fatalf("execution route mismatch:\nq=%+v\nnq=%+v\nref=%v\nexecution=%v\nseqscan=%v", q, nq, ref, execOut, want)
				}
				t.Fatalf("execution route mismatch:\nq=%+v\nnq=%+v\nref=%v\nexecution=%v", q, nq, ref, execOut)
			}
		} else {
			assertNoOrderWindowSubset(t, nq, execOut, noOrderFull, "execution")
		}
		usedExecution = true
	}

	planOut, ok, err := view.tryPlan(&viewQ, nil)
	if err != nil {
		t.Fatalf("tryPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateIDs(t, "planner", planOut)
		if strictEqual {
			if !queryIDsEqual(nq, ref, planOut) {
				if tt, ok := t.(*testing.T); ok {
					want, werr := expectedKeysUint64(tt, db.rootDB(), q)
					if werr != nil {
						t.Fatalf("planner route mismatch:\nq=%+v\nnq=%+v\nref=%v\nplanner=%v\nseqscan_err=%v", q, nq, ref, planOut, werr)
					}
					t.Fatalf("planner route mismatch:\nq=%+v\nnq=%+v\nref=%v\nplanner=%v\nseqscan=%v", q, nq, ref, planOut, want)
				}
				t.Fatalf("planner route mismatch:\nq=%+v\nnq=%+v\nref=%v\nplanner=%v", q, nq, ref, planOut)
			}
		} else {
			assertNoOrderWindowSubset(t, nq, planOut, noOrderFull, "planner")
		}
		usedPlanner = true
	}

	return nq, ref, usedExecution, usedPlanner
}

func cloneQuery(q *qx.QX) *qx.QX {
	return qx.Clone(q)
}

func normalizeQueryForTest(q *qx.QX) *qx.QX {
	return qx.Normalize(cloneQuery(q))
}

func wrapExprWithNoise(e qx.Expr, mode int) qx.Expr {
	switch mode % 4 {
	case 0:
		// e AND true
		return qx.AND(e, qx.Expr{})
	case 1:
		// e OR false
		return qx.OR(e, qx.NOT(qx.Expr{}))
	case 2:
		// (true AND e) AND true
		return qx.AND(qx.AND(qx.Expr{}, e), qx.Expr{})
	default:
		// e OR (false OR false)
		return qx.OR(e, qx.OR(qx.NOT(qx.Expr{}), qx.NOT(qx.Expr{})))
	}
}

func withNoisyEquivalentQuery(q *qx.QX, noiseMode int) *qx.QX {
	out := cloneQuery(q)
	out.Filter = wrapExprWithNoise(q.Filter, noiseMode)
	return out
}

func TestQuery_Metamorphic_NormalizeAndNoiseEquivalence(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 20_000)

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "OR_Order_Offset",
			q: qx.Query(
				qx.OR(
					qx.AND(
						qx.EQ("active", true),
						qx.IN("country", []string{"NL", "DE", "PL"}),
						qx.GTE("score", 30.0),
					),
					qx.AND(
						qx.PREFIX("full_name", "FN-1"),
						qx.NOTIN("country", []string{"Thailand"}),
						qx.GTE("age", 20),
					),
					qx.AND(
						qx.HASANY("tags", []string{"go", "db"}),
						qx.NE("name", "alice"),
					),
				),
			).Sort("score", qx.DESC).Offset(300).Limit(120),
		},
		{
			name: "AND_NoOrder_Complex",
			q: qx.Query(
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL", "PL"}),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("age", 22),
			),
		},
		{
			name: "AutocompleteLike",
			q: qx.Query(
				qx.PREFIX("full_name", "FN-1"),
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL"}),
			).Sort("score", qx.DESC).Limit(80),
		},
		{
			name: "OrderRange",
			q: qx.Query(
				qx.GTE("age", 25),
				qx.LTE("age", 40),
				qx.GT("score", 20.0),
			).Sort("score", qx.DESC).Offset(100).Limit(90),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := runQueryKeysChecked(t, db, tc.q)
			normalizedQ := normalizeQueryForTest(tc.q)
			normalized := runQueryKeysChecked(t, db, normalizedQ)
			assertQueryIDsEqual(t, tc.q, base, normalized)

			noisyQ := withNoisyEquivalentQuery(tc.q, len(tc.name))
			noisy := runQueryKeysChecked(t, db, noisyQ)
			assertQueryIDsEqual(t, tc.q, base, noisy)
		})
	}
}

type metamorphicDataProfile struct {
	name        string
	scoreLevels int
	activeTrue  float64
	hotCountryP float64
	hotTagP     float64
}

func seedMetamorphicDataProfile(t *testing.T, db *DB[uint64, Rec], n int, p metamorphicDataProfile) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	r := newRand(173 + int64(n) + int64(p.scoreLevels)*11)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland", "US", "JP"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}
	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	ids := make([]uint64, 0, batchSize)
	vals := make([]*Rec, 0, batchSize)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			t.Fatalf("BatchSet(seed profile batch=%d): %v", len(ids), err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		country := countries[r.IntN(len(countries))]
		if r.Float64() < p.hotCountryP {
			country = "NL"
		}

		active := r.Float64() < p.activeTrue
		name := names[r.IntN(len(names))]
		age := 18 + r.IntN(55)

		scoreBase := 0
		if p.scoreLevels > 0 {
			scoreBase = r.IntN(p.scoreLevels)
		}
		scoreJitter := r.Float64() * 0.001
		score := float64(scoreBase) + scoreJitter
		if p.scoreLevels <= 1 {
			score = math.Round((r.Float64()*100.0)*100) / 100
		}

		tagCount := 1 + r.IntN(3)
		if r.Float64() > p.hotTagP {
			tagCount = 1 + r.IntN(2)
		}
		tags := make([]string, 0, tagCount)
		if r.Float64() < p.hotTagP {
			tags = append(tags, "go")
		}
		for len(tags) < tagCount {
			tags = append(tags, tagPool[r.IntN(len(tagPool))])
		}

		rec := &Rec{
			Meta:     Meta{Country: country},
			Name:     name,
			Email:    fmt.Sprintf("%s-%d@example.test", name, i),
			Age:      age,
			Score:    score,
			Active:   active,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}

		if i%7 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			rec.Opt = &s
		}

		ids = append(ids, uint64(i))
		vals = append(vals, rec)
		if len(ids) == cap(ids) {
			flush()
		}
	}
	flush()
}

func randomMetamorphicLeaf(r *rand.Rand) qx.Expr {
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}

	switch r.IntN(14) {
	case 0:
		return qx.EQ("active", r.IntN(2) == 0)
	case 1:
		return qx.IN("country", pickVals(countries, 2))
	case 2:
		return qx.NOTIN("country", pickVals(countries, 2))
	case 3:
		return qx.GTE("age", 18+r.IntN(30))
	case 4:
		return qx.LTE("age", 30+r.IntN(35))
	case 5:
		return qx.HASANY("tags", pickVals(tagPool, 2))
	case 6:
		return qx.HASALL("tags", pickVals(tagPool, 2))
	case 7:
		return qx.HASNONE("tags", pickVals(tagPool, 2))
	case 8:
		return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", 1+r.IntN(3)))
	case 9:
		return qx.SUFFIX("country", "land")
	case 10:
		return qx.GT("score", float64(r.IntN(40)))
	case 11:
		return qx.NE("name", []string{"alice", "bob", "carol"}[r.IntN(3)])
	case 12:
		return qx.CONTAINS("country", "land")
	default:
		return qx.NOT(qx.EQ("active", r.IntN(2) == 0))
	}
}

func randomMetamorphicAndExpr(r *rand.Rand, leafN int) qx.Expr {
	if leafN < 1 {
		leafN = 1
	}
	ops := make([]qx.Expr, 0, leafN)
	for i := 0; i < leafN; i++ {
		ops = append(ops, randomMetamorphicLeaf(r))
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return qx.AND(ops...)
}

func randomMetamorphicQuery(r *rand.Rand) *qx.QX {
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}
	pickOrder := func() []qx.Order {
		switch r.IntN(5) {
		case 0, 1:
			return []qx.Order{testOrderBasic("score", r.IntN(2) == 0)}
		case 2:
			return []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
		case 3:
			return []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
		default:
			if r.IntN(100) < 50 {
				return []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
			}
			return []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
		}
	}

	var expr qx.Expr
	if r.IntN(100) < 35 {
		branchN := 2 + r.IntN(2)
		branches := make([]qx.Expr, 0, branchN)
		for i := 0; i < branchN; i++ {
			branches = append(branches, randomMetamorphicAndExpr(r, 1+r.IntN(3)))
		}
		expr = qx.OR(branches...)
	} else {
		expr = randomMetamorphicAndExpr(r, 1+r.IntN(4))
	}

	q := &qx.QX{
		Filter: expr,
	}
	mode := r.IntN(100)
	if mode < 45 {
		// paged + ordered
		q.Order = pickOrder()
		q.Window.Offset = uint64(r.IntN(120))
		q.Window.Limit = uint64(20 + r.IntN(120))
		if r.IntN(100) < 25 {
			q.Window.Offset = uint64(250 + r.IntN(400))
		}
	} else if mode < 65 {
		// paged + no-order (window invariants should hold across routes)
		q.Window.Offset = uint64(r.IntN(180))
		q.Window.Limit = uint64(10 + r.IntN(120))
		if r.IntN(100) < 20 {
			q.Window.Offset = uint64(250 + r.IntN(350))
		}
	} else if mode < 80 {
		// ordered + unpaged
		q.Order = pickOrder()
		q.Window.Offset = 0
		q.Window.Limit = 0
	} else {
		// no-order + unpaged baseline
		q.Window.Offset = 0
		q.Window.Limit = 0
	}
	return q
}

func TestQuery_Metamorphic_RandomizedProfiles_RouteEquivalence(t *testing.T) {
	profiles := []metamorphicDataProfile{
		{
			name:        "Uniform",
			scoreLevels: 50_000,
			activeTrue:  0.50,
			hotCountryP: 0.15,
			hotTagP:     0.30,
		},
		{
			name:        "Skewed",
			scoreLevels: 30_000,
			activeTrue:  0.88,
			hotCountryP: 0.75,
			hotTagP:     0.85,
		},
		{
			name:        "HighCardOrder",
			scoreLevels: 100_000,
			activeTrue:  0.52,
			hotCountryP: 0.20,
			hotTagP:     0.35,
		},
		{
			name:        "LowCardOrder",
			scoreLevels: 16,
			activeTrue:  0.55,
			hotCountryP: 0.30,
			hotTagP:     0.50,
		},
	}

	for pi := range profiles {
		p := profiles[pi]
		t.Run(p.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
			seedMetamorphicDataProfile(t, db, 8_000, p)

			r := newRand(777 + int64(pi)*1000)
			const queryCount = 70

			for i := 0; i < queryCount; i++ {
				q := randomMetamorphicQuery(r)

				base := runQueryKeysChecked(t, db, q)

				nq := normalizeQueryForTest(q)
				normalized := runQueryKeysChecked(t, db, nq)
				if !queryIDsEqual(q, base, normalized) {
					t.Fatalf(
						"normalize mismatch (profile=%s, i=%d):\nq=%+v\nnq=%+v\nbase=%v\nnorm=%v",
						p.name, i, q, nq, base, normalized,
					)
				}

				noisy := runQueryKeysChecked(t, db, withNoisyEquivalentQuery(q, i))
				assertQueryIDsEqual(t, q, base, noisy)

				_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, q)
				assertQueryIDsEqual(t, q, base, prepared)

				countQ := cloneQuery(q)
				countQ.Order = nil
				countQ.Window.Offset = 0
				countQ.Window.Limit = 0

				wantCountKeys, err := expectedKeysUint64(t, db, countQ)
				if err != nil {
					t.Fatalf(
						"expectedKeysUint64(count profile=%s i=%d): %v\nq=%+v",
						p.name, i, err, q,
					)
				}
				wantCount := uint64(len(wantCountKeys))

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					t.Fatalf("Count(profile=%s i=%d): %v\nq=%+v", p.name, i, err, q)
				}
				if gotCount != wantCount {
					t.Fatalf(
						"count mismatch (profile=%s i=%d): got=%d want=%d\nq=%+v\ncountQ=%+v",
						p.name, i, gotCount, wantCount, q, countQ,
					)
				}

				preparedCount, err := db.countPreparedExpr(normalizeQueryForTest(q).Filter)
				if err != nil {
					t.Fatalf("countPreparedExpr(profile=%s i=%d): %v\nq=%+v", p.name, i, err, q)
				}
				if preparedCount != wantCount {
					t.Fatalf(
						"prepared count mismatch (profile=%s i=%d): got=%d want=%d\nq=%+v\ncountQ=%+v",
						p.name, i, preparedCount, wantCount, q, countQ,
					)
				}
			}
		})
	}
}

func capturedNotInOrderOffsetQuery() *qx.QX {
	return qx.Query(
		qx.NOTIN("country", []string{"Iceland", "Finland"}),
		qx.NOTIN("country", []string{"Iceland", "DE"}),
	).Sort("score", qx.ASC).Offset(446).Limit(70)
}

func openSkewedNotInRegressionDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
		name:        "Skewed",
		scoreLevels: 30_000,
		activeTrue:  0.88,
		hotCountryP: 0.75,
		hotTagP:     0.85,
	})
	return db
}

func assertNotInOrderOffsetRouteEquivalence(t *testing.T, q *qx.QX) {
	t.Helper()

	db := openSkewedNotInRegressionDB(t)

	got := runQueryKeysChecked(t, db, q)
	_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, q)
	assertQueryIDsEqual(t, q, got, prepared)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func TestRegression_NotInOrderOffset_RouteEquivalence(t *testing.T) {
	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "captured_shape",
			q:    capturedNotInOrderOffsetQuery(),
		},
		{
			name: "different_values",
			q: qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				qx.NOTIN("country", []string{"Thailand", "US"}),
			).Sort("score", qx.ASC).Offset(210).Limit(90),
		},
		{
			name: "desc_order",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.DESC).Offset(446).Limit(70),
		},
		{
			name: "without_offset",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.ASC).Limit(70),
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			assertNotInOrderOffsetRouteEquivalence(t, tests[i].q)
		})
	}
}

func TestRegression_NotInOrderOffset_NoStateCorruption(t *testing.T) {
	db := openSkewedNotInRegressionDB(t)
	q := capturedNotInOrderOffsetQuery()

	before := runQueryKeysChecked(t, db, q)
	_, _, _, _ = assertPreparedRouteEquivalence(t, db, q)
	mid := runQueryKeysChecked(t, db, q)
	_, _, _, _ = assertPreparedRouteEquivalence(t, db, q)
	after := runQueryKeysChecked(t, db, q)

	if !slices.Equal(before, mid) || !slices.Equal(before, after) {
		t.Fatalf("query results changed after interleaved route checks:\nbefore=%v\nmid=%v\nafter=%v", before, mid, after)
	}
}

func TestRegression_MultiTermHAS_LeadSelfCheck_RouteAndCount(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
		name:        "Uniform",
		scoreLevels: 50_000,
		activeTrue:  0.50,
		hotCountryP: 0.15,
		hotTagP:     0.30,
	})

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "no_order_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Limit(120),
		},
		{
			name: "no_order_offset_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Offset(40).Limit(80),
		},
		{
			name: "ordered_offset_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Sort("age", qx.ASC).Offset(20).Limit(90),
		},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			got, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			want, err := expectedKeysUint64(t, db, tc.q)
			if err != nil {
				t.Fatalf("expectedKeysUint64: %v", err)
			}
			assertQueryIDsEqual(t, tc.q, got, want)

			_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, tc.q)
			assertQueryIDsEqual(t, tc.q, got, prepared)

			cntPred, ok, err := db.tryCountByPredicates(tc.q.Filter, nil)
			if err != nil {
				t.Fatalf("tryCountByPredicates: %v", err)
			}
			if !ok {
				t.Fatalf("expected tryCountByPredicates fast-path for query shape")
			}

			countQ := cloneQuery(tc.q)
			countQ.Order = nil
			countQ.Window.Offset = 0
			countQ.Window.Limit = 0
			wantCountKeys, err := expectedKeysUint64(t, db, countQ)
			if err != nil {
				t.Fatalf("expectedKeysUint64(count): %v", err)
			}
			wantCount := uint64(len(wantCountKeys))

			cnt, err := db.Count(tc.q.Filter)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if cnt != wantCount {
				t.Fatalf("Count mismatch: got=%d want=%d", cnt, wantCount)
			}
			if cntPred != wantCount {
				t.Fatalf("tryCountByPredicates mismatch: got=%d want=%d", cntPred, wantCount)
			}
		})
	}
}

func TestRegression_CountORByPredicates_MultiTermHASLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
		name:        "Skewed",
		scoreLevels: 30_000,
		activeTrue:  0.88,
		hotCountryP: 0.75,
		hotTagP:     0.85,
	})

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.SUFFIX("country", "land"),
			),
			qx.AND(
				qx.HASALL("tags", []string{"go", "db"}),
				qx.PREFIX("full_name", "FN-1"),
			),
		),
	)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	cntFast, ok, err := db.tryCountORByPredicates(q.Filter, nil)
	if err != nil {
		t.Fatalf("tryCountORByPredicates: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryCountORByPredicates fast-path for OR shape")
	}
	if cntFast != uint64(len(want)) {
		t.Fatalf("tryCountORByPredicates mismatch: got=%d want=%d", cntFast, len(want))
	}

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(want))
	}
}

func TestQueryKeys_NoOrderBroadNegativeAll_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "GB"}
	seedGeneratedUint64Data(t, db, 100_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    18 + (i % 50),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(qx.NOTIN("country", []string{"US"}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) < 64_000 {
		t.Fatalf("expected broad negative result to exercise large no-order route, got %d rows", len(got))
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_BaseAndMutated(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 8_000)

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(base): %v", err)
	}

	queries := []*qx.QX{
		qx.Query(
			qx.EQ("active", true),
			qx.GTE("age", 22),
			qx.LT("age", 50),
		).Sort("age", qx.ASC).Limit(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).Sort("full_name", qx.ASC).Limit(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Limit(140),
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.HASANY("tags", []string{"go", "ops"}),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.GTE("age", 25),
				),
			),
		).Sort("score", qx.DESC).Offset(40).Limit(90),
		queryOrderSortByArrayCount(qx.Query(
			qx.GTE("age", 20),
		), "tags", qx.DESC).Offset(7).Limit(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Limit(85),
		qx.Query(
			qx.GTE("age", 24),
			qx.LTE("age", 42),
			qx.EQ("active", true),
		).Limit(95),
		qx.Query(
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"Thailand", "Iceland"}),
		),
	}

	runChecks := func(label string) {
		t.Helper()

		var sawExec bool
		var sawPlan bool

		for i := range queries {
			q := queries[i]
			nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalence(t, db, q)
			sawExec = sawExec || usedExec
			sawPlan = sawPlan || usedPlan

			got, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("%s q%d QueryKeys: %v", label, i, err)
			}

			want, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("%s q%d expectedKeysUint64: %v", label, i, err)
			}

			if len(q.Order) == 0 && (q.Window.Limit > 0 || q.Window.Offset > 0) {
				fullQ := cloneQuery(q)
				fullQ.Window.Offset = 0
				fullQ.Window.Limit = 0
				fullWant, err := expectedKeysUint64(t, db, fullQ)
				if err != nil {
					t.Fatalf("%s q%d expectedKeysUint64(full): %v", label, i, err)
				}
				assertNoOrderWindowSubset(t, q, got, fullWant, fmt.Sprintf("%s q%d QueryKeys", label, i))
				assertNoOrderWindowSubset(t, nq, ref, fullWant, fmt.Sprintf("%s q%d prepared", label, i))
			} else {
				assertQueryIDsEqual(t, q, got, want)
				assertQueryIDsEqual(t, nq, ref, want)
			}

			if q.Window.Limit == 0 && q.Window.Offset == 0 {
				cnt, err := db.countPreparedExpr(nq.Filter)
				if err != nil {
					t.Fatalf("%s q%d countPreparedExpr: %v", label, i, err)
				}
				if cnt != uint64(len(ref)) {
					t.Fatalf("%s q%d count mismatch on prepared route: got=%d want=%d", label, i, cnt, len(ref))
				}
			}
		}

		if !sawExec {
			t.Fatalf("%s: expected at least one execution fast-path route", label)
		}
		if !sawPlan {
			t.Fatalf("%s: expected at least one planner fast-path route", label)
		}
	}

	runChecks("base")

	r := newRand(20260227)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}
	for i := 0; i < 320; i++ {
		id := uint64(1 + r.IntN(10_500))
		switch r.IntN(3) {
		case 0:
			name := names[r.IntN(len(names))]
			rec := &Rec{
				Meta:     Meta{Country: countries[r.IntN(len(countries))]},
				Name:     name,
				Email:    fmt.Sprintf("%s-%d@example.test", name, id),
				Age:      18 + r.IntN(65),
				Score:    float64(r.IntN(2_000))/10.0 + r.Float64()*0.001,
				Active:   r.IntN(2) == 0,
				Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
				FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(15_000)),
			}
			if err := db.Set(id, rec); err != nil {
				t.Fatalf("mutated Set(id=%d): %v", id, err)
			}
		case 1:
			patch := []Field{{Name: "age", Value: float64(18 + r.IntN(65))}}
			if err := db.Patch(id, patch); err != nil {
				t.Fatalf("mutated Patch(id=%d): %v", id, err)
			}
		default:
			if err := db.Delete(id); err != nil {
				t.Fatalf("mutated Delete(id=%d): %v", id, err)
			}
		}
	}

	runChecks("mutated")
}

/**/

func TestNormalize_WrappedQueryMatchesDirectResults(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 10_000)

	direct := qx.Query(
		qx.GTE("age", 18),
		qx.EQ("active", true),
	).Sort("score", qx.DESC).Offset(500).Limit(100)

	wrapped := &qx.QX{
		Filter: qx.OR(
			direct.Filter,
			qx.NOT(qx.Expr{}),
		),
		Order:  direct.Order,
		Window: direct.Window,
	}

	gotDirect, err := db.QueryKeys(direct)
	if err != nil {
		t.Fatalf("QueryKeys(direct): %v", err)
	}
	gotWrapped, err := db.QueryKeys(wrapped)
	if err != nil {
		t.Fatalf("QueryKeys(wrapped): %v", err)
	}

	if !slices.Equal(gotWrapped, gotDirect) {
		t.Fatalf("results mismatch:\n wrapped=%v\n direct=%v", gotWrapped, gotDirect)
	}
}

func TestNormalizeExpr_AlreadyCanonicalAND_AllocsPerRunStayZero(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	expr := qir.Expr{
		Op: qir.OpAND,
		Operands: []qir.Expr{
			{Op: qir.OpEQ, FieldOrdinal: 0, Value: true},
			{Op: qir.OpIN, FieldOrdinal: 1, Value: []string{"DE", "NL"}},
			{Op: qir.OpHASANY, FieldOrdinal: 2, Value: []string{"go", "ops"}},
			{Op: qir.OpGTE, FieldOrdinal: 3, Value: 21},
		},
	}

	out, changed := qir.NormalizeExpr(expr)
	if changed {
		t.Fatalf("expected canonical AND to stay unchanged")
	}
	if !reflect.DeepEqual(out, expr) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, expr)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = qir.NormalizeExpr(expr)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}

func TestNormalizeExpr_AlreadyCanonicalOR_AllocsPerRunStayZero(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	expr := qir.Expr{
		Op: qir.OpOR,
		Operands: []qir.Expr{
			{Op: qir.OpEQ, FieldOrdinal: 0, Value: true},
			{Op: qir.OpIN, FieldOrdinal: 1, Value: []string{"DE", "NL"}},
			{Op: qir.OpPREFIX, FieldOrdinal: 2, Value: "ali"},
		},
	}

	out, changed := qir.NormalizeExpr(expr)
	if changed {
		t.Fatalf("expected canonical OR to stay unchanged")
	}
	if !reflect.DeepEqual(out, expr) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, expr)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = qir.NormalizeExpr(expr)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}
