package rbi

import (
	"fmt"
	"github.com/vapstack/rbi/rbierrors"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qir"
	"go.etcd.io/bbolt"
)

type Meta struct {
	Country string `db:"country" rbi:"index"`
}

func compareFloat64QuerySemantics(a, b float64) int {
	a = keycodec.CanonicalizeFloat64ForIndex(a)
	b = keycodec.CanonicalizeFloat64ForIndex(b)

	aNaN := math.IsNaN(a)
	bNaN := math.IsNaN(b)
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	case bNaN:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

const (
	expectedOrderBasic = iota + 1
	expectedOrderArrayPos
	expectedOrderArrayCount
)

type expectedOrder struct {
	kind     int
	field    string
	desc     bool
	priority []string
}

func queryExpectedOrder(q *qx.QX) (expectedOrder, error) {
	if len(q.Order) == 0 {
		return expectedOrder{}, nil
	}
	if len(q.Order) > 1 {
		return expectedOrder{}, fmt.Errorf("rbi does not support multi-column ordering")
	}

	src := q.Order[0]
	by := src.By
	switch by.Kind {
	case qx.KindREF:
		return expectedOrder{kind: expectedOrderBasic, field: by.Name, desc: src.Desc}, nil
	case qx.KindOP:
		switch by.Name {
		case qx.OpLEN:
			if len(by.Args) != 1 || by.Args[0].Kind != qx.KindREF {
				return expectedOrder{}, fmt.Errorf("rbi: invalid LEN order expression")
			}
			return expectedOrder{kind: expectedOrderArrayCount, field: by.Args[0].Name, desc: src.Desc}, nil
		case qx.OpPOS:
			if len(by.Args) != 2 || by.Args[0].Kind != qx.KindREF || by.Args[1].Kind != qx.KindLIT {
				return expectedOrder{}, fmt.Errorf("rbi: invalid POS order expression")
			}
			priority, _ := by.Args[1].Value.([]string)
			return expectedOrder{kind: expectedOrderArrayPos, field: by.Args[0].Name, desc: src.Desc, priority: priority}, nil
		}
	}
	return expectedOrder{}, fmt.Errorf("rbi does not support order expression")
}

func unwrapExprValue(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return reflect.Value{}, true
			}
			v = v.Elem()
		default:
			return v, false
		}
	}
	return reflect.Value{}, true
}

func (db *DB[K, V]) idxFromUserKey(id K) uint64 {
	idx, _ := db.idxFromUserKeyWithCreated(id)
	return idx
}

func (db *DB[K, V]) idxFromUserKeyWithCreated(id K) (uint64, bool) {
	if db.strKey {
		var out uint64
		err := db.bolt.View(func(tx *bbolt.Tx) error {
			var keyBuf [8]byte
			v := tx.Bucket(db.dataBucket).Get(keycodec.UserKeyBytesWithBuf(id, true, &keyBuf))
			if v == nil {
				return nil
			}
			idx := keycodec.U64FromBytes(v[:8])
			out = idx
			return nil
		})
		if err != nil {
			panic(err)
		}
		return out, false
	}
	return *(*uint64)(unsafe.Pointer(&id)), false
}

type Rec struct {
	Meta

	Name   string   `db:"name"   rbi:"index"`
	Email  string   `db:"email"  rbi:"index"`
	Age    int      `db:"age"    rbi:"index"`
	Score  float64  `db:"score"  rbi:"index"`
	Active bool     `db:"active" rbi:"index"`
	Tags   []string `db:"tags"   rbi:"index"`

	FullName string  `db:"full_name" json:"fullName" rbi:"index"`
	Opt      *string `db:"opt"                       rbi:"index"`
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

func seedData(t *testing.T, db *DB[uint64, Rec], n int) []uint64 {
	t.Helper()

	db.disableSync()
	defer db.enableSync()

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

	db.disableSync()
	defer db.enableSync()

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

func testOrderBasic(field string, desc bool) qx.Order {
	return qx.Order{By: qx.REF(field), Desc: desc}
}

func testOrderByArrayPos(field string, priority []string, desc bool) qx.Order {
	return qx.Order{By: qx.POS(field, priority), Desc: desc}
}

func testOrderByArrayCount(field string, desc bool) qx.Order {
	return qx.Order{By: qx.LEN(field), Desc: desc}
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

func (testExprFieldResolver) ResolveField(name string) (qir.FieldInfo, bool) {
	switch name {
	case "country":
		return qir.FieldInfo{Ordinal: 0, Caps: qir.FieldCapAll}, true
	case "name":
		return qir.FieldInfo{Ordinal: 1, Caps: qir.FieldCapAll}, true
	case "email":
		return qir.FieldInfo{Ordinal: 2, Caps: qir.FieldCapAll}, true
	case "age":
		return qir.FieldInfo{Ordinal: 3, Caps: qir.FieldCapAll}, true
	case "score":
		return qir.FieldInfo{Ordinal: 4, Caps: qir.FieldCapAll}, true
	case "active":
		return qir.FieldInfo{Ordinal: 5, Caps: qir.FieldCapAll}, true
	case "tags":
		return qir.FieldInfo{Ordinal: 6, Caps: qir.FieldCapAll}, true
	case "full_name", "fullName":
		return qir.FieldInfo{Ordinal: 7, Caps: qir.FieldCapAll}, true
	case "opt":
		return qir.FieldInfo{Ordinal: 8, Caps: qir.FieldCapAll}, true
	default:
		return qir.FieldInfo{Ordinal: qir.NoFieldOrdinal}, false
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
	if e.Op == qir.OpConst {
		if e.Not {
			return false, nil
		}
		return true, nil
	}

	if e.Op == qir.OpAND || e.Op == qir.OpOR {
		if len(e.Operands) == 0 {
			return false, rbierrors.ErrInvalidQuery
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
func expectedKeysUint64(t testing.TB, db *DB[uint64, Rec], q *qx.QX) ([]uint64, error) {
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
		order, err := queryExpectedOrder(q)
		if err != nil {
			return nil, err
		}
		f := order.field
		switch order.kind {
		case expectedOrderBasic:
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)
				cmp := compareOrderedFieldValues(va, vb, order.desc)
				if cmp == 0 {
					return a.id < b.id
				}
				return cmp < 0
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case expectedOrderArrayPos:
			want := order.priority
			if len(want) == 0 {
				sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
				break
			}
			priority := want
			if order.desc {
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

		case expectedOrderArrayCount:
			sort.Slice(rows, func(i, j int) bool {
				ai, _ := fieldValue(rows[i].rec, f).([]string)
				aj, _ := fieldValue(rows[j].rec, f).([]string)
				ci := distinctCountStrings(ai)
				cj := distinctCountStrings(aj)
				if ci == cj {
					return rows[i].id < rows[j].id
				}
				if order.desc {
					return ci > cj
				}
				return ci < cj
			})

		default:
			return nil, fmt.Errorf("test harness: unknown order kind %v", order.kind)
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
				idx: db.idxFromUserKey(id),
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
		order, err := queryExpectedOrder(q)
		if err != nil {
			return nil, err
		}
		f := order.field
		switch order.kind {
		case expectedOrderBasic:
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)
				cmp := compareOrderedFieldValues(va, vb, order.desc)
				if cmp == 0 {
					return a.idx < b.idx
				}
				return cmp < 0
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case expectedOrderArrayPos:
			want := order.priority
			if len(want) == 0 {
				sort.Slice(rows, func(i, j int) bool { return rows[i].idx < rows[j].idx })
				break
			}
			priority := want
			if order.desc {
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

		case expectedOrderArrayCount:
			sort.Slice(rows, func(i, j int) bool {
				ai, _ := fieldValue(rows[i].rec, f).([]string)
				aj, _ := fieldValue(rows[j].rec, f).([]string)
				ci := distinctCountStrings(ai)
				cj := distinctCountStrings(aj)
				if ci == cj {
					return rows[i].idx < rows[j].idx
				}
				if order.desc {
					return ci > cj
				}
				return ci < cj
			})

		default:
			return nil, fmt.Errorf("test harness: unknown order kind %v", order.kind)
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

func assertNoOrderWindowSubsetString(t testing.TB, q *qx.QX, got, full []string, label string) {
	t.Helper()
	if err := queryContractValidateNoOrderWindow(q, got, full); err != nil {
		t.Fatalf("%s: %v", label, err)
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
	if slices.Equal(a, b) {
		return true
	}
	return slices.Equal(sortedStringIDs(a), sortedStringIDs(b))
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
	if queryContractNoOrderWindow(q) {
		t.Fatalf("no-order window results must be checked with assertNoOrderWindowSubset: q=%+v\nA=%v\nB=%v", q, a, b)
	}
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
	if slices.Equal(a, b) {
		return true
	}

	sa := sortedIDs(a)
	sb := sortedIDs(b)
	return slices.Equal(sa, sb)
}

func assertNoOrderWindowSubset(t testing.TB, q *qx.QX, got, full []uint64, label string) {
	t.Helper()
	if err := queryContractValidateNoOrderWindow(q, got, full); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
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

type metamorphicDataProfile struct {
	name        string
	scoreLevels int
	activeTrue  float64
	hotCountryP float64
	hotTagP     float64
}

func seedMetamorphicDataProfile(t *testing.T, db *DB[uint64, Rec], n int, p metamorphicDataProfile) {
	t.Helper()

	db.disableSync()
	defer db.enableSync()

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

func assertNotInOrderOffsetQueryMatchesReference(t *testing.T, q *qx.QX) {
	t.Helper()

	db := openSkewedNotInRegressionDB(t)

	got := runQueryKeysChecked(t, db, q)
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

type queryContract[K ~uint64 | ~string] struct {
	t         testing.TB
	db        *DB[K, Rec]
	reference func(testing.TB, *DB[K, Rec], *qx.QX) ([]K, error)
	equal     func(*qx.QX, []K, []K) bool
}

type queryContractReference[K ~uint64 | ~string] struct {
	pageKeys []K
	fullKeys []K
	pageSet  bool
	fullSet  bool
}

func (r *queryContractReference[K]) page(c queryContract[K], q *qx.QX) []K {
	if !r.pageSet {
		keys, err := c.reference(c.t, c.db, q)
		if err != nil {
			c.t.Fatalf("reference keys(%+v): %v", q, err)
		}
		r.pageKeys = keys
		r.pageSet = true
	}
	return r.pageKeys
}

func (r *queryContractReference[K]) full(c queryContract[K], q *qx.QX) []K {
	if !r.fullSet {
		if q.Window.Offset == 0 && q.Window.Limit == 0 && len(q.Order) == 0 {
			r.fullKeys = r.page(c, q)
		} else {
			fullQ := cloneQuery(q)
			clearQueryOrderWindowForTest(fullQ)
			keys, err := c.reference(c.t, c.db, fullQ)
			if err != nil {
				c.t.Fatalf("reference keys(%+v): %v", fullQ, err)
			}
			r.fullKeys = keys
		}
		r.fullSet = true
	}
	return r.fullKeys
}

func (r *queryContractReference[K]) count(c queryContract[K], q *qx.QX) uint64 {
	if q.Window.Offset == 0 && q.Window.Limit == 0 && len(q.Order) == 0 {
		return uint64(len(r.page(c, q)))
	}
	return uint64(len(r.full(c, q)))
}

func newUint64QueryContract(t testing.TB, db *DB[uint64, Rec]) queryContract[uint64] {
	t.Helper()
	return queryContract[uint64]{
		t:         t,
		db:        db,
		reference: expectedKeysUint64,
		equal:     queryIDsEqual,
	}
}

func (c queryContract[K]) ReferenceKeys(q *qx.QX) []K {
	c.t.Helper()
	var ref queryContractReference[K]
	return ref.page(c, q)
}

func (c queryContract[K]) ReferenceFullKeys(q *qx.QX) []K {
	c.t.Helper()
	var ref queryContractReference[K]
	return ref.full(c, q)
}

func (c queryContract[K]) AssertQueryKeysMatchReference(q *qx.QX) []K {
	c.t.Helper()
	got, err := c.db.QueryKeys(q)
	if err != nil {
		c.t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	var ref queryContractReference[K]
	c.assertKeysMatchReference("QueryKeys", q, got, &ref)
	return got
}

func (c queryContract[K]) AssertKeysMatchReference(label string, q *qx.QX, got []K) {
	c.t.Helper()
	var ref queryContractReference[K]
	c.assertKeysMatchReference(label, q, got, &ref)
}

func (c queryContract[K]) assertKeysMatchReference(label string, q *qx.QX, got []K, ref *queryContractReference[K]) {
	c.t.Helper()
	if err := queryContractValidateNoDuplicateKeys(label, got); err != nil {
		c.t.Fatal(err)
	}

	if queryContractNoOrderWindow(q) {
		full := ref.full(c, q)
		if err := queryContractValidateNoOrderWindow(q, got, full); err != nil {
			c.t.Fatalf("%s no-order window mismatch: %v\ngot=%v\nfull=%v", label, err, got, full)
		}
		return
	}

	want := ref.page(c, q)
	if !c.equal(q, got, want) {
		c.t.Fatalf("%s mismatch:\nq=%+v\ngot=%v\nwant=%v", label, q, got, want)
	}
}

func (c queryContract[K]) AssertQueryRecordsMatchReference(q *qx.QX) []*Rec {
	c.t.Helper()

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}

	if len(q.Order) > 0 {
		wantIDs := c.ReferenceKeys(q)
		want := c.batchGet("BatchGet(reference keys)", wantIDs)
		queryContractAssertRecordSlicesEqual(c.t, "Query", q, got, want)
		return got
	}

	fullIDs := c.ReferenceFullKeys(q)
	full := c.batchGet("BatchGet(full reference keys)", fullIDs)
	if queryContractNoOrderWindow(q) {
		if err := queryContractValidateNoOrderRecordsWindow(q, got, full); err != nil {
			c.t.Fatalf("Query no-order window mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
		}
		return got
	}

	if err := queryContractValidateNoOrderRecordsFull(got, full); err != nil {
		c.t.Fatalf("Query no-order full-set mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
	}
	return got
}

func (c queryContract[K]) AssertQueryRecordsMatchKeys(q *qx.QX, keys []K) []*Rec {
	c.t.Helper()

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}
	want := c.batchGet("BatchGet(QueryKeys result)", keys)
	queryContractAssertRecordSlicesEqual(c.t, "Query vs QueryKeys", q, got, want)
	return got
}

func (c queryContract[K]) AssertCountMatchesReference(q *qx.QX) uint64 {
	c.t.Helper()

	var ref queryContractReference[K]
	return c.assertCountMatchesReference(q, &ref)
}

func (c queryContract[K]) assertCountMatchesReference(q *qx.QX, ref *queryContractReference[K]) uint64 {
	c.t.Helper()

	want := ref.count(c, q)
	got, err := c.db.Count(q.Filter)
	if err != nil {
		c.t.Fatalf("Count(%+v): %v", q, err)
	}
	if got != want {
		c.t.Fatalf("Count mismatch:\nq=%+v\ngot=%d\nwant=%d", q, got, want)
	}
	return got
}

func (c queryContract[K]) assertAllReadPathsMatchReference(q *qx.QX) queryContractReference[K] {
	c.t.Helper()
	var ref queryContractReference[K]

	keys, err := c.db.QueryKeys(q)
	if err != nil {
		c.t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	c.assertKeysMatchReference("QueryKeys", q, keys, &ref)

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}

	if len(q.Order) > 0 {
		wantIDs := ref.page(c, q)
		want := c.batchGet("BatchGet(reference keys)", wantIDs)
		queryContractAssertRecordSlicesEqual(c.t, "Query", q, got, want)
	} else {
		fullIDs := ref.full(c, q)
		full := c.batchGet("BatchGet(full reference keys)", fullIDs)
		if queryContractNoOrderWindow(q) {
			if err := queryContractValidateNoOrderRecordsWindow(q, got, full); err != nil {
				c.t.Fatalf("Query no-order window mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
			}
		} else if err := queryContractValidateNoOrderRecordsFull(got, full); err != nil {
			c.t.Fatalf("Query no-order full-set mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
		}
	}

	wantByKeys := c.batchGet("BatchGet(QueryKeys result)", keys)
	queryContractAssertRecordSlicesEqual(c.t, "Query vs QueryKeys", q, got, wantByKeys)

	c.assertCountMatchesReference(q, &ref)

	return ref
}

func (c queryContract[K]) batchGet(label string, ids []K) []*Rec {
	c.t.Helper()
	items, err := c.db.BatchGet(ids...)
	if err != nil {
		c.t.Fatalf("%s: %v", label, err)
	}
	return items
}

func clearQueryOrderWindowForTest(q *qx.QX) {
	if q == nil {
		return
	}
	q.Order = nil
	q.Window.Offset = 0
	q.Window.Limit = 0
}

func queryContractNoOrderWindow(q *qx.QX) bool {
	return q != nil && len(q.Order) == 0 && (q.Window.Offset > 0 || q.Window.Limit > 0)
}

func queryContractWindowMaxLen(q *qx.QX, fullLen int) int {
	maxLen := fullLen
	if q.Window.Offset >= uint64(fullLen) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = fullLen - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	return maxLen
}

func queryContractValidateNoDuplicateKeys[K comparable](label string, keys []K) error {
	seen := make(map[K]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s duplicate key=%v result=%v", label, key, keys)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func queryContractValidateNoOrderWindow[K comparable](q *qx.QX, got, full []K) error {
	if err := queryContractValidateNoDuplicateKeys("no-order window", got); err != nil {
		return err
	}

	allow := make(map[K]struct{}, len(full))
	for _, key := range full {
		allow[key] = struct{}{}
	}
	for _, key := range got {
		if _, ok := allow[key]; !ok {
			return fmt.Errorf("key=%v outside full result set", key)
		}
	}

	maxLen := queryContractWindowMaxLen(q, len(full))
	if len(got) > maxLen {
		return fmt.Errorf("window overflow got=%d max=%d", len(got), maxLen)
	}
	return nil
}

func queryContractRecSignature(rec *Rec) string {
	if rec == nil {
		return "<nil>"
	}
	opt := "<nil>"
	if rec.Opt != nil {
		opt = *rec.Opt
	}
	return fmt.Sprintf(
		"%s|%s|%d|%g|%t|%s|%s|%s|%s",
		rec.Name,
		rec.Email,
		rec.Age,
		rec.Score,
		rec.Active,
		rec.Country,
		rec.FullName,
		opt,
		strings.Join(rec.Tags, "\x1f"),
	)
}

func queryContractBuildRecSignatureCounts(items []*Rec) map[string]int {
	out := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			continue
		}
		out[queryContractRecSignature(items[i])]++
	}
	return out
}

func queryContractRecordSignatures(items []*Rec) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = queryContractRecSignature(items[i])
	}
	return out
}

func queryContractValidateNoOrderRecordsWindow(q *qx.QX, items, full []*Rec) error {
	fullSigCounts := queryContractBuildRecSignatureCounts(full)
	seen := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			return fmt.Errorf("nil item at i=%d", i)
		}
		sig := queryContractRecSignature(items[i])
		limit, ok := fullSigCounts[sig]
		if !ok {
			return fmt.Errorf("item %q outside full result set", sig)
		}
		seen[sig]++
		if seen[sig] > limit {
			return fmt.Errorf("duplicate item %q exceeds full-set multiplicity", sig)
		}
	}

	maxLen := queryContractWindowMaxLen(q, len(full))
	if len(items) > maxLen {
		return fmt.Errorf("items window overflow got=%d max=%d", len(items), maxLen)
	}
	return nil
}

func queryContractValidateNoOrderRecordsFull(items, full []*Rec) error {
	if len(items) != len(full) {
		return fmt.Errorf("full-set len mismatch got=%d want=%d", len(items), len(full))
	}
	gotCounts := queryContractBuildRecSignatureCounts(items)
	wantCounts := queryContractBuildRecSignatureCounts(full)
	if !reflect.DeepEqual(gotCounts, wantCounts) {
		return fmt.Errorf("full-set signature mismatch got=%v want=%v", gotCounts, wantCounts)
	}
	return nil
}

func queryContractAssertRecordSlicesEqual(t testing.TB, label string, q *qx.QX, got, want []*Rec) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s items len mismatch:\nq=%+v\ngot=%d\nwant=%d", label, q, len(got), len(want))
	}
	for i := range want {
		if got[i] == nil || want[i] == nil {
			t.Fatalf("%s nil item mismatch at i=%d:\nq=%+v\ngot=%#v\nwant=%#v", label, i, q, got[i], want[i])
		}
		if !reflect.DeepEqual(*got[i], *want[i]) {
			t.Fatalf("%s item mismatch at i=%d:\nq=%+v\ngot=%#v\nwant=%#v", label, i, q, got[i], want[i])
		}
	}
}

func seedQueryExtOptArrayPosDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := map[uint64]*Rec{
		1: {Name: "nil-1", Opt: nil, Active: true},
		2: {Name: "alpha", Opt: strPtr("alpha"), Active: true},
		3: {Name: "beta", Opt: strPtr("beta"), Active: false},
		4: {Name: "empty", Opt: strPtr(""), Active: true},
		5: {Name: "nil-2", Opt: nil, Active: false},
		6: {Name: "gamma", Opt: strPtr("gamma"), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	return db
}

func seedQueryExtOptAllNilDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "nil-b", Opt: nil, Active: false},
		3: {Name: "nil-c", Opt: nil, Active: true},
		4: {Name: "nil-d", Opt: nil, Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	return db
}

func queryExtOptPriority() []string {
	return []string{"alpha", "beta", "", "gamma"}
}

func clearQueryExtOrderWindow(q *qx.QX) {
	clearQueryOrderWindowForTest(q)
}

func queryExtSortByArrayPos(q *qx.QX, field string, priority []string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.POS(field, priority), dir)
}

func queryExtSortByArrayCount(q *qx.QX, field string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.LEN(field), dir)
}

func assertQueryExtIDsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()
	contract := newUint64QueryContract(t, db)
	contract.AssertQueryKeysMatchReference(q)
	return contract.ReferenceKeys(q)
}

func assertQueryExtItemsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).AssertQueryRecordsMatchReference(q)
}

func assertQueryExtCountMatchesOrderedResultSet(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q, err)
	}
	if cnt != uint64(len(got)) {
		t.Fatalf("Count(%+v)=%d does not match len(QueryKeys)=%d", q, cnt, len(got))
	}
}

func assertQueryExtCountMatchesBaseQuery(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).AssertCountMatchesReference(q)
}

func assertQueryExtAllReadPathsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).assertAllReadPathsMatchReference(q)
}

func queryExtItemNames(t testing.TB, items []*Rec) []string {
	t.Helper()

	out, ok := queryExtItemNamesOK(items)
	if !ok {
		t.Fatalf("nil item in result: %#v", items)
	}
	return out
}

func queryExtItemNamesOK(items []*Rec) ([]string, bool) {
	out := make([]string, len(items))
	for i := range items {
		if items[i] == nil {
			return nil, false
		}
		out[i] = items[i].Name
	}
	return out, true
}

func assertQueryExtConcurrentReadStable(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	wantPage, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(page %+v): %v", q, err)
	}
	wantItems, err := db.BatchGet(wantPage...)
	if err != nil {
		t.Fatalf("BatchGet(wantPage): %v", err)
	}
	countQ := cloneQuery(q)
	clearQueryExtOrderWindow(countQ)
	wantAll, err := expectedKeysUint64(t, db, countQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(count %+v): %v", countQ, err)
	}
	wantCount := uint64(len(wantAll))

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantPage) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, wantPage)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				if len(gotItems) != len(wantItems) {
					errCh <- fmt.Errorf("g=%d i=%d Query len mismatch: got=%d want=%d", gid, i, len(gotItems), len(wantItems))
					return
				}
				for j := range wantItems {
					if gotItems[j] == nil || wantItems[j] == nil || !reflect.DeepEqual(*gotItems[j], *wantItems[j]) {
						errCh <- fmt.Errorf("g=%d i=%d Query item mismatch at j=%d", gid, i, j)
						return
					}
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func assertQueryExtraPublicReadPathsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	assertQueryExtIDsMatchExpected(t, db, q)
	assertQueryExtItemsMatchExpected(t, db, q)
	assertQueryExtCountMatchesBaseQuery(t, db, q)
}

func queryExtOrderedORNegativeResidualFixture() ([]uint64, []*Rec, []*Rec, *qx.QX) {
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	stateA := []*Rec{
		{Name: "A1-fr-low", Age: 20, Score: 30, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "alice", Age: 21, Score: 40, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "A3-de-hi", Age: 22, Score: 70, Active: true, Meta: Meta{Country: "DE"}},
		{Name: "A4-us-mid", Age: 23, Score: 60, Active: false, Meta: Meta{Country: "US"}},
		{Name: "A5-pl-mid", Age: 24, Score: 50, Active: true, Meta: Meta{Country: "PL"}},
		{Name: "A6-nl-none", Age: 25, Score: 80, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 26, Score: 54, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "alice", Age: 27, Score: 53, Active: true, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "B1-nl-none", Age: 35, Score: 30, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 36, Score: 54, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "B3-de-low", Age: 37, Score: 48, Active: true, Meta: Meta{Country: "DE"}},
		{Name: "B4-us-low", Age: 38, Score: 20, Active: false, Meta: Meta{Country: "US"}},
		{Name: "B5-de-none", Age: 39, Score: 90, Active: false, Meta: Meta{Country: "DE"}},
		{Name: "B6-pl-hi", Age: 40, Score: 80, Active: false, Meta: Meta{Country: "PL"}},
		{Name: "alice", Age: 41, Score: 40, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 42, Score: 53, Active: true, Meta: Meta{Country: "FR"}},
	}

	q := qx.Query(
		qx.OR(
			qx.NOTIN("country", []string{"NL", "DE"}),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("score", 45.0),
			),
		),
	).Sort("age", qx.ASC).Offset(2).Limit(4)

	return ids, stateA, stateB, q
}

func queryExtStringOrderedORFixture() ([]string, []*Rec, []*Rec, *qx.QX) {
	ids := []string{"id-1", "id-2", "id-3", "id-4", "id-5", "id-6", "id-7"}

	stateA := []*Rec{
		{Name: "aa/00", Active: true, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/01", Active: false, Score: 40, Meta: Meta{Country: "FR"}},
		{Name: "aa/02", Active: true, Score: 70, Meta: Meta{Country: "NL"}},
		{Name: "ab/00", Active: false, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/01", Active: true, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
		{Name: "zz/00", Active: true, Score: 5, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "aa/00", Active: false, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/03", Active: true, Score: 20, Meta: Meta{Country: "FR"}},
		{Name: "aa/04", Active: true, Score: 40, Meta: Meta{Country: "DE"}},
		{Name: "ab/00", Active: true, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/02", Active: false, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ab/03", Active: true, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("name", "aa/"),
				qx.EQ("active", true),
			),
			qx.AND(
				qx.PREFIX("name", "ab/"),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "aa/03"),
				qx.LT("score", 25.0),
			),
		),
	).Sort("name", qx.ASC).Offset(1).Limit(3)

	return ids, stateA, stateB, q
}

func queryExtNoOrderORDisjointFixture() ([]uint64, []*Rec, []*Rec, *qx.QX) {
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	stateA := []*Rec{
		{Name: "A1-fr", Email: "cold/01", Active: false, Age: 20, Score: 20, Meta: Meta{Country: "FR"}},
		{Name: "A2-hot", Email: "hot/02", Active: false, Age: 21, Score: 30, Meta: Meta{Country: "US"}},
		{Name: "A3-miss", Email: "cold/03", Active: false, Age: 22, Score: 40, Meta: Meta{Country: "PL"}},
		{Name: "A4-fr", Email: "cold/04", Active: false, Age: 23, Score: 50, Meta: Meta{Country: "FR"}},
		{Name: "A5-off", Email: "cold/05", Active: false, Age: 45, Score: 90, Meta: Meta{Country: "NL"}},
		{Name: "A6-off", Email: "hot/06", Active: false, Age: 46, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "A7-off", Email: "cold/07", Active: false, Age: 47, Score: 70, Meta: Meta{Country: "NL"}},
		{Name: "A8-off", Email: "hot/08", Active: false, Age: 48, Score: 60, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "B1-off", Email: "cold/01", Active: false, Age: 20, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "B2-off", Email: "hot/02", Active: false, Age: 21, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "B3-off", Email: "cold/03", Active: false, Age: 22, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "B4-off", Email: "hot/04", Active: false, Age: 23, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "B5-nl", Email: "vip/05", Active: true, Age: 45, Score: 40, Meta: Meta{Country: "NL"}},
		{Name: "B6-nl", Email: "vip/06", Active: true, Age: 46, Score: 50, Meta: Meta{Country: "NL"}},
		{Name: "B7-nl", Email: "vip/07", Active: true, Age: 47, Score: 60, Meta: Meta{Country: "NL"}},
		{Name: "B8-nl", Email: "vip/08", Active: true, Age: 48, Score: 70, Meta: Meta{Country: "NL"}},
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "FR"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.PREFIX("email", "hot/"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("age", 45),
				qx.EQ("country", "NL"),
			),
		),
	).Offset(1).Limit(3)

	return ids, stateA, stateB, q
}

func queryExtRecSignature(rec *Rec) string {
	return queryContractRecSignature(rec)
}

func queryExtBuildSignatureCounts(items []*Rec) map[string]int {
	return queryContractBuildRecSignatureCounts(items)
}

func queryExtValidateNoOrderItemsAgainstFullSet(q *qx.QX, items []*Rec, fullSigCounts map[string]int, fullLen int) error {
	seen := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			return fmt.Errorf("nil item at i=%d", i)
		}
		sig := queryExtRecSignature(items[i])
		limit, ok := fullSigCounts[sig]
		if !ok {
			return fmt.Errorf("item %q outside full result set", sig)
		}
		seen[sig]++
		if seen[sig] > limit {
			return fmt.Errorf("duplicate item %q exceeds full-set multiplicity", sig)
		}
	}

	maxLen := fullLen
	if q.Window.Offset >= uint64(fullLen) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = fullLen - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	if len(items) > maxLen {
		return fmt.Errorf("items window overflow got=%d max=%d", len(items), maxLen)
	}
	return nil
}
