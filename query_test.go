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
	"go.etcd.io/bbolt"
)

type Meta struct {
	Country string `db:"country"`
}

type Rec struct {
	Meta

	Name   string   `db:"name"`
	Email  string   `db:"email"`
	Age    int      `db:"age"`
	Score  float64  `db:"score"`
	Active bool     `db:"active"`
	Tags   []string `db:"tags"`

	FullName string  `db:"full_name" json:"fullName"`
	Opt      *string `db:"opt"`
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
	db, err := New[K, V](raw, opts)
	if err != nil {
		_ = raw.Close()
		tb.Fatalf("New: %v", err)
	}
	return db, raw
}

func seedData(t *testing.T, db *DB[uint64, Rec], n int) []uint64 {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	r := newRand(1)
	ids := make([]uint64, 0, n)
	batchSize := 8192
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

func mustExtractAndLeaves(t testing.TB, e qx.Expr) []qx.Expr {
	t.Helper()
	leaves, ok := extractAndLeaves(e)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves failed: ok=%v len=%d", ok, len(leaves))
	}
	return leaves
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

// evalExprBool tries to implement the query logic to serve as a reference implementation
func evalExprBool(rec *Rec, e qx.Expr) (bool, error) {

	if e.Op == qx.OpNOOP {
		if e.Not {
			return false, nil
		}
		return true, nil
	}

	if e.Op == qx.OpAND || e.Op == qx.OpOR {
		if len(e.Operands) == 0 {
			return false, ErrInvalidQuery
		}
		if e.Op == qx.OpAND {
			out := true
			for _, ch := range e.Operands {
				b, err := evalExprBool(rec, ch)
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
			b, err := evalExprBool(rec, ch)
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

	fv := fieldValue(rec, e.Field)

	var out bool

	switch e.Op {
	case qx.OpEQ:
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
			out = ok2 && v == f
		case bool:
			b, ok2 := e.Value.(bool)
			out = ok2 && v == b
		case string:
			s, ok2 := e.Value.(string)
			out = ok2 && v == s
		case []string:
			return false, fmt.Errorf("test harness: EQ not defined for slice field %q", e.Field)
		default:
			return false, fmt.Errorf("test harness: unsupported EQ field type %T", fv)
		}

	case qx.OpIN:
		if e.Value == nil {
			out = false
			break
		}
		switch v := fv.(type) {
		case int:
			vals, ok2 := e.Value.([]int)
			if ok2 {
				out = false
				for _, x := range vals {
					if x == v {
						out = true
						break
					}
				}
				break
			}
			vals64, ok3 := e.Value.([]int64)
			if ok3 {
				out = false
				for _, x := range vals64 {
					if int64(v) == x {
						out = true
						break
					}
				}
				break
			}
			return false, fmt.Errorf("test harness: IN expects []int/[]int64 for int field")
		case string:
			vals, ok2 := e.Value.([]string)
			if !ok2 {
				return false, fmt.Errorf("test harness: IN expects []string for string field")
			}
			out = false
			for _, x := range vals {
				if x == v {
					out = true
					break
				}
			}
		default:
			return false, fmt.Errorf("test harness: unsupported IN field type %T", fv)
		}

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
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
			case qx.OpGT:
				out = int64(v) > i
			case qx.OpGTE:
				out = int64(v) >= i
			case qx.OpLT:
				out = int64(v) < i
			case qx.OpLTE:
				out = int64(v) <= i
			}
		case float64:
			f, ok2 := asFloat(e.Value)
			if !ok2 {
				return false, fmt.Errorf("test harness: bad float value for %v", e.Op)
			}
			switch e.Op {
			case qx.OpGT:
				out = v > f
			case qx.OpGTE:
				out = v >= f
			case qx.OpLT:
				out = v < f
			case qx.OpLTE:
				out = v <= f
			}
		case string:
			s, ok2 := asString(e.Value)
			if !ok2 {
				return false, fmt.Errorf("test harness: bad string value for %v", e.Op)
			}
			switch e.Op {
			case qx.OpGT:
				out = v > s
			case qx.OpGTE:
				out = v >= s
			case qx.OpLT:
				out = v < s
			case qx.OpLTE:
				out = v <= s
			}
		default:
			return false, fmt.Errorf("test harness: unsupported range field type %T", fv)
		}

	case qx.OpPREFIX:
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: PREFIX only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: PREFIX expects string")
		}
		out = strings.HasPrefix(s, p)

	case qx.OpSUFFIX:
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: SUFFIX only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: SUFFIX expects string")
		}
		out = strings.HasSuffix(s, p)

	case qx.OpCONTAINS:
		s, ok2 := asString(fv)
		if !ok2 {
			return false, fmt.Errorf("test harness: CONTAINS only for string fields")
		}
		p, ok3 := asString(e.Value)
		if !ok3 {
			return false, fmt.Errorf("test harness: CONTAINS expects string")
		}
		out = strings.Contains(s, p)

	case qx.OpHAS, qx.OpHASANY:
		arr, ok2 := fv.([]string)
		if !ok2 {
			return false, fmt.Errorf("test harness: %v only for []string in this test", e.Op)
		}
		if e.Value == nil {
			switch e.Op {
			case qx.OpHAS:
				return false, fmt.Errorf("HAS: no values provided")
			case qx.OpHASANY:
				out = false
			}
			break
		}
		needles, ok3 := e.Value.([]string)
		if !ok3 {
			return false, fmt.Errorf("test harness: %v expects []string", e.Op)
		}
		switch e.Op {
		case qx.OpHAS:
			out = containsAll(arr, needles)
		case qx.OpHASANY:
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
		ok, e := evalExprBool(v, q.Expr)
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
		o := q.Order[0]
		switch o.Type {
		case qx.OrderBasic:
			f := o.Field
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)

				switch xa := va.(type) {
				case int:
					xb := vb.(int)
					if xa == xb {
						return a.id < b.id
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				case float64:
					xb := vb.(float64)
					if xa == xb {
						return a.id < b.id
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				case string:
					xb := vb.(string)
					if xa == xb {
						return a.id < b.id
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				default:
					if o.Desc {
						return a.id > b.id
					}
					return a.id < b.id
				}
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case qx.OrderByArrayPos:
			f := o.Field
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

		case qx.OrderByArrayCount:
			f := o.Field
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
			return nil, fmt.Errorf("test harness: unknown order type %v", o.Type)
		}
	}

	off := int(q.Offset)
	if off > len(rows) {
		return nil, nil
	}
	rows = rows[off:]
	if q.Limit != 0 && int(q.Limit) < len(rows) {
		rows = rows[:int(q.Limit)]
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
		ok, e := evalExprBool(v, q.Expr)
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
		o := q.Order[0]
		switch o.Type {
		case qx.OrderBasic:
			f := o.Field
			less := func(a, b row) bool {
				va := fieldValue(a.rec, f)
				vb := fieldValue(b.rec, f)

				switch xa := va.(type) {
				case int:
					xb := vb.(int)
					if xa == xb {
						return a.idx < b.idx
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				case float64:
					xb := vb.(float64)
					if xa == xb {
						return a.idx < b.idx
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				case string:
					xb := vb.(string)
					if xa == xb {
						return a.idx < b.idx
					}
					if o.Desc {
						return xa > xb
					}
					return xa < xb
				default:
					if o.Desc {
						return a.idx > b.idx
					}
					return a.idx < b.idx
				}
			}
			sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })

		case qx.OrderByArrayPos:
			f := o.Field
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

		case qx.OrderByArrayCount:
			f := o.Field
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
			return nil, fmt.Errorf("test harness: unknown order type %v", o.Type)
		}
	}

	off := int(q.Offset)
	if off > len(rows) {
		return nil, nil
	}
	rows = rows[off:]
	if q.Limit != 0 && int(q.Limit) < len(rows) {
		rows = rows[:int(q.Limit)]
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
	if q.Offset >= uint64(len(full)) {
		maxLen = 0
	} else if q.Offset > 0 {
		maxLen = len(full) - int(q.Offset)
	}
	if q.Limit > 0 && int(q.Limit) < maxLen {
		maxLen = int(q.Limit)
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
			return qx.HAS("tags", pickVals(tagPool, 2))
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
			return []qx.Order{{Field: "score", Desc: r.IntN(2) == 0, Type: qx.OrderBasic}}
		case 2:
			return []qx.Order{{Field: "age", Desc: r.IntN(2) == 0, Type: qx.OrderBasic}}
		case 3:
			return []qx.Order{{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(tagPool, 3)}}
		default:
			if r.IntN(100) < 50 {
				return []qx.Order{{Field: "country", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(countries, 3)}}
			}
			return []qx.Order{{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayCount}}
		}
	}

	for step := 0; step < 180; step++ {
		q := &qx.QX{Expr: randomExpr()}
		mode := r.IntN(100)
		switch {
		case mode < 45:
			q.Order = randomOrder()
			q.Offset = uint64(r.IntN(120))
			q.Limit = uint64(20 + r.IntN(120))
		case mode < 65:
			q.Offset = uint64(r.IntN(180))
			q.Limit = uint64(10 + r.IntN(120))
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
		if len(q.Order) == 0 && (q.Offset > 0 || q.Limit > 0) {
			fullQ := cloneQuery(q)
			fullQ.Offset = 0
			fullQ.Limit = 0
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
		countQ.Offset = 0
		countQ.Limit = 0
		wantAll, err := expectedKeysString(t, db, countQ)
		if err != nil {
			t.Fatalf("step=%d expectedKeysString(all %+v): %v", step, q, err)
		}
		cnt, err := db.Count(q)
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

func assertPreparedRouteEquivalenceString(
	t testing.TB,
	db *DB[string, Rec],
	q *qx.QX,
) (nq *qx.QX, ref []string, usedExecution bool, usedPlanner bool) {
	t.Helper()

	nq = normalizeQueryForTest(q)
	if err := db.checkUsedFields(nq); err != nil {
		t.Fatalf("checkUsedFields(%+v): %v", nq, err)
	}

	ref, err := db.execPreparedQuery(nq)
	if err != nil {
		t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	assertNoDuplicateStringIDs(t, "prepared", ref)

	strictEqual := len(nq.Order) > 0 || (nq.Limit == 0 && nq.Offset == 0)
	var noOrderFull []string
	if !strictEqual {
		fullQ := cloneQuery(nq)
		fullQ.Offset = 0
		fullQ.Limit = 0

		noOrderFull, err = db.execPreparedQuery(fullQ)
		if err != nil {
			t.Fatalf("execPreparedQuery(full %+v): %v", fullQ, err)
		}
		assertNoOrderWindowSubsetString(t, nq, ref, noOrderFull, "prepared")
	}

	execOut, ok, err := db.tryExecutionPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateStringIDs(t, "execution", execOut)
		if strictEqual {
			if !queryStringIDsEqual(nq, ref, execOut) {
				want, werr := expectedKeysString(t, db, q)
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

	planOut, ok, err := db.tryPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateStringIDs(t, "planner", planOut)
		if strictEqual {
			if !queryStringIDsEqual(nq, ref, planOut) {
				want, werr := expectedKeysString(t, db, q)
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
		).By("age", qx.ASC).Max(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).By("full_name", qx.ASC).Max(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Max(140),
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
		).By("score", qx.DESC).Skip(40).Max(90),
		qx.Query(
			qx.GTE("age", 20),
		).ByArrayCount("tags", qx.DESC).Skip(7).Max(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Max(85),
		qx.Query(
			qx.HAS("tags", []string{"go", "db"}),
			qx.HASANY("tags", []string{"go", "ops"}),
		).By("score", qx.DESC).Skip(30).Max(70),
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
		if len(q.Order) == 0 && (q.Limit > 0 || q.Offset > 0) {
			fullQ := cloneQuery(q)
			fullQ.Offset = 0
			fullQ.Limit = 0
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

		qx.Query(qx.HAS("tags", []string{"go", "db"})),
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
		qx.Query(qx.GT("age", 20)).By("age", qx.ASC).Skip(5).Max(10),
		qx.Query().ByArrayCount("tags", qx.DESC),
		qx.Query().ByArrayPos("tags", []string{"go", "java"}, qx.ASC),
	)

	for i, q := range queries {
		q := q
		t.Run(fmt.Sprintf("q%d_%v", i, q.Expr.Op), func(t *testing.T) {
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
			qNoPage.Offset = 0
			qNoPage.Limit = 0

			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("expectedKeysUint64(all): %v", err)
			}

			cnt, err := db.Count(q)
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
		ok, e := evalExprBool(v, q.Expr)
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

	q := qx.Query(qx.EQ("age", 20)).By("age", qx.ASC)

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
	).Skip(10).Max(40)

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

func TestQuery_OrderVariants_BaseAndDelta_MatchSeqScanModel(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 320)

	// Force a clean base snapshot (without field deltas) to exercise base order paths.
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(base): %v", err)
	}

	runChecks := func(label string, queries []*qx.QX) {
		t.Helper()
		for i, q := range queries {
			q := q
			t.Run(fmt.Sprintf("%s_q%d_%v", label, i, q.Order[0].Type), func(t *testing.T) {
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
		qx.Query(qx.GTE("age", 20)).By("age", qx.ASC).Max(64),
		qx.Query(qx.GTE("age", 20)).By("age", qx.DESC).Skip(7).Max(41),

		// Base array order over slice field.
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayPos("tags", []string{"go", "java", "ops"}, qx.ASC).Skip(3).Max(33),
		qx.Query(qx.NOT(qx.EQ("active", true))).ByArrayCount("tags", qx.DESC).Skip(2).Max(37),

		// ArrayPos over scalar field to cover scalar ordering variants.
		qx.Query().ByArrayPos("country", []string{"NL", "DE", "PL"}, qx.ASC).Max(60),
		qx.Query().ByArrayPos("country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("base", baseQueries)

	// Introduce deltas for basic/slice/scalar order fields.
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
		Name:     "delta-user",
		Email:    "delta-user@example.com",
		Age:      37,
		Score:    77.7,
		Active:   true,
		Tags:     []string{"java", "ops"},
		FullName: "delta-user",
	}); err != nil {
		t.Fatalf("Set(delta): %v", err)
	}
	if err := db.Delete(4); err != nil {
		t.Fatalf("Delete(delta): %v", err)
	}

	deltaQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 20)).By("age", qx.ASC).Max(64),
		qx.Query(qx.GTE("age", 20)).By("age", qx.DESC).Skip(7).Max(41),
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayPos("tags", []string{"go", "java", "ops"}, qx.DESC).Skip(3).Max(33),
		qx.Query(qx.NOT(qx.EQ("active", true))).ByArrayCount("tags", qx.ASC).Skip(2).Max(37),
		qx.Query().ByArrayPos("country", []string{"NL", "DE", "PL"}, qx.ASC).Max(60),
		qx.Query().ByArrayPos("country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("delta", deltaQueries)
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
						return qx.Expr{Op: qx.OpAND, Operands: []qx.Expr{a, b}}
					}
					return qx.Expr{Op: qx.OpOR, Operands: []qx.Expr{a, b}}
				}
				return a
			}
			randomQuery := func() *qx.QX {
				q := &qx.QX{Expr: randomExpr()}
				switch r.IntN(4) {
				case 0:
					q.Order = []qx.Order{{Field: "age", Desc: r.IntN(2) == 0, Type: qx.OrderBasic}}
				case 1:
					q.Order = []qx.Order{
						{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(tagPool, 3)},
					}
				case 2:
					q.Order = []qx.Order{
						{Field: "country", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(countries, 3)},
					}
				default:
					q.Order = []qx.Order{{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayCount}}
				}

				switch r.IntN(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
					q.Offset = 0
					q.Limit = 0
				case 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
					q.Offset = uint64(350 + r.IntN(300))
					q.Limit = uint64(25 + r.IntN(60))
				default:
					q.Offset = uint64(r.IntN(45))
					q.Limit = uint64(1 + r.IntN(120))
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
							if q.Order[0].Type == qx.OrderByArrayPos && q.Order[0].Field == "tags" {
								gid := got[first]
								wid := want[first]
								gv, _ := db.Get(gid)
								wv, _ := db.Get(wid)
								vals, _ := q.Order[0].Data.([]string)
								var bits []string
								for _, tag := range vals {
									bm, owned := db.fieldLookupOwned("tags", tag, nil)
									gHas := bm != nil && bm.Contains(gid)
									wHas := bm != nil && bm.Contains(wid)
									if owned && bm != nil {
										releaseRoaringBuf(bm)
									}
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
				return qx.Expr{Op: qx.OpAND, Operands: []qx.Expr{a, b}}
			}
			return qx.Expr{Op: qx.OpOR, Operands: []qx.Expr{a, b}}
		}
		return a
	}
	randomQuery := func() *qx.QX {
		q := &qx.QX{Expr: randomExpr()}
		switch r.IntN(4) {
		case 0:
			q.Order = []qx.Order{{Field: "age", Desc: r.IntN(2) == 0, Type: qx.OrderBasic}}
		case 1:
			q.Order = []qx.Order{
				{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(tagPool, 3)},
			}
		case 2:
			q.Order = []qx.Order{
				{Field: "country", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayPos, Data: pickVals(countries, 3)},
			}
		default:
			q.Order = []qx.Order{{Field: "tags", Desc: r.IntN(2) == 0, Type: qx.OrderByArrayCount}}
		}
		q.Offset = uint64(r.IntN(40))
		if r.IntN(100) < 10 {
			q.Limit = 0
		} else {
			q.Limit = uint64(1 + r.IntN(120))
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
					var gl, wl int
					if gv != nil {
						gl = distinctCountStrings(gv.Tags)
					}
					if wv != nil {
						wl = distinctCountStrings(wv.Tags)
					}
					var gm, wm []string
					for _, tag := range []string{"rust", "java", "infra", "go", "db"} {
						bm, owned := db.fieldLookupOwned("tags", tag, nil)
						gHas := bm != nil && bm.Contains(gid)
						wHas := bm != nil && bm.Contains(wid)
						if owned && bm != nil {
							releaseRoaringBuf(bm)
						}
						gm = append(gm, fmt.Sprintf("%s=%v", tag, gHas))
						wm = append(wm, fmt.Sprintf("%s=%v", tag, wHas))
					}
					extra = fmt.Sprintf(
						"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v gotDistinctTags=%d tagMembership={%s}\nwantRec=%#v wantDistinctTags=%d tagMembership={%s}",
						first, gid, wid, gv, gl, strings.Join(gm, ","), wv, wl, strings.Join(wm, ","),
					)
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
			qNoPage.Offset = 0
			qNoPage.Limit = 0
			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("step=%d qi=%d expectedKeys(all): %v q=%+v", step, qi, err, q)
			}
			cnt, err := db.Count(q)
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

	baseQ := qx.Query(qx.EQ("active", false)).ByArrayPos("country", []string{"NL", "NL", "PL"}, qx.ASC).Skip(3).Max(120)
	check("base", baseQ)

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

	overlayQ := qx.Query(qx.EQ("active", false)).ByArrayPos("country", []string{"NL", "NL", "PL"}, qx.ASC).Skip(3).Max(120)
	check("overlay", overlayQ)
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
	if q.Offset >= uint64(len(full)) {
		maxLen = 0
	} else if q.Offset > 0 {
		maxLen = len(full) - int(q.Offset)
	}
	if q.Limit > 0 && int(q.Limit) < maxLen {
		maxLen = int(q.Limit)
	}
	if len(got) > maxLen {
		t.Fatalf("%s: no-order window overflow got=%d max=%d", label, len(got), maxLen)
	}
}

func assertPreparedRouteEquivalence(
	t testing.TB,
	db *DB[uint64, Rec],
	q *qx.QX,
) (nq *qx.QX, ref []uint64, usedExecution bool, usedPlanner bool) {
	t.Helper()

	nq = normalizeQueryForTest(q)
	if err := db.checkUsedFields(nq); err != nil {
		t.Fatalf("checkUsedFields(%+v): %v", nq, err)
	}

	ref, err := db.execPreparedQuery(nq)
	if err != nil {
		t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	assertNoDuplicateIDs(t, "prepared", ref)

	strictEqual := len(nq.Order) > 0 || (nq.Limit == 0 && nq.Offset == 0)
	var noOrderFull []uint64
	if !strictEqual {
		fullQ := cloneQuery(nq)
		fullQ.Offset = 0
		fullQ.Limit = 0

		noOrderFull, err = db.execPreparedQuery(fullQ)
		if err != nil {
			t.Fatalf("execPreparedQuery(full %+v): %v", fullQ, err)
		}
		assertNoOrderWindowSubset(t, nq, ref, noOrderFull, "prepared")
	}

	execOut, ok, err := db.tryExecutionPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateIDs(t, "execution", execOut)
		if strictEqual {
			if !queryIDsEqual(nq, ref, execOut) {
				if tt, ok := t.(*testing.T); ok {
					want, werr := expectedKeysUint64(tt, db, q)
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

	planOut, ok, err := db.tryPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryPlan(%+v): %v", nq, err)
	}
	if ok {
		assertNoDuplicateIDs(t, "planner", planOut)
		if strictEqual {
			if !queryIDsEqual(nq, ref, planOut) {
				if tt, ok := t.(*testing.T); ok {
					want, werr := expectedKeysUint64(tt, db, q)
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
	if q == nil {
		return nil
	}
	out := *q
	if len(q.Order) > 0 {
		out.Order = append([]qx.Order(nil), q.Order...)
	}
	return &out
}

func normalizeQueryForTest(q *qx.QX) *qx.QX {
	n := cloneQuery(q)
	n = normalizeQuery(n)
	return n
}

func wrapExprWithNoise(e qx.Expr, mode int) qx.Expr {
	switch mode % 4 {
	case 0:
		// e AND true
		return qx.Expr{
			Op: qx.OpAND,
			Operands: []qx.Expr{
				e,
				{Op: qx.OpNOOP},
			},
		}
	case 1:
		// e OR false
		return qx.Expr{
			Op: qx.OpOR,
			Operands: []qx.Expr{
				e,
				{Op: qx.OpNOOP, Not: true},
			},
		}
	case 2:
		// (true AND e) AND true
		return qx.Expr{
			Op: qx.OpAND,
			Operands: []qx.Expr{
				{
					Op: qx.OpAND,
					Operands: []qx.Expr{
						{Op: qx.OpNOOP},
						e,
					},
				},
				{Op: qx.OpNOOP},
			},
		}
	default:
		// e OR (false OR false)
		return qx.Expr{
			Op: qx.OpOR,
			Operands: []qx.Expr{
				e,
				{
					Op: qx.OpOR,
					Operands: []qx.Expr{
						{Op: qx.OpNOOP, Not: true},
						{Op: qx.OpNOOP, Not: true},
					},
				},
			},
		}
	}
}

func withNoisyEquivalentQuery(q *qx.QX, noiseMode int) *qx.QX {
	out := cloneQuery(q)
	out.Expr = wrapExprWithNoise(q.Expr, noiseMode)
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
			).By("score", qx.DESC).Skip(300).Max(120),
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
			).By("score", qx.DESC).Max(80),
		},
		{
			name: "OrderRange",
			q: qx.Query(
				qx.GTE("age", 25),
				qx.LTE("age", 40),
				qx.GT("score", 20.0),
			).By("score", qx.DESC).Skip(100).Max(90),
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
	batchSize := 8192
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
		return qx.HAS("tags", pickVals(tagPool, 2))
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
	return qx.Expr{
		Op:       qx.OpAND,
		Operands: ops,
	}
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
			return []qx.Order{
				{
					Field: "score",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderBasic,
				},
			}
		case 2:
			return []qx.Order{
				{
					Field: "age",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderBasic,
				},
			}
		case 3:
			return []qx.Order{
				{
					Field: "tags",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderByArrayPos,
					Data:  pickVals(tagPool, 3),
				},
			}
		default:
			if r.IntN(100) < 50 {
				return []qx.Order{
					{
						Field: "country",
						Desc:  r.IntN(2) == 0,
						Type:  qx.OrderByArrayPos,
						Data:  pickVals(countries, 3),
					},
				}
			}
			return []qx.Order{
				{
					Field: "tags",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderByArrayCount,
				},
			}
		}
	}

	var expr qx.Expr
	if r.IntN(100) < 35 {
		branchN := 2 + r.IntN(2)
		branches := make([]qx.Expr, 0, branchN)
		for i := 0; i < branchN; i++ {
			branches = append(branches, randomMetamorphicAndExpr(r, 1+r.IntN(3)))
		}
		expr = qx.Expr{
			Op:       qx.OpOR,
			Operands: branches,
		}
	} else {
		expr = randomMetamorphicAndExpr(r, 1+r.IntN(4))
	}

	q := &qx.QX{
		Expr: expr,
	}
	mode := r.IntN(100)
	if mode < 45 {
		// paged + ordered
		q.Order = pickOrder()
		q.Offset = uint64(r.IntN(120))
		q.Limit = uint64(20 + r.IntN(120))
		if r.IntN(100) < 25 {
			q.Offset = uint64(250 + r.IntN(400))
		}
	} else if mode < 65 {
		// paged + no-order (window invariants should hold across routes)
		q.Offset = uint64(r.IntN(180))
		q.Limit = uint64(10 + r.IntN(120))
		if r.IntN(100) < 20 {
			q.Offset = uint64(250 + r.IntN(350))
		}
	} else if mode < 80 {
		// ordered + unpaged
		q.Order = pickOrder()
		q.Offset = 0
		q.Limit = 0
	} else {
		// no-order + unpaged baseline
		q.Offset = 0
		q.Limit = 0
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
				countQ.Offset = 0
				countQ.Limit = 0

				wantCountKeys, err := expectedKeysUint64(t, db, countQ)
				if err != nil {
					t.Fatalf(
						"expectedKeysUint64(count profile=%s i=%d): %v\nq=%+v",
						p.name, i, err, q,
					)
				}
				wantCount := uint64(len(wantCountKeys))

				gotCount, err := db.Count(q)
				if err != nil {
					t.Fatalf("Count(profile=%s i=%d): %v\nq=%+v", p.name, i, err, q)
				}
				if gotCount != wantCount {
					t.Fatalf(
						"count mismatch (profile=%s i=%d): got=%d want=%d\nq=%+v\ncountQ=%+v",
						p.name, i, gotCount, wantCount, q, countQ,
					)
				}

				preparedCount, err := db.countPreparedExpr(normalizeQueryForTest(q).Expr)
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
	).By("score", qx.ASC).Skip(446).Max(70)
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
			).By("score", qx.ASC).Skip(210).Max(90),
		},
		{
			name: "desc_order",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).By("score", qx.DESC).Skip(446).Max(70),
		},
		{
			name: "without_offset",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).By("score", qx.ASC).Max(70),
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
				qx.HAS("tags", []string{"db", "rust"}),
				qx.HAS("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Max(120),
		},
		{
			name: "no_order_offset_limit",
			q: qx.Query(
				qx.HAS("tags", []string{"db", "rust"}),
				qx.HAS("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Skip(40).Max(80),
		},
		{
			name: "ordered_offset_limit",
			q: qx.Query(
				qx.HAS("tags", []string{"db", "rust"}),
				qx.HAS("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).By("age", qx.ASC).Skip(20).Max(90),
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

			expr, _ := normalizeExpr(tc.q.Expr)
			cntPred, ok, err := db.tryCountByPredicates(expr)
			if err != nil {
				t.Fatalf("tryCountByPredicates: %v", err)
			}
			if !ok {
				t.Fatalf("expected tryCountByPredicates fast-path for query shape")
			}

			countQ := cloneQuery(tc.q)
			countQ.Order = nil
			countQ.Offset = 0
			countQ.Limit = 0
			wantCountKeys, err := expectedKeysUint64(t, db, countQ)
			if err != nil {
				t.Fatalf("expectedKeysUint64(count): %v", err)
			}
			wantCount := uint64(len(wantCountKeys))

			cnt, err := db.Count(tc.q)
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
				qx.HAS("tags", []string{"db", "rust"}),
				qx.HAS("tags", []string{"db", "infra"}),
				qx.SUFFIX("country", "land"),
			),
			qx.AND(
				qx.HAS("tags", []string{"go", "db"}),
				qx.PREFIX("full_name", "FN-1"),
			),
		),
	)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	expr, _ := normalizeExpr(q.Expr)
	cntFast, ok, err := db.tryCountORByPredicates(expr)
	if err != nil {
		t.Fatalf("tryCountORByPredicates: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryCountORByPredicates fast-path for OR shape")
	}
	if cntFast != uint64(len(want)) {
		t.Fatalf("tryCountORByPredicates mismatch: got=%d want=%d", cntFast, len(want))
	}

	cnt, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(want))
	}
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_BaseAndDelta(t *testing.T) {
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
		).By("age", qx.ASC).Max(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).By("full_name", qx.ASC).Max(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Max(140),
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
		).By("score", qx.DESC).Skip(40).Max(90),
		qx.Query(
			qx.GTE("age", 20),
		).ByArrayCount("tags", qx.DESC).Skip(7).Max(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Max(85),
		qx.Query(
			qx.GTE("age", 24),
			qx.LTE("age", 42),
			qx.EQ("active", true),
		).Max(95),
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

			if len(q.Order) == 0 && (q.Limit > 0 || q.Offset > 0) {
				fullQ := cloneQuery(q)
				fullQ.Offset = 0
				fullQ.Limit = 0
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

			if q.Limit == 0 && q.Offset == 0 {
				cnt, err := db.countPreparedExpr(nq.Expr)
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
				t.Fatalf("delta Set(id=%d): %v", id, err)
			}
		case 1:
			patch := []Field{{Name: "age", Value: float64(18 + r.IntN(65))}}
			if err := db.Patch(id, patch); err != nil {
				t.Fatalf("delta Patch(id=%d): %v", id, err)
			}
		default:
			if err := db.Delete(id); err != nil {
				t.Fatalf("delta Delete(id=%d): %v", id, err)
			}
		}
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected active snapshot delta after mutation phase")
	}

	runChecks("delta")
}
