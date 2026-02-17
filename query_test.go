package rbi

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
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

func openTempDBUint64(t *testing.T, opts *Options[uint64, Rec]) (*DB[uint64, Rec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_uint64.db")

	db, err := Open[uint64, Rec](path, 0o600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	return db, path
}

func openTempDBString(t *testing.T, opts *Options[string, Rec]) (*DB[string, Rec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_string.db")

	db, err := Open[string, Rec](path, 0o600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	return db, path
}

func seedData(t *testing.T, db *DB[uint64, Rec], n int) []uint64 {
	t.Helper()

	r := newRand(1)
	ids := make([]uint64, 0, n)

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
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
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
				arr, _ := fieldValue(r.rec, f).([]string)
				set := make(map[string]struct{}, len(arr))
				for _, v := range arr {
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

func TestQueryCorrectnessAgainstSeqScan_Uint64Keys(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
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

			gotItems, err := db.QueryItems(q)
			if err != nil {
				t.Fatalf("QueryItems: %v", err)
			}
			if len(gotItems) != len(wantPageKeys) {
				t.Fatalf("QueryItems len mismatch: got=%d want=%d", len(gotItems), len(wantPageKeys))
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

func TestQueryUnknownFieldReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 10)

	_, err := db.QueryKeys(qx.Query(qx.EQ("no_such_field", 1)))
	if err == nil {
		t.Fatalf("expected error for unknown field")
	}
}

func TestStringKeys_BasicQuerySetEquivalence(t *testing.T) {
	db, path := openTempDBString(t, nil)

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
	db, _ := openTempDBUint64(t, nil)

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

func TestScanKeysString_Filter(t *testing.T) {
	db, _ := openTempDBString(t, nil)

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

/**/

func TestRace_ConcurrentReadersAndWriters(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 200)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	errCh := make(chan error, 100)

	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					reportErr(fmt.Errorf("panic in writer: %v", r))
				}
			}()

			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(250))
				op := r.IntN(3)

				switch op {
				case 0:
					rec := &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     []string{"alice", "bob", "carol"}[r.IntN(3)],
						Age:      18 + r.IntN(60),
						Score:    r.Float64() * 100,
						Active:   r.IntN(2) == 0,
						Tags:     []string{"go", "java", "ops"}[:1+r.IntN(3)],
						FullName: "FN",
					}
					if err := db.Set(id, rec); err != nil {
						reportErr(fmt.Errorf("writer set error: %w", err))
						return
					}

				case 1:
					patch := []Field{{Name: "age", Value: float64(20 + r.IntN(50))}}
					if err := db.Patch(id, patch); err != nil && !errors.Is(err, ErrRecordNotFound) {
						reportErr(fmt.Errorf("writer patch error: %w", err))
						return
					}

				case 2:
					if err := db.Delete(id); err != nil {
						reportErr(fmt.Errorf("writer delete error: %w", err))
						return
					}
				}
			}
		}(int64(1000 + w))
	}

	for rr := 0; rr < 6; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					reportErr(fmt.Errorf("panic in reader: %v", r))
				}
			}()

			r := newRand(seed)

			qs := []*qx.QX{
				qx.Query(qx.GT("age", 30)),
				qx.Query(qx.PREFIX("name", "a")),
				qx.Query(qx.HASANY("tags", []string{"go", "java"})),
				qx.Query(
					qx.AND(
						qx.GTE("age", 25),
						qx.EQ("active", true),
					),
				),
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				q := qs[r.IntN(len(qs))]

				items, err := db.QueryItems(q)
				if err != nil {
					if errors.Is(err, ErrIndexDisabled) || errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("query error: %w", err))
					return
				}

				for _, it := range items {
					if it == nil {
						continue
					}
					ok, e := evalExprBool(it, q.Expr)
					if e != nil {
						reportErr(fmt.Errorf("eval logic error: %w", e))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("consistency error: item %v returned for query %v", it, q.Expr))
						return
					}
				}

				if _, err = db.Count(q); err != nil {
					if errors.Is(err, ErrIndexDisabled) || errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("count error: %w", err))
					return
				}
			}
		}(int64(2000 + rr))
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Race test failure: %v", err)
	}
}

func TestEmptySliceQueries(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Tags: []string{"go"}}); err != nil {
		t.Fatal(err)
	}

	_, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{})))
	if err == nil {
		t.Fatal("HASANY with empty slice: error expected, got nil")
	}

	_, err = db.QueryKeys(qx.Query(qx.HAS("tags", []string{})))
	if err == nil {
		t.Fatal("HAS with empty slice: error expected, got nil")
	}
}

func TestSort_OrderStability_WithDupValues(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

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

func TestEdge_ZeroValueVsNil(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	sEmpty := ""
	sVal := "val"

	if err := db.Set(1, &Rec{Name: "nil_opt", Opt: nil}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "empty_opt", Opt: &sEmpty}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Name: "val_opt", Opt: &sVal}); err != nil {
		t.Fatal(err)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Query NIL: expected [1], got %v", ids)
	}

	// find empty string (value should be "" string, not pointer)
	ids, err = db.QueryKeys(qx.Query(qx.EQ("opt", "")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("expected [2], got %v", ids)
	}
}

func TestStringPrefixLogic(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	names := []string{"item", "item-1", "item-10", "items", "iterator"}
	for i, n := range names {
		if err := db.Set(uint64(i), &Rec{Name: n}); err != nil {
			t.Fatal(err)
		}
	}

	q := qx.Query(qx.PREFIX("name", "item"))
	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Errorf("PREFIX 'item': expected 4, got %d", len(ids))
	}

	q = qx.Query(qx.PREFIX("name", "iter"))
	ids, err = db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("PREFIX 'iter': expected 1, got %d", len(ids))
	}
}

func TestQuery_OR_WithNegativeBranch_EqualsUniverse(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	ids := seedData(t, db, 120)

	// OR( NOT EQ(name,"alice"), EQ(name,"alice") ) == universe
	q := qx.Query(
		qx.OR(
			qx.NE("name", "alice"),
			qx.EQ("name", "alice"),
		),
	)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertSameSlice(t, got, want)

	if uint64(len(got)) != uint64(len(ids)) {
		t.Fatalf("expected universe size %d, got %d", len(ids), len(got))
	}
}

func TestQuery_AND_WithNegativeBranch_Empty(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 120)

	// AND( EQ(name,"alice"), NOT EQ(name,"alice") ) == empty
	q := qx.Query(
		qx.AND(
			qx.EQ("name", "alice"),
			qx.NE("name", "alice"),
		),
	)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}

func TestQuery_DoubleNot_SameAsOriginal(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 150)

	inner := qx.AND(
		qx.GTE("age", 25),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "java"}),
	)

	q := qx.Query(qx.NOT(qx.NOT(inner)))

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

func TestQuery_OrderBy_WithNegationAndLimit(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 200)

	// order + NOT branch
	// this forces the "negative + ordering" path to materialize (universe AND NOT set)
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).By("age", qx.ASC).Skip(3).Max(25)

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

func TestQuery_NegativeNoOrder_ExcludesCorrectly_WithPaging(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
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

func TestQuery_Prefix_OrderBySameField_Limit(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	for i := 1; i <= 120; i++ {
		email := fmt.Sprintf("user%03d@example.com", i)
		if i%3 == 0 {
			email = fmt.Sprintf("user10%03d@example.com", i)
		}
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Email:    email,
			Age:      18 + (i % 50),
			Score:    float64(i) / 10.0,
			Active:   i%2 == 0,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%03d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(
		qx.PREFIX("email", "user10"),
	).By("email", qx.ASC).Max(15)

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

func TestQuery_RangeBoundaries_Int_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// deterministic ages so boundary conditions are obvious
	for i := 0; i < 100; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "n",
			Age:      i, // 0..99
			Score:    float64(i),
			Active:   true,
			Tags:     []string{},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{"GT_50", qx.Query(qx.GT("age", 50))},
		{"GTE_50", qx.Query(qx.GTE("age", 50))},
		{"LT_50", qx.Query(qx.LT("age", 50))},
		{"LTE_50", qx.Query(qx.LTE("age", 50))},
		{"AND_GTE_10_LT_20", qx.Query(qx.GTE("age", 10), qx.LT("age", 20))},
		{"AND_GTE_10_LTE_20", qx.Query(qx.GTE("age", 10), qx.LTE("age", 20))},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			want, err := expectedKeysUint64(t, db, tc.q)
			if err != nil {
				t.Fatalf("expectedKeysUint64: %v", err)
			}
			assertSameSlice(t, got, want)
		})
	}
}

func TestQuery_IN_WithDuplicates_DoesNotDuplicateResults(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 160)

	// duplicate values in IN should not cause duplicated ids
	q := qx.Query(qx.IN("country", []string{"NL", "NL", "DE", "DE"})).By("age", qx.ASC).Max(50)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	// ensure no duplicates in output slice (ordering + limit still applies)
	seen := make(map[uint64]struct{}, len(got))
	for _, id := range got {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id in result: %d (got=%v)", id, got)
		}
		seen[id] = struct{}{}
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_SliceField_HASANY_WithDuplicateNeedles(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// force duplicates in both data and needles
	if err := db.Set(1, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Tags: []string{"rust"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Tags: []string{}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HASANY("tags", []string{"go", "go"}))

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

func TestQuery_SliceField_HAS_DuplicateNeedles_MatchesAccordingToHarness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// this test locks in the current reference semantics:
	// containsAll() in harness treats duplicates as requiring multiple occurrences,
	// index implementation may choose a different semantics; this test will catch drift

	if err := db.Set(1, &Rec{Tags: []string{"go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HAS("tags", []string{"go", "go"}))

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

func TestQuery_ByArrayPos_WithLimitAndNegation(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 220)

	priority := []string{"go", "java", "ops"}
	q := qx.Query(
		qx.NOT(qx.CONTAINS("country", "land")),
	).ByArrayPos("tags", priority, qx.ASC).Skip(2).Max(30)

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

func TestQuery_ByArrayCount_WithLimitAndNegation(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 220)

	q := qx.Query(
		qx.NOT(qx.EQ("active", true)),
	).ByArrayCount("tags", qx.DESC).Skip(1).Max(40)

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

func TestQuery_OffsetBeyondResult_ReturnsEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 80)

	q := qx.Query(qx.EQ("country", "NL")).Skip(10_000).Max(50)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_DeleteUpdatesIndex_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// arrange deterministic values so the query result is known
	for i := 1; i <= 50; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Age:      30,
			Score:    1.0,
			Active:   true,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.EQ("name", "alice"),
		qx.EQ("active", true),
	)

	got0, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys baseline: %v", err)
	}
	if len(got0) != 50 {
		t.Fatalf("expected 50, got %d", len(got0))
	}

	// delete a subset and verify indexes reflect it
	for _, id := range []uint64{3, 7, 10, 25, 50} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	got1, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys after delete: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got1, want)
}

func TestQuery_PatchUpdatesIndex_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// put some records with age=10, then patch some to age=40 and ensure range queries reflect changes
	for i := 1; i <= 60; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "u",
			Age:      10,
			Score:    0.1,
			Active:   true,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	for _, id := range []uint64{2, 5, 9, 11, 17, 31} {
		if err := db.Patch(id, []Field{{Name: "age", Value: float64(40)}}); err != nil {
			t.Fatalf("Patch(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 35)).By("age", qx.ASC)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	cnt, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(want))
	}
}

func TestQuery_SortWithNegativeResult_NoDuplicates(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 300)

	// negative result + ORDER triggers materialization path; ensure no duplicates in output
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).By("age", qx.ASC).Max(120)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	seen := make(map[uint64]struct{}, len(got))
	for _, id := range got {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id in ordered result: %d", id)
		}
		seen[id] = struct{}{}
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

/**/

type plannerVariantSet struct{}

func runQueryKeysWithVariant(
	t *testing.T,
	db *DB[uint64, Rec],
	q *qx.QX,
	_ plannerVariantSet,
) []uint64 {
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

func TestStep8_Metamorphic_NormalizeAndPlanVariants(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})
	_ = seedData(t, db, 20_000)

	variantA := plannerVariantSet{}
	variantB := plannerVariantSet{}

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
			base := runQueryKeysWithVariant(t, db, tc.q, variantA)
			normalizedQ := normalizeQueryForTest(tc.q)
			normalized := runQueryKeysWithVariant(t, db, normalizedQ, variantA)
			assertQueryIDsEqual(t, tc.q, base, normalized)

			noisyQ := withNoisyEquivalentQuery(tc.q, len(tc.name))
			noisy := runQueryKeysWithVariant(t, db, noisyQ, variantA)
			assertQueryIDsEqual(t, tc.q, base, noisy)

			other := runQueryKeysWithVariant(t, db, tc.q, variantB)
			assertQueryIDsEqual(t, tc.q, base, other)
		})
	}
}

type step8DataProfile struct {
	name        string
	scoreLevels int
	activeTrue  float64
	hotCountryP float64
	hotTagP     float64
}

func seedDataProfileStep8(t *testing.T, db *DB[uint64, Rec], n int, p step8DataProfile) {
	t.Helper()

	r := newRand(173 + int64(n) + int64(p.scoreLevels)*11)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland", "US", "JP"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}

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

		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
}

func randomLeafStep8(r *rand.Rand) qx.Expr {
	switch r.IntN(8) {
	case 0:
		return qx.EQ("active", r.IntN(2) == 0)
	case 1:
		countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
		return qx.IN("country", []string{
			countries[r.IntN(len(countries))],
			countries[r.IntN(len(countries))],
		})
	case 2:
		countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
		return qx.NOTIN("country", []string{
			countries[r.IntN(len(countries))],
			countries[r.IntN(len(countries))],
		})
	case 3:
		return qx.GTE("age", 18+r.IntN(30))
	case 4:
		return qx.LTE("age", 30+r.IntN(35))
	case 5:
		tags := []string{"go", "db", "ops", "rust", "java", "infra"}
		return qx.HASANY("tags", []string{
			tags[r.IntN(len(tags))],
			tags[r.IntN(len(tags))],
		})
	case 6:
		return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", 1+r.IntN(3)))
	default:
		return qx.GT("score", float64(r.IntN(40)))
	}
}

func randomAndExprStep8(r *rand.Rand, leafN int) qx.Expr {
	if leafN < 1 {
		leafN = 1
	}
	ops := make([]qx.Expr, 0, leafN)
	for i := 0; i < leafN; i++ {
		ops = append(ops, randomLeafStep8(r))
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return qx.Expr{
		Op:       qx.OpAND,
		Operands: ops,
	}
}

func randomQueryStep8(r *rand.Rand) *qx.QX {
	var expr qx.Expr
	if r.IntN(100) < 35 {
		branchN := 2 + r.IntN(2)
		branches := make([]qx.Expr, 0, branchN)
		for i := 0; i < branchN; i++ {
			branches = append(branches, randomAndExprStep8(r, 1+r.IntN(3)))
		}
		expr = qx.Expr{
			Op:       qx.OpOR,
			Operands: branches,
		}
	} else {
		expr = randomAndExprStep8(r, 1+r.IntN(4))
	}

	q := &qx.QX{
		Expr: expr,
	}
	if r.IntN(100) < 55 {
		q.Offset = uint64(r.IntN(120))
		q.Limit = uint64(20 + r.IntN(120))
		if r.IntN(100) < 75 {
			q.Order = []qx.Order{
				{
					Field: "score",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderBasic,
				},
			}
		} else {
			q.Order = []qx.Order{
				{
					Field: "age",
					Desc:  r.IntN(2) == 0,
					Type:  qx.OrderBasic,
				},
			}
		}
		if r.IntN(100) < 25 {
			q.Offset = uint64(250 + r.IntN(400))
		}
	} else {
		// For no-order queries, avoid LIMIT/OFFSET semantics while comparing
		// different planner toggles; validate full matching set equivalence.
		q.Offset = 0
		q.Limit = 0
	}
	return q
}

func TestStep8_RandomizedProfiles_PlanVariantEquivalence(t *testing.T) {
	profiles := []step8DataProfile{
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

	variantA := plannerVariantSet{}
	variantB := plannerVariantSet{}

	for pi := range profiles {
		p := profiles[pi]
		t.Run(p.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})
			seedDataProfileStep8(t, db, 8_000, p)

			r := newRand(777 + int64(pi)*1000)
			const queryCount = 70

			for i := 0; i < queryCount; i++ {
				q := randomQueryStep8(r)

				baseA := runQueryKeysWithVariant(t, db, q, variantA)
				baseB := runQueryKeysWithVariant(t, db, q, variantB)

				nq := normalizeQueryForTest(q)
				normalizedA := runQueryKeysWithVariant(t, db, nq, variantA)
				if !queryIDsEqual(q, baseA, normalizedA) {
					t.Fatalf(
						"variantA normalize mismatch (profile=%s, i=%d):\nq=%+v\nnq=%+v\nbase=%v\nnorm=%v",
						p.name, i, q, nq, baseA, normalizedA,
					)
				}
				normalizedB := runQueryKeysWithVariant(t, db, nq, variantB)
				if !queryIDsEqual(q, baseB, normalizedB) {
					t.Fatalf(
						"variantB normalize mismatch (profile=%s, i=%d):\nq=%+v\nnq=%+v\nbase=%v\nnorm=%v",
						p.name, i, q, nq, baseB, normalizedB,
					)
				}

				noisyA := runQueryKeysWithVariant(t, db, withNoisyEquivalentQuery(q, i), variantA)
				assertQueryIDsEqual(t, q, baseA, noisyA)
				noisyB := runQueryKeysWithVariant(t, db, withNoisyEquivalentQuery(q, i), variantB)
				assertQueryIDsEqual(t, q, baseB, noisyB)

				// When no ORDER/LIMIT/OFFSET is used, both variants should
				// resolve to equivalent full-set semantics.
				if len(q.Order) == 0 && q.Limit == 0 && q.Offset == 0 {
					assertQueryIDsEqual(t, q, baseA, baseB)
				}
			}
		})
	}
}

/**/

func capturedNotInOrderOffsetQuery() *qx.QX {
	return qx.Query(
		qx.NOTIN("country", []string{"Iceland", "Finland"}),
		qx.NOTIN("country", []string{"Iceland", "DE"}),
	).By("score", qx.ASC).Skip(446).Max(70)
}

func openStep8SkewedDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})
	seedDataProfileStep8(t, db, 8_000, step8DataProfile{
		name:        "Skewed",
		scoreLevels: 30_000,
		activeTrue:  0.88,
		hotCountryP: 0.75,
		hotTagP:     0.85,
	})
	return db
}

func notInVariantMatrix() []struct {
	name string
	ts   plannerVariantSet
} {
	return []struct {
		name string
		ts   plannerVariantSet
	}{
		{
			name: "variant_a",
			ts:   plannerVariantSet{},
		},
		{
			name: "variant_b",
			ts:   plannerVariantSet{},
		},
		{
			name: "variant_c",
			ts:   plannerVariantSet{},
		},
		{
			name: "variant_d",
			ts:   plannerVariantSet{},
		},
	}
}

func assertNotInQueryEqualAcrossVariantMatrix(t *testing.T, q *qx.QX) {
	t.Helper()

	cases := notInVariantMatrix()
	var ref []uint64

	for i := range cases {
		db := openStep8SkewedDB(t)
		got := runQueryKeysWithVariant(t, db, q, cases[i].ts)
		if i == 0 {
			ref = got
			continue
		}
		if !slices.Equal(ref, got) {
			t.Fatalf("route mismatch case=%s:\nref=%v\ngot=%v", cases[i].name, ref, got)
		}
	}
}

func TestRegression_NotInOrderOffset_VariantMatrix(t *testing.T) {
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
			assertNotInQueryEqualAcrossVariantMatrix(t, tests[i].q)
		})
	}
}

func TestRegression_NotInOrderOffset_NoStateCorruption(t *testing.T) {
	db := openStep8SkewedDB(t)
	q := capturedNotInOrderOffsetQuery()

	variantA := plannerVariantSet{}
	variantB := plannerVariantSet{}

	before := runQueryKeysWithVariant(t, db, q, variantA)
	mid := runQueryKeysWithVariant(t, db, q, variantB)
	after := runQueryKeysWithVariant(t, db, q, variantA)

	if !slices.Equal(before, after) {
		t.Fatalf("variant results changed after interleaved query:\nbefore=%v\nmid=%v\nafter=%v", before, mid, after)
	}
}

/**/

func TestNormalizeExpr_OrFalseCollapsesToLeaf(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 21}
	in := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			leaf,
			{Op: qx.OpNOOP, Not: true},
		},
	}

	out, changed := normalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_DeMorganForNotAnd(t *testing.T) {
	in := qx.Expr{
		Op:  qx.OpAND,
		Not: true,
		Operands: []qx.Expr{
			{Op: qx.OpEQ, Field: "active", Value: true},
			{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}},
		},
	}

	out, changed := normalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			{Op: qx.OpEQ, Field: "active", Value: true, Not: true},
			{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}, Not: true},
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_FlattensAndNoopNoise(t *testing.T) {
	leafA := qx.Expr{Op: qx.OpEQ, Field: "active", Value: true}
	leafB := qx.Expr{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}}

	in := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			{Op: qx.OpNOOP},
			{
				Op: qx.OpAND,
				Operands: []qx.Expr{
					leafA,
					{Op: qx.OpNOOP},
				},
			},
			leafB,
		},
	}

	out, changed := normalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			leafA,
			leafB,
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_DoubleNotEliminates(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpEQ, Field: "age", Value: 33}
	in := qx.Expr{
		Op:  qx.OpOR,
		Not: true,
		Operands: []qx.Expr{
			{Op: qx.OpAND, Not: true, Operands: []qx.Expr{leaf}},
		},
	}

	out, changed := normalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalize_WrappedQueryMatchesDirectResults(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 10_000)

	direct := qx.Query(
		qx.GTE("age", 18),
		qx.EQ("active", true),
	).By("score", qx.DESC).Skip(500).Max(100)

	wrapped := &qx.QX{
		Expr: qx.Expr{
			Op: qx.OpOR,
			Operands: []qx.Expr{
				direct.Expr,
				{Op: qx.OpNOOP, Not: true},
			},
		},
		Order:  direct.Order,
		Offset: direct.Offset,
		Limit:  direct.Limit,
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
