package rbi

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
)

func TestRace_ConcurrentReadersAndWriters(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
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
					if err := db.Patch(id, patch); err != nil {
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

				items, err := db.Query(q)
				if err != nil {
					if errors.Is(err, ErrClosed) {
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
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("count error: %w", err))
					return
				}
			}
		}(int64(2000 + rr))
	}

	time.Sleep(250 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Race test failure: %v", err)
	}
}

func TestRace_ConcurrentReadersAndWriters_SnapshotDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
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

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(260))
				switch r.IntN(3) {
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
					if err := db.Patch(id, patch); err != nil {
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
		}(int64(3100 + w))
	}

	for rr := 0; rr < 6; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			qs := []*qx.QX{
				qx.Query(qx.GT("age", 30)),
				qx.Query(qx.PREFIX("name", "a")),
				qx.Query(qx.HASANY("tags", []string{"go", "java"})),
				qx.Query(qx.AND(qx.GTE("age", 25), qx.EQ("active", true))),
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				q := qs[r.IntN(len(qs))]
				items, err := db.Query(q)
				if err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("query items error: %w", err))
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

				ids, err := db.QueryKeys(q)
				if err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("query keys error: %w", err))
					return
				}
				_ = ids

				if _, err = db.Count(q); err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("count error: %w", err))
					return
				}

				seen := 0
				if err = db.ScanKeys(0, func(_ uint64) (bool, error) {
					seen++
					return seen < 64, nil
				}); err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("scan keys error: %w", err))
					return
				}
			}
		}(int64(4100 + rr))
	}

	time.Sleep(250 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Race test failure: %v", err)
	}
}

func TestRace_ConcurrentWriters_SnapshotRouteEquivalence(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_200)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	var errMu sync.Mutex
	var firstErr error
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	loadErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()

			r := newRand(seed)
			randomRec := func(id uint64) *Rec {
				name := names[r.IntN(len(names))]
				return &Rec{
					Meta:     Meta{Country: countries[r.IntN(len(countries))]},
					Name:     name,
					Email:    fmt.Sprintf("%s-%d@example.test", name, id),
					Age:      18 + r.IntN(65),
					Score:    float64(r.IntN(2_000))/10.0 + r.Float64()*0.001,
					Active:   r.IntN(2) == 0,
					Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
					FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(20_000)),
				}
			}
			randomPatch := func() []Field {
				switch r.IntN(5) {
				case 0:
					return []Field{{Name: "age", Value: float64(18 + r.IntN(65))}}
				case 1:
					return []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
				case 2:
					return []Field{{Name: "active", Value: r.IntN(2) == 0}}
				case 3:
					return []Field{{Name: "name", Value: names[r.IntN(len(names))]}}
				default:
					return []Field{{Name: "tags", Value: []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]}}}
				}
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(16_000))
				switch r.IntN(3) {
				case 0:
					reportErr(db.Set(id, randomRec(id)))
				case 1:
					reportErr(db.Patch(id, randomPatch()))
				default:
					reportErr(db.Delete(id))
				}
				if loadErr() != nil {
					return
				}
			}
		}(int64(90210 + w))
	}

	cleanupDone := false
	defer func() {
		if cleanupDone {
			return
		}
		close(stop)
		wg.Wait()
	}()

	queries := []*qx.QX{
		qx.Query(
			qx.EQ("active", true),
			qx.GTE("age", 24),
			qx.LT("age", 50),
		).By("age", qx.ASC).Max(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).By("full_name", qx.ASC).Max(60),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Max(90),
		qx.Query(
			qx.HASANY("tags", []string{"go", "ops"}),
			qx.NOTIN("country", []string{"Thailand", "Iceland"}),
		),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Max(75),
		qx.Query(
			qx.GTE("age", 24),
			qx.LTE("age", 42),
			qx.EQ("active", true),
		).Max(85),
		qx.Query(
			qx.GTE("age", 20),
		).ByArrayCount("tags", qx.DESC).Skip(5).Max(40),
	}

	var sawExec bool
	var sawPlan bool

	r := newRand(20260228)
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := loadErr(); err != nil {
			t.Fatalf("writer error: %v", err)
		}

		q := queries[r.IntN(len(queries))]
		snap := db.getSnapshot()
		view := db.makeQueryView(snap)

		nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalence(t, view, q)
		sawExec = sawExec || usedExec
		sawPlan = sawPlan || usedPlan

		if q.Limit == 0 && q.Offset == 0 {
			cnt, err := view.countPreparedExpr(nq.Expr)
			if err != nil {
				db.releaseQueryView(view)
				t.Fatalf("countPreparedExpr: %v", err)
			}
			if cnt != uint64(len(ref)) {
				db.releaseQueryView(view)
				t.Fatalf("count mismatch on snapshot view: got=%d want=%d", cnt, len(ref))
			}
		}

		db.releaseQueryView(view)
	}

	close(stop)
	wg.Wait()
	cleanupDone = true
	if err := loadErr(); err != nil {
		t.Fatalf("writer error: %v", err)
	}

	if !sawExec {
		t.Fatalf("expected execution fast-path to be exercised during concurrent snapshot checks")
	}
	if !sawPlan {
		t.Fatalf("expected planner fast-path to be exercised during concurrent snapshot checks")
	}
}

func TestRace_ConcurrentWriters_OROrderScoreChurn_SnapshotRouteEquivalence(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_600)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	var errMu sync.Mutex
	var firstErr error
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	loadErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()

			r := newRand(seed)
			randomRec := func(id uint64) *Rec {
				name := names[r.IntN(len(names))]
				score := float64(r.IntN(20_000))/10.0 + r.Float64()*0.001
				if r.IntN(100) < 5 {
					score = 9_000 + float64(r.IntN(1_000)) + r.Float64()*0.001
				}
				return &Rec{
					Meta:     Meta{Country: countries[r.IntN(len(countries))]},
					Name:     name,
					Email:    fmt.Sprintf("%s-%d@example.test", name, id),
					Age:      18 + r.IntN(65),
					Score:    score,
					Active:   r.IntN(2) == 0,
					Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
					FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(30_000)),
				}
			}
			randomPatch := func() []Field {
				switch r.IntN(5) {
				case 0:
					return []Field{{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001}}
				case 1:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "active", Value: r.IntN(2) == 0},
					}
				case 2:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "country", Value: countries[r.IntN(len(countries))]},
					}
				case 3:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "tags", Value: []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]}},
					}
				default:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "age", Value: float64(18 + r.IntN(65))},
					}
				}
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(24_000))
				switch r.IntN(10) {
				case 0, 1, 2, 3:
					reportErr(db.Set(id, randomRec(id)))
				case 4, 5, 6, 7, 8:
					reportErr(db.Patch(id, randomPatch()))
				default:
					reportErr(db.Delete(id))
				}
				if loadErr() != nil {
					return
				}
			}
		}(int64(20260304 + w))
	}

	cleanupDone := false
	defer func() {
		if cleanupDone {
			return
		}
		close(stop)
		wg.Wait()
	}()

	queries := []*qx.QX{
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.IN("country", []string{"NL", "DE", "PL"}),
					qx.HASANY("tags", []string{"go", "ops"}),
					qx.GTE("score", 40.0),
				),
				qx.AND(
					qx.EQ("name", "alice"),
					qx.GTE("age", 20),
					qx.LTE("age", 55),
				),
				qx.AND(
					qx.HASANY("tags", []string{"rust", "db"}),
					qx.GTE("score", 30.0),
				),
			),
		).By("score", qx.DESC).Skip(180).Max(90),
		qx.Query(
			qx.OR(
				qx.AND(qx.PREFIX("full_name", "FN-1"), qx.EQ("active", true)),
				qx.AND(qx.EQ("country", "US"), qx.GTE("age", 25)),
				qx.AND(qx.HASANY("tags", []string{"go", "db"}), qx.GTE("score", 60.0)),
			),
		).By("score", qx.ASC).Max(75),
		qx.Query(
			qx.OR(
				qx.AND(qx.EQ("active", false), qx.NOTIN("country", []string{"Thailand", "Iceland"}), qx.GTE("score", 35.0)),
				qx.AND(qx.EQ("name", "bob"), qx.GTE("age", 22)),
			),
		).By("score", qx.DESC).Skip(30).Max(120),
	}

	var sawPlan bool
	r := newRand(20260305)
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := loadErr(); err != nil {
			t.Fatalf("writer error: %v", err)
		}

		q := queries[r.IntN(len(queries))]
		snap := db.getSnapshot()
		view := db.makeQueryView(snap)

		_, _, _, usedPlan := assertPreparedRouteEquivalence(t, view, q)
		sawPlan = sawPlan || usedPlan

		db.releaseQueryView(view)
	}

	close(stop)
	wg.Wait()
	cleanupDone = true
	if err := loadErr(); err != nil {
		t.Fatalf("writer error: %v", err)
	}
	if !sawPlan {
		t.Fatalf("expected planner route to be exercised for OR+ORDER churn checks")
	}
}

func TestRace_StringKeyGrowth_FastPaths_SnapshotRouteEquivalence(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}

	const seedN = 2_000
	rSeed := newRand(20260306)
	for i := 1; i <= seedN; i++ {
		name := names[rSeed.IntN(len(names))]
		rec := &Rec{
			Meta:     Meta{Country: countries[rSeed.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%06d@example.test", name, i),
			Age:      18 + rSeed.IntN(65),
			Score:    float64(rSeed.IntN(20_000))/10.0 + rSeed.Float64()*0.001,
			Active:   rSeed.IntN(2) == 0,
			Tags:     []string{tagPool[rSeed.IntN(len(tagPool))], tagPool[rSeed.IntN(len(tagPool))]},
			FullName: fmt.Sprintf("FN-%05d", i),
		}
		if err := db.Set(fmt.Sprintf("dyn-%06d", i), rec); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	var (
		errMu    sync.Mutex
		firstErr error
		nextMu   sync.Mutex
		nextID   = seedN
	)
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	loadErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}
	nextKey := func() string {
		nextMu.Lock()
		nextID++
		id := nextID
		nextMu.Unlock()
		return fmt.Sprintf("dyn-%06d", id)
	}
	randomExistingKey := func(r *rand.Rand) string {
		nextMu.Lock()
		maxID := nextID
		nextMu.Unlock()
		id := 1 + r.IntN(maxID)
		return fmt.Sprintf("dyn-%06d", id)
	}

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)

			makeRec := func(key string) *Rec {
				name := names[r.IntN(len(names))]
				return &Rec{
					Meta:     Meta{Country: countries[r.IntN(len(countries))]},
					Name:     name,
					Email:    fmt.Sprintf("%s-%s@example.test", name, key),
					Age:      18 + r.IntN(65),
					Score:    float64(r.IntN(20_000))/10.0 + r.Float64()*0.001,
					Active:   r.IntN(2) == 0,
					Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
					FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(60_000)),
				}
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				switch r.IntN(10) {
				case 0, 1, 2, 3, 4, 5:
					key := nextKey()
					reportErr(db.Set(key, makeRec(key)))
				case 6, 7, 8:
					key := randomExistingKey(r)
					patch := []Field{
						{Name: "age", Value: float64(18 + r.IntN(65))},
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "active", Value: r.IntN(2) == 0},
					}
					reportErr(db.Patch(key, patch))
				default:
					key := randomExistingKey(r)
					reportErr(db.Delete(key))
				}

				if loadErr() != nil {
					return
				}
			}
		}(int64(20260307 + w))
	}

	cleanupDone := false
	defer func() {
		if cleanupDone {
			return
		}
		close(stop)
		wg.Wait()
	}()

	queries := []*qx.QX{
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).By("full_name", qx.ASC).Max(50),
		qx.Query(
			qx.GTE("age", 25),
			qx.LT("age", 60),
			qx.EQ("active", true),
		).By("age", qx.ASC).Max(60),
		qx.Query(
			qx.OR(
				qx.AND(qx.EQ("active", true), qx.HASANY("tags", []string{"go", "ops"})),
				qx.AND(qx.EQ("name", "alice"), qx.GTE("age", 20)),
				qx.PREFIX("full_name", "FN-1"),
			),
		).By("score", qx.DESC).Skip(20).Max(80),
		qx.Query(
			qx.EQ("active", true),
			qx.NOTIN("country", []string{"NL", "DE"}),
			qx.IN("name", []string{"alice", "bob", "carol"}),
		).By("age", qx.ASC).Max(80),
		qx.Query(
			qx.PREFIX("full_name", "FN-"),
		).Max(90),
	}

	var sawExec bool
	var sawPlan bool
	r := newRand(20260308)
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := loadErr(); err != nil {
			t.Fatalf("writer error: %v", err)
		}

		q := queries[r.IntN(len(queries))]
		snap := db.getSnapshot()
		view := db.makeQueryView(snap)

		nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalenceString(t, view, q)
		sawExec = sawExec || usedExec
		sawPlan = sawPlan || usedPlan

		// strmap snapshot must provide stable idx<->key round-trip for returned ids.
		for _, key := range ref {
			idx, ok := view.strmapView.getIdxNoLock(key)
			if !ok {
				db.releaseQueryView(view)
				t.Fatalf("missing idx mapping in strmap snapshot for key=%q", key)
			}
			back, ok := view.strmapView.getStringNoLock(idx)
			if !ok || back != key {
				db.releaseQueryView(view)
				t.Fatalf("strmap round-trip mismatch: key=%q idx=%d back=%q ok=%v", key, idx, back, ok)
			}
		}

		if q.Limit == 0 && q.Offset == 0 {
			cnt, err := view.countPreparedExpr(nq.Expr)
			if err != nil {
				db.releaseQueryView(view)
				t.Fatalf("countPreparedExpr: %v", err)
			}
			if cnt != uint64(len(ref)) {
				db.releaseQueryView(view)
				t.Fatalf("count mismatch on string snapshot view: got=%d want=%d", cnt, len(ref))
			}
		}

		db.releaseQueryView(view)
	}

	close(stop)
	wg.Wait()
	cleanupDone = true
	if err := loadErr(); err != nil {
		t.Fatalf("writer error: %v", err)
	}
	if !sawExec {
		t.Fatalf("expected execution fast-path to be exercised during string-key growth checks")
	}
	if !sawPlan {
		t.Fatalf("expected planner fast-path to be exercised during string-key growth checks")
	}
}
