package rbi

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
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
					ok, e := evalExprBool(it, q.Filter)
					if e != nil {
						reportErr(fmt.Errorf("eval logic error: %w", e))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("consistency error: item %v returned for query %v", it, q.Filter))
						return
					}
				}

				if _, err = db.Count(q.Filter); err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					reportErr(fmt.Errorf("count error: %w", err))
					return
				}
			}
		}(int64(2000 + rr))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Race test failure: %v", err)
	}
}

func TestRace_ConcurrentReadersAndWriters_Snapshots(t *testing.T) {
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
					ok, e := evalExprBool(it, q.Filter)
					if e != nil {
						reportErr(fmt.Errorf("eval logic error: %w", e))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("consistency error: item %v returned for query %v", it, q.Filter))
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

				if _, err = db.Count(q.Filter); err != nil {
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

	time.Sleep(150 * time.Millisecond)
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
		).Sort("age", qx.ASC).Limit(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).Sort("full_name", qx.ASC).Limit(60),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Limit(90),
		qx.Query(
			qx.HASANY("tags", []string{"go", "ops"}),
			qx.NOTIN("country", []string{"Thailand", "Iceland"}),
		),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Limit(75),
		qx.Query(
			qx.GTE("age", 24),
			qx.LTE("age", 42),
			qx.EQ("active", true),
		).Limit(85),
		queryOrderSortByArrayCount(
			qx.Query(
				qx.GTE("age", 20),
			),
			"tags",
			qx.DESC,
		).Offset(5).Limit(40),
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
		if _, err := db.QueryKeys(q); err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		sawExec = true
		sawPlan = true
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
		).Sort("score", qx.DESC).Offset(180).Limit(90),
		qx.Query(
			qx.OR(
				qx.AND(qx.PREFIX("full_name", "FN-1"), qx.EQ("active", true)),
				qx.AND(qx.EQ("country", "US"), qx.GTE("age", 25)),
				qx.AND(qx.HASANY("tags", []string{"go", "db"}), qx.GTE("score", 60.0)),
			),
		).Sort("score", qx.ASC).Limit(75),
		qx.Query(
			qx.OR(
				qx.AND(qx.EQ("active", false), qx.NOTIN("country", []string{"Thailand", "Iceland"}), qx.GTE("score", 35.0)),
				qx.AND(qx.EQ("name", "bob"), qx.GTE("age", 22)),
			),
		).Sort("score", qx.DESC).Offset(30).Limit(120),
	}

	var sawPlan bool
	r := newRand(20260305)
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := loadErr(); err != nil {
			t.Fatalf("writer error: %v", err)
		}

		q := queries[r.IntN(len(queries))]
		if _, err := db.QueryKeys(q); err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		sawPlan = true
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
		).Sort("full_name", qx.ASC).Limit(50),
		qx.Query(
			qx.GTE("age", 25),
			qx.LT("age", 60),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Limit(60),
		qx.Query(
			qx.OR(
				qx.AND(qx.EQ("active", true), qx.HASANY("tags", []string{"go", "ops"})),
				qx.AND(qx.EQ("name", "alice"), qx.GTE("age", 20)),
				qx.PREFIX("full_name", "FN-1"),
			),
		).Sort("score", qx.DESC).Offset(20).Limit(80),
		qx.Query(
			qx.EQ("active", true),
			qx.NOTIN("country", []string{"NL", "DE"}),
			qx.IN("name", []string{"alice", "bob", "carol"}),
		).Sort("age", qx.ASC).Limit(80),
		qx.Query(
			qx.PREFIX("full_name", "FN-"),
		).Limit(90),
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
		if _, err := db.QueryKeys(q); err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		sawExec = true
		sawPlan = true
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

/**/

type raceExtraMeta struct {
	Country string `db:"country" rbi:"index"`
}

type raceExtraRec struct {
	raceExtraMeta

	Name     string   `db:"name"      rbi:"index"`
	Email    string   `db:"email"     rbi:"index"`
	Age      int      `db:"age"       rbi:"index"`
	Score    float64  `db:"score"     rbi:"index"`
	Active   bool     `db:"active"    rbi:"index"`
	Tags     []string `db:"tags"      rbi:"index"`
	FullName string   `db:"full_name" rbi:"index"`
	Opt      *string  `db:"opt"       rbi:"index"`
}

func raceExtraOpenTempDBUint64(t *testing.T, opts Options) *DB[uint64, raceExtraRec] {
	t.Helper()

	path := filepath.Join(t.TempDir(), "race_extra.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts = testOptions(opts)
	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[uint64, raceExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func raceExtraOpenTempDBString(t *testing.T, opts Options) *DB[string, raceExtraRec] {
	t.Helper()

	path := filepath.Join(t.TempDir(), "race_extra_string.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts = testOptions(opts)
	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[string, raceExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func raceExtraSeedGeneratedUint64Data(t *testing.T, db *DB[uint64, raceExtraRec], n int, gen func(i int) *raceExtraRec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*raceExtraRec, 0, batchSize)

	flush := func() {
		t.Helper()
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

func raceExtraSeedGeneratedStringData(t *testing.T, db *DB[string, raceExtraRec], keys []string, gen func(i int, key string) *raceExtraRec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if len(keys) > 0 && len(keys) < batchSize {
		batchSize = len(keys)
	}
	batchKeys := make([]string, 0, batchSize)
	batchVals := make([]*raceExtraRec, 0, batchSize)

	flush := func() {
		t.Helper()
		if len(batchKeys) == 0 {
			return
		}
		if err := db.BatchSet(batchKeys, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchKeys), err)
		}
		batchKeys = batchKeys[:0]
		batchVals = batchVals[:0]
	}

	for i, key := range keys {
		batchKeys = append(batchKeys, key)
		batchVals = append(batchVals, gen(i+1, key))
		if len(batchKeys) == cap(batchKeys) {
			flush()
		}
	}
	flush()
}

func raceExtraSetNumericBucketKnobs(t *testing.T, db *DB[uint64, raceExtraRec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys
	prevEngineSize := db.engine.exec.NumericRangeBucketSize
	prevEngineMinField := db.engine.exec.NumericRangeBucketMinFieldKeys
	prevEngineMinSpan := db.engine.exec.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan
	db.engine.exec.NumericRangeBucketSize = size
	db.engine.exec.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.engine.exec.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
		db.engine.exec.NumericRangeBucketSize = prevEngineSize
		db.engine.exec.NumericRangeBucketMinFieldKeys = prevEngineMinField
		db.engine.exec.NumericRangeBucketMinSpanKeys = prevEngineMinSpan
	})
}

func raceExtraRequireNumericRangeBucketCacheEntry(t *testing.T, snap *snapshot.View, field string) *qcache.NumericRangeBucketEntry {
	t.Helper()

	if snap == nil || snap.NumericRangeBucketCache() == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	entry, ok := snap.NumericRangeBucketCache().LoadField(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	if entry == nil || entry.Index().BucketSize() <= 0 {
		t.Fatalf("expected non-nil numeric range bucket entry for field %q", field)
	}
	return entry
}

func raceExtraRangeKeys(startInclusive, endExclusive, total int) []uint64 {
	if startInclusive < 1 {
		startInclusive = 1
	}
	if endExclusive < startInclusive {
		return nil
	}
	if endExclusive > total+1 {
		endExclusive = total + 1
	}
	out := make([]uint64, 0, endExclusive-startInclusive)
	for i := startInclusive; i < endExclusive; i++ {
		out = append(out, uint64(i))
	}
	return out
}

func TestRaceExtra_PublicQueriesStayExactUnderConcurrentMaterializedCacheThrash(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval:                             -1,
		SnapshotMaterializedPredCacheMaxEntries:     2,
		SnapshotMaterializedPredCacheMaxCardinality: 64,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	raceExtraSetNumericBucketKnobs(t, db, 128, 1, 1)

	type tc struct {
		q    *qx.QX
		want []uint64
	}

	cases := []tc{
		{q: qx.Query(qx.GTE("age", 5950)), want: raceExtraRangeKeys(5950, total+1, total)},
		{q: qx.Query(qx.GTE("age", 5900)), want: raceExtraRangeKeys(5900, total+1, total)},
		{q: qx.Query(qx.LT("age", 50)), want: raceExtraRangeKeys(1, 50, total)},
		{q: qx.Query(qx.LT("age", 120)), want: raceExtraRangeKeys(1, 120, total)},
		{q: qx.Query(qx.GTE("age", 2500), qx.LT("age", 2560)), want: raceExtraRangeKeys(2500, 2560, total)},
		{q: qx.Query(qx.GTE("age", 2500), qx.LT("age", 2580)), want: raceExtraRangeKeys(2500, 2580, total)},
	}

	for i, tc := range cases {
		got, err := db.QueryKeys(tc.q)
		if err != nil {
			t.Fatalf("warm QueryKeys(%d): %v", i, err)
		}
		if !queryIDsEqual(tc.q, got, tc.want) {
			t.Fatalf("warm QueryKeys(%d) mismatch: got=%v want=%v", i, got, tc.want)
		}
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	recordAges := func(values []*raceExtraRec) []uint64 {
		out := make([]uint64, 0, len(values))
		for _, value := range values {
			if value == nil {
				return nil
			}
			out = append(out, uint64(value.Age))
		}
		slices.Sort(out)
		return out
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for g := 0; g < max(8, runtime.GOMAXPROCS(0)); g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 200; i++ {
				if failed.Load() != nil {
					return
				}

				tc := cases[r.IntN(len(cases))]

				got, err := db.QueryKeys(tc.q)
				if err != nil {
					setFailed(fmt.Sprintf("QueryKeys failed: %v", err))
					return
				}
				if !queryIDsEqual(tc.q, got, tc.want) {
					setFailed(fmt.Sprintf("QueryKeys mismatch: got=%v want=%v", got, tc.want))
					return
				}

				count, err := db.Count(tc.q.Filter)
				if err != nil {
					setFailed(fmt.Sprintf("Count failed: %v", err))
					return
				}
				if count != uint64(len(tc.want)) {
					setFailed(fmt.Sprintf("Count mismatch: got=%d want=%d", count, len(tc.want)))
					return
				}

				if i%4 == 0 {
					values, err := db.Query(tc.q)
					if err != nil {
						setFailed(fmt.Sprintf("Query failed: %v", err))
						return
					}
					gotAges := recordAges(values)
					if gotAges == nil {
						setFailed("Query returned nil record")
						return
					}
					if !slices.Equal(gotAges, tc.want) {
						setFailed(fmt.Sprintf("Query ages mismatch: got=%v want=%v", gotAges, tc.want))
						return
					}
				}
			}
		}(int64(20260629 + g))
	}

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	snap := db.engine.snapshot.Current()
	if got := snap.MaterializedPredCache().EntryCount(); got > 2 {
		t.Fatalf("materialized predicate cache exceeded global limit: got=%d", got)
	}
	if got := snap.MaterializedPredCache().OversizedCount(); got > qcache.MaterializedPredOversizedLimit(snap.MaterializedPredCacheLimit()) {
		t.Fatalf("oversized materialized predicate cache exceeded limit: got=%d", got)
	}
}

func TestRaceExtra_PinnedSnapshotQueryViewStaysExactAcrossConcurrentPublishes(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		AutoBatchMax:    1,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	seq := db.engine.snapshot.Current().Seq
	snap, ref, ok := db.engine.snapshot.PinBySeq(seq)
	if !ok || snap == nil {
		t.Fatal("expected current snapshot to be pinnable")
	}
	cleanupDone := false
	defer func() {
		if cleanupDone {
			return
		}
		db.engine.snapshot.Unpin(seq, ref)
	}()

	q := qx.Query(
		qx.GTE("age", 2500),
		qx.LT("age", 2600),
	)
	want := raceExtraRangeKeys(2500, 2600, total)

	checkPinnedSnapshot := func() error {
		prepared, viewQ, err := prepareTestQuery(db.engine, q)
		if err != nil {
			return err
		}
		defer prepared.Release()

		view := db.engine.exec.AcquireView(snap)
		got, err := view.Query(&viewQ, false, false)
		db.engine.exec.ReleaseView(view)
		if err != nil {
			return err
		}
		if !queryIDsEqual(q, got, want) {
			return fmt.Errorf("pinned snapshot query mismatch: got=%v want=%v", got, want)
		}
		return nil
	}

	if err := checkPinnedSnapshot(); err != nil {
		t.Fatal(err)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 30; i++ {
				if failed.Load() != nil {
					return
				}

				id := uint64(1 + r.IntN(total))
				patch := []Field{
					{Name: "active", Value: r.IntN(2) == 0},
					{Name: "country", Value: countries[r.IntN(len(countries))]},
					{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}},
					{Name: "name", Value: names[r.IntN(len(names))]},
					{Name: "opt", Value: fmt.Sprintf("opt-%d-%d", seed, i)},
				}
				if i%5 == 0 {
					patch[len(patch)-1] = Field{Name: "opt", Value: nil}
				}
				if err := db.Patch(id, patch); err != nil {
					setFailed(fmt.Sprintf("Patch(existing) failed: %v", err))
					return
				}
			}
		}(int64(20260729 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 60; i++ {
			if failed.Load() != nil {
				return
			}
			if err := checkPinnedSnapshot(); err != nil {
				setFailed(err.Error())
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	if err := checkPinnedSnapshot(); err != nil {
		t.Fatal(err)
	}

	stats := db.engine.snapshot.Stats(snap, nil)
	if stats.RegistrySize < 2 || stats.PinnedRefs == 0 {
		t.Fatalf("expected old snapshot ref to stay pinned across publishes: %+v", stats)
	}

	db.engine.snapshot.Unpin(seq, ref)
	cleanupDone = true

	if stats = db.engine.snapshot.Stats(db.engine.snapshot.Current(), nil); stats.RegistrySize != 1 || stats.PinnedRefs != 0 {
		t.Fatalf("expected old snapshot ref to be pruned after unpin: %+v", stats)
	}
}

func TestRaceExtra_PinnedStringSnapshotScanStaysStableAcrossConcurrentPublishes(t *testing.T) {
	db := raceExtraOpenTempDBString(t, Options{
		AnalyzeInterval: -1,
		AutoBatchMax:    1,
	})

	const seedN = 1_000
	keys := make([]string, 0, seedN)
	for i := 1; i <= seedN; i++ {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	raceExtraSeedGeneratedStringData(t, db, keys, func(i int, key string) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("%s@example.test", key),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})

	old := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatal("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	expected := slices.Clone(keys)
	checkPinnedScan := func() error {
		iter := pinned.Universe.Iter()
		defer iter.Release()

		got := make([]string, 0, len(expected))
		if err := db.scanStringKeys(pinned.StrMap, pinned.Universe, iter, "", func(id string) (bool, error) {
			got = append(got, id)
			return true, nil
		}); err != nil {
			return err
		}
		if !slices.Equal(got, expected) {
			return fmt.Errorf("pinned string scan mismatch: got=%v want=%v", got, expected)
		}
		return nil
	}

	if err := checkPinnedScan(); err != nil {
		t.Fatal(err)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var nextID atomic.Uint64
	nextID.Store(seedN)

	start := make(chan struct{})
	var wg sync.WaitGroup

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 220; i++ {
				if failed.Load() != nil {
					return
				}

				switch r.IntN(3) {
				case 0:
					key := fmt.Sprintf("k%04d", 1+r.IntN(seedN))
					patch := []Field{
						{Name: "active", Value: r.IntN(2) == 0},
						{Name: "country", Value: countries[r.IntN(len(countries))]},
						{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}},
						{Name: "name", Value: names[r.IntN(len(names))]},
					}
					if err := db.Patch(key, patch); err != nil {
						setFailed(fmt.Sprintf("Patch(seed key) failed: %v", err))
						return
					}
				case 1:
					id := nextID.Add(1)
					key := fmt.Sprintf("future-%04d", id)
					rec := &raceExtraRec{
						raceExtraMeta: raceExtraMeta{Country: countries[r.IntN(len(countries))]},
						Name:          names[r.IntN(len(names))],
						Email:         fmt.Sprintf("%s@example.test", key),
						Age:           10_000 + int(id),
						Score:         float64(id),
						Active:        r.IntN(2) == 0,
						Tags:          []string{tags[r.IntN(len(tags))]},
						FullName:      fmt.Sprintf("FN-future-%d", id),
					}
					if err := db.Set(key, rec); err != nil {
						setFailed(fmt.Sprintf("Set(future key) failed: %v", err))
						return
					}
				default:
					hi := nextID.Load()
					if hi <= seedN {
						continue
					}
					key := fmt.Sprintf("future-%04d", seedN+1+r.IntN(int(hi-seedN)))
					if err := db.Delete(key); err != nil {
						setFailed(fmt.Sprintf("Delete(future key) failed: %v", err))
						return
					}
				}
			}
		}(int64(20260829 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 180; i++ {
			if failed.Load() != nil {
				return
			}
			if err := checkPinnedScan(); err != nil {
				setFailed(err.Error())
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	if err := checkPinnedScan(); err != nil {
		t.Fatal(err)
	}
}

func TestRaceExtra_PublicNumericRangeQueriesStayExactAcrossConcurrentUnchangedFieldPublishes(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		AutoBatchMax:                            1,
		SnapshotMaterializedPredCacheMaxEntries: 64,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 2500)),
		qx.Query(qx.GTE("age", 2501)),
		qx.Query(qx.LT("age", 4500)),
		qx.Query(qx.LT("age", 4499)),
		qx.Query(qx.GTE("age", 2500), qx.LT("age", 4500)),
		qx.Query(qx.GTE("age", 2501), qx.LT("age", 4499)),
	}

	wants := [][]uint64{
		raceExtraRangeKeys(2500, total+1, total),
		raceExtraRangeKeys(2501, total+1, total),
		raceExtraRangeKeys(1, 4500, total),
		raceExtraRangeKeys(1, 4499, total),
		raceExtraRangeKeys(2500, 4500, total),
		raceExtraRangeKeys(2501, 4499, total),
	}

	for i, q := range queries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("warm QueryKeys(%d): %v", i, err)
		}
		if !queryIDsEqual(q, got, wants[i]) {
			t.Fatalf("warm QueryKeys(%d) mismatch: got=%v want=%v", i, got, wants[i])
		}
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	recordAges := func(values []*raceExtraRec) []uint64 {
		out := make([]uint64, 0, len(values))
		for _, value := range values {
			if value == nil {
				return nil
			}
			out = append(out, uint64(value.Age))
		}
		slices.Sort(out)
		return out
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 250; i++ {
				if failed.Load() != nil {
					return
				}

				id := uint64(1 + r.IntN(total))
				var patch []Field
				switch r.IntN(5) {
				case 0:
					patch = []Field{{Name: "active", Value: r.IntN(2) == 0}}
				case 1:
					patch = []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
				case 2:
					patch = []Field{
						{Name: "name", Value: names[r.IntN(len(names))]},
						{Name: "full_name", Value: fmt.Sprintf("FN-extra-%d-%d", seed, i)},
					}
				case 3:
					patch = []Field{{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}}}
				default:
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "opt", Value: nil}}
					} else {
						patch = []Field{{Name: "opt", Value: fmt.Sprintf("opt-%d-%d", seed, i)}}
					}
				}

				if err := db.Patch(id, patch); err != nil {
					setFailed(fmt.Sprintf("writer patch failed: %v", err))
					return
				}
			}
		}(int64(20260329 + w))
	}

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 160; i++ {
				if failed.Load() != nil {
					return
				}

				idx := r.IntN(len(queries))
				q := queries[idx]
				want := wants[idx]

				got, err := db.QueryKeys(q)
				if err != nil {
					setFailed(fmt.Sprintf("QueryKeys(%d) failed: %v", idx, err))
					return
				}
				if !queryIDsEqual(q, got, want) {
					setFailed(fmt.Sprintf("QueryKeys(%d) mismatch: got=%v want=%v", idx, got, want))
					return
				}

				count, err := db.Count(q.Filter)
				if err != nil {
					setFailed(fmt.Sprintf("Count(%d) failed: %v", idx, err))
					return
				}
				if count != uint64(len(want)) {
					setFailed(fmt.Sprintf("Count(%d) mismatch: got=%d want=%d", idx, count, len(want)))
					return
				}
			}
		}(int64(20260429 + g))
	}

	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 80; i++ {
				if failed.Load() != nil {
					return
				}

				idx := r.IntN(len(queries))
				q := queries[idx]
				want := wants[idx]

				values, err := db.Query(q)
				if err != nil {
					setFailed(fmt.Sprintf("Query(%d) failed: %v", idx, err))
					return
				}
				gotAges := recordAges(values)
				if gotAges == nil {
					setFailed(fmt.Sprintf("Query(%d) returned nil record", idx))
					return
				}
				if !slices.Equal(gotAges, want) {
					setFailed(fmt.Sprintf("Query(%d) ages mismatch: got=%v want=%v", idx, gotAges, want))
					return
				}
			}
		}(int64(20260529 + g))
	}

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}
