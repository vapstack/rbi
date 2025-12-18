package rbi

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
)

func TestRace_ConcurrentReadersAndWriters(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 200)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.Intn(250))
				op := r.Intn(3)

				switch op {
				case 0:
					rec := &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     []string{"alice", "bob", "carol"}[r.Intn(3)],
						Age:      18 + r.Intn(60),
						Score:    r.Float64() * 100,
						Active:   r.Intn(2) == 0,
						Tags:     []string{"go", "java", "ops"}[:1+r.Intn(3)],
						FullName: "FN",
					}
					_ = db.Set(id, rec)

				case 1:
					_ = db.Patch(id, []Field{{Name: "age", Value: float64(20 + r.Intn(50))}})

				case 2:
					_ = db.Delete(id)
				}
			}
		}(int64(1000 + w))
	}

	for rr := 0; rr < 6; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))

			qs := []*qx.QX{
				{Expr: qx.Expr{Op: qx.OpGT, Field: "age", Value: 30}},
				{Expr: qx.Expr{Op: qx.OpPREFIX, Field: "name", Value: "a"}},
				{Expr: qx.Expr{Op: qx.OpHASANY, Field: "tags", Value: []string{"go", "java"}}},
				{
					Expr: qx.Expr{
						Op: qx.OpAND,
						Operands: []qx.Expr{
							{Op: qx.OpGTE, Field: "age", Value: 25},
							{Op: qx.OpEQ, Field: "active", Value: true},
						},
					},
				},
			}

			for {
				select {
				case <-stop:
					return
				default:
				}

				q := qs[r.Intn(len(qs))]

				ids, err := db.QueryKeys(q)
				if errors.Is(err, ErrIndexDisabled) || errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					return
				}

				items, _ := db.GetMany(ids...)
				for _, it := range items {
					if it == nil {
						continue
					}
					ok, e := evalExprBool(it, q.Expr)
					if e != nil || !ok {
						break
					}
				}

				_, _ = db.Count(q)
			}
		}(int64(2000 + rr))
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()
}
