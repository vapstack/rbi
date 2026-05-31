package rbi

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func TestUnique_QueryEQ_Max1Equivalent(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	qNoLimit := qx.Query(qx.EQ("email", "a@x"))
	qLimit1 := qx.Query(qx.EQ("email", "a@x")).Limit(1)

	idsNoLimit, err := db.QueryKeys(qNoLimit)
	if err != nil {
		t.Fatalf("QueryKeys(no limit): %v", err)
	}
	idsLimit1, err := db.QueryKeys(qLimit1)
	if err != nil {
		t.Fatalf("QueryKeys(limit=1): %v", err)
	}
	if !slices.Equal(idsNoLimit, idsLimit1) {
		t.Fatalf("unique eq mismatch: no-limit=%v limit1=%v", idsNoLimit, idsLimit1)
	}

	// Same invariant for conjunctions anchored by unique EQ.
	qAndNoLimit := qx.Query(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 1),
	)
	qAndLimit1 := qx.Query(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 1),
	).Limit(1)

	idsAndNoLimit, err := db.QueryKeys(qAndNoLimit)
	if err != nil {
		t.Fatalf("QueryKeys(and no limit): %v", err)
	}
	idsAndLimit1, err := db.QueryKeys(qAndLimit1)
	if err != nil {
		t.Fatalf("QueryKeys(and limit=1): %v", err)
	}
	if !slices.Equal(idsAndNoLimit, idsAndLimit1) {
		t.Fatalf("unique and-eq mismatch: no-limit=%v limit1=%v", idsAndNoLimit, idsAndLimit1)
	}
}

func TestUnique_QueryEQ_UsesUniquePlan_AcrossHitMissAndUnsatisfiable(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64Unique(t, opts)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	tests := []struct {
		name    string
		q       *qx.QX
		wantLen int
	}{
		{
			name:    "hit_no_limit",
			q:       qx.Query(qx.EQ("email", "a@x")),
			wantLen: 1,
		},
		{
			name:    "hit_limit_1",
			q:       qx.Query(qx.EQ("email", "a@x")).Limit(1),
			wantLen: 1,
		},
		{
			name:    "miss_no_limit",
			q:       qx.Query(qx.EQ("email", "missing@x")),
			wantLen: 0,
		},
		{
			name:    "miss_limit_1",
			q:       qx.Query(qx.EQ("email", "missing@x")).Limit(1),
			wantLen: 0,
		},
		{
			name: "unsatisfiable_conjunction_limit_1",
			q: qx.Query(
				qx.EQ("email", "a@x"),
				qx.HASALL("tags", []string{"missing"}),
			).Limit(1),
			wantLen: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := len(events)
			ids, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			if len(ids) != tc.wantLen {
				t.Fatalf("expected len=%d, got %v", tc.wantLen, ids)
			}
			if len(events) <= before {
				t.Fatalf("expected trace event for query")
			}
			last := events[len(events)-1]
			if last.Plan != string(PlanUniqueEq) {
				t.Fatalf("expected plan %q, got %q", PlanUniqueEq, last.Plan)
			}
		})
	}
}

func TestUnique_QueryEQ_DirectFastPathRequiresNoOrderNoOffset(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	opt := "present"
	if err := db.Set(3, &UniqueTestRec{Email: "c@x", Code: 3, Opt: &opt}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	t.Run("offset", func(t *testing.T) {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "c@x")).Offset(1))
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("expected empty result after offset, got %v", ids)
		}
	})

	t.Run("ordered_nullable_unique", func(t *testing.T) {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)).Sort("code", qx.DESC))
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		want := []uint64{2, 1}
		if !slices.Equal(ids, want) {
			t.Fatalf("unexpected ordered ids: got=%v want=%v", ids, want)
		}
	})

	t.Run("invalid_order_field", func(t *testing.T) {
		if _, err := db.QueryKeys(qx.Query(qx.EQ("email", "c@x")).Sort("missing", qx.ASC)); err == nil {
			t.Fatalf("expected QueryKeys to fail for unknown order field")
		}
	})
}

func TestUnique_QueryEQ_NullLimit_TraceCountsOnlyEmittedRows(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64Unique(t, opts)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)).Limit(1))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one id, got %v", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanUniqueEq) {
		t.Fatalf("expected plan %q, got %q", PlanUniqueEq, last.Plan)
	}
	if last.RowsExamined != 1 {
		t.Fatalf("expected RowsExamined=1, got trace=%+v", last)
	}
}

func TestUnique_Count_WithUniqueAnchor(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	c1, err := db.Count(qx.EQ("email", "a@x"))
	if err != nil {
		t.Fatalf("Count(eq unique): %v", err)
	}
	if c1 != 1 {
		t.Fatalf("expected count=1, got %d", c1)
	}

	c2, err := db.Count(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 1),
	)
	if err != nil {
		t.Fatalf("Count(and hit): %v", err)
	}
	if c2 != 1 {
		t.Fatalf("expected count=1, got %d", c2)
	}

	c3, err := db.Count(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 2),
	)
	if err != nil {
		t.Fatalf("Count(and miss): %v", err)
	}
	if c3 != 0 {
		t.Fatalf("expected count=0, got %d", c3)
	}
}

func TestUnique_Set_DuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_Set_SameRecordUpdateAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_Patch_ConflictingUpdateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Patch(2, []Field{{Name: "email", Value: "a@x"}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_NilAllowedMultipleTimes(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_OptDuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	v := "same"

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: &v}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &v})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_BatchSet_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	ids := []uint64{1, 2}
	vals := []*UniqueTestRec{
		{Email: "a@x", Code: 1},
		{Email: "a@x", Code: 2},
	}

	err := db.BatchSet(ids, vals)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_BatchSet_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{2, 3}
	vals := []*UniqueTestRec{
		{Email: "b@x", Code: 2},
		{Email: "a@x", Code: 3},
	}

	err := db.BatchSet(ids, vals)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_BatchSet_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{1, 2}
	vals := []*UniqueTestRec{
		{Email: "y@x", Code: 10},
		{Email: "x@x", Code: 20},
	}

	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet swap should be allowed, got: %v", err)
	}
}

func TestUnique_BatchSet_DuplicateID_TransientConflictResolvedByLastWrite(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "u1@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "u2@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	// First write for id=1 temporarily conflicts with id=2 (u2@x),
	// second write resolves conflict and is the final value in the same tx.
	ids := []uint64{1, 1}
	vals := []*UniqueTestRec{
		{Email: "u2@x", Code: 1},
		{Email: "u3@x", Code: 1},
	}

	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet duplicate-id transient conflict should resolve by last write, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "u3@x" || v1.Code != 1 {
		t.Fatalf("unexpected final value for id=1: %#v", v1)
	}

	// Ensure unique index state is consistent.
	q := qx.Query(qx.EQ("email", "u2@x"))
	idsU2, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(email=u2@x): %v", err)
	}
	if len(idsU2) != 1 || idsU2[0] != 2 {
		t.Fatalf("unexpected owners for u2@x: %v", idsU2)
	}

	idsU3, err := db.QueryKeys(qx.Query(qx.EQ("email", "u3@x")))
	if err != nil {
		t.Fatalf("QueryKeys(email=u3@x): %v", err)
	}
	if len(idsU3) != 1 || idsU3[0] != 1 {
		t.Fatalf("unexpected owners for u3@x: %v", idsU3)
	}
}

func TestUnique_BatchSet_DuplicateID_FinalConflictStillRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "u1@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "u2@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	err := db.BatchSet(
		[]uint64{1, 1},
		[]*UniqueTestRec{
			{Email: "u3@x", Code: 1},
			{Email: "u2@x", Code: 1}, // final state conflicts with id=2
		},
	)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "u1@x" || v1.Code != 1 {
		t.Fatalf("id=1 should remain unchanged after rejected BatchSet, got: %#v", v1)
	}
}

func TestUnique_BatchSet_DuplicateNewID_FinalValueDrivesConstraint(t *testing.T) {
	t.Run("final_non_conflicting_value_passes", func(t *testing.T) {
		db, _ := openTempDBUint64Unique(t)

		if err := db.Set(2, &UniqueTestRec{Email: "u2@x", Code: 2}); err != nil {
			t.Fatalf("Set(2): %v", err)
		}

		err := db.BatchSet(
			[]uint64{1, 1},
			[]*UniqueTestRec{
				{Email: "u2@x", Code: 1}, // transient conflict
				{Email: "u3@x", Code: 1}, // final value should win
			},
		)
		if err != nil {
			t.Fatalf("BatchSet should succeed with final non-conflicting value, got: %v", err)
		}

		v1, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v1 == nil || v1.Email != "u3@x" {
			t.Fatalf("unexpected id=1 value: %#v", v1)
		}
	})

	t.Run("final_conflicting_value_fails", func(t *testing.T) {
		db, _ := openTempDBUint64Unique(t)

		if err := db.Set(2, &UniqueTestRec{Email: "u2@x", Code: 2}); err != nil {
			t.Fatalf("Set(2): %v", err)
		}

		err := db.BatchSet(
			[]uint64{1, 1},
			[]*UniqueTestRec{
				{Email: "u3@x", Code: 1},
				{Email: "u2@x", Code: 1}, // final value conflicts
			},
		)
		if err == nil || !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("expected ErrUniqueViolation, got: %v", err)
		}

		v1, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v1 != nil {
			t.Fatalf("id=1 should not be inserted on failed BatchSet, got: %#v", v1)
		}
	})
}

func TestUnique_RandomMixedWrites_ModelConsistency(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{AutoBatchMax: 1})

	type modelRec struct {
		Email string
		Code  int
		Tags  []string
	}

	const (
		seed  = int64(20260324)
		steps = 160
	)
	r := rand.New(rand.NewSource(seed))

	model := make(map[uint64]modelRec)
	seenEmails := make(map[string]struct{}, 256)

	copyModel := func(src map[uint64]modelRec) map[uint64]modelRec {
		out := make(map[uint64]modelRec, len(src))
		for id, rec := range src {
			cp := rec
			cp.Tags = slices.Clone(rec.Tags)
			out[id] = cp
		}
		return out
	}

	hasUniqueViolation := func(state map[uint64]modelRec) bool {
		byEmail := make(map[string]uint64, len(state))
		byCode := make(map[int]uint64, len(state))
		for id, rec := range state {
			if prev, ok := byEmail[rec.Email]; ok && prev != id {
				return true
			}
			byEmail[rec.Email] = id
			if prev, ok := byCode[rec.Code]; ok && prev != id {
				return true
			}
			byCode[rec.Code] = id
		}
		return false
	}

	applyPatch := func(rec modelRec, patch []Field) modelRec {
		for _, f := range patch {
			switch f.Name {
			case "email":
				rec.Email = f.Value.(string)
			case "code":
				rec.Code = f.Value.(int)
			}
		}
		return rec
	}

	checkState := func(step int) {
		t.Helper()

		cnt, err := db.Count()
		if err != nil {
			t.Fatalf("step=%d Count(): %v", step, err)
		}
		if cnt != uint64(len(model)) {
			t.Fatalf("step=%d count mismatch: got=%d want=%d", step, cnt, len(model))
		}

		liveEmails := make(map[string]struct{}, len(model))
		for id, exp := range model {
			v, err := db.Get(id)
			if err != nil {
				t.Fatalf("step=%d Get(%d): %v", step, id, err)
			}
			if v == nil {
				t.Fatalf("step=%d Get(%d): nil", step, id)
			}
			if v.Email != exp.Email || v.Code != exp.Code || !slices.Equal(v.Tags, exp.Tags) {
				t.Fatalf("step=%d value mismatch id=%d: got=%#v want=%#v", step, id, v, exp)
			}

			ids, err := db.QueryKeys(qx.Query(qx.EQ("email", exp.Email)))
			if err != nil {
				t.Fatalf("step=%d QueryKeys(email=%q): %v", step, exp.Email, err)
			}
			if len(ids) != 1 || ids[0] != id {
				t.Fatalf("step=%d email index mismatch for %q: %v", step, exp.Email, ids)
			}

			ids, err = db.QueryKeys(qx.Query(qx.EQ("code", exp.Code)))
			if err != nil {
				t.Fatalf("step=%d QueryKeys(code=%d): %v", step, exp.Code, err)
			}
			if len(ids) != 1 || ids[0] != id {
				t.Fatalf("step=%d code index mismatch for %d: %v", step, exp.Code, ids)
			}

			liveEmails[exp.Email] = struct{}{}
		}

		staleChecks := 0
		for email := range seenEmails {
			if _, live := liveEmails[email]; live {
				continue
			}
			ids, err := db.QueryKeys(qx.Query(qx.EQ("email", email)))
			if err != nil {
				t.Fatalf("step=%d QueryKeys(stale email=%q): %v", step, email, err)
			}
			if len(ids) != 0 {
				t.Fatalf("step=%d stale email %q still indexed: %v", step, email, ids)
			}
			staleChecks++
			if staleChecks >= 24 {
				break
			}
		}
	}

	randRec := func() modelRec {
		return modelRec{
			Email: fmt.Sprintf("u%02d@x", r.Intn(28)),
			Code:  1 + r.Intn(28),
			Tags:  []string{fmt.Sprintf("g-%d", r.Intn(6))},
		}
	}
	randPatch := func() []Field {
		out := make([]Field, 0, 2)
		if r.Intn(2) == 0 {
			out = append(out, Field{Name: "email", Value: fmt.Sprintf("u%02d@x", r.Intn(28))})
		}
		if r.Intn(2) == 0 {
			out = append(out, Field{Name: "code", Value: 1 + r.Intn(28)})
		}
		if len(out) == 0 {
			if r.Intn(2) == 0 {
				out = append(out, Field{Name: "email", Value: fmt.Sprintf("u%02d@x", r.Intn(28))})
			} else {
				out = append(out, Field{Name: "code", Value: 1 + r.Intn(28)})
			}
		}
		return out
	}

	for step := 0; step < steps; step++ {
		switch r.Intn(6) {
		case 0: // Set
			id := uint64(1 + r.Intn(48))
			rec := randRec()
			seenEmails[rec.Email] = struct{}{}

			next := copyModel(model)
			next[id] = rec
			wantUniqueErr := hasUniqueViolation(next)

			err := db.Set(id, &UniqueTestRec{
				Email: rec.Email,
				Code:  rec.Code,
				Tags:  slices.Clone(rec.Tags),
			})

			if wantUniqueErr {
				if err == nil || !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("step=%d Set expected ErrUniqueViolation, got: %v", step, err)
				}
			} else {
				if err != nil {
					t.Fatalf("step=%d Set unexpected err: %v", step, err)
				}
				model = next
			}

		case 1: // BatchSet (with possible duplicate ids)
			n := 2 + r.Intn(5)
			ids := make([]uint64, n)
			vals := make([]*UniqueTestRec, n)
			next := copyModel(model)
			for i := 0; i < n; i++ {
				id := uint64(1 + r.Intn(48))
				rec := randRec()
				ids[i] = id
				vals[i] = &UniqueTestRec{
					Email: rec.Email,
					Code:  rec.Code,
					Tags:  slices.Clone(rec.Tags),
				}
				seenEmails[rec.Email] = struct{}{}
				next[id] = rec
			}
			wantUniqueErr := hasUniqueViolation(next)

			err := db.BatchSet(ids, vals)
			if wantUniqueErr {
				if err == nil || !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("step=%d BatchSet expected ErrUniqueViolation, got: %v", step, err)
				}
			} else {
				if err != nil {
					t.Fatalf("step=%d BatchSet unexpected err: %v", step, err)
				}
				model = next
			}

		case 2: // Patch
			id := uint64(1 + r.Intn(48))
			patch := randPatch()
			next := copyModel(model)
			if cur, ok := next[id]; ok {
				next[id] = applyPatch(cur, patch)
			}
			wantUniqueErr := hasUniqueViolation(next)

			err := db.Patch(id, patch)
			if wantUniqueErr {
				if err == nil || !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("step=%d Patch expected ErrUniqueViolation, got: %v patch=%v", step, err, patch)
				}
			} else {
				if err != nil {
					t.Fatalf("step=%d Patch unexpected err: %v patch=%v", step, err, patch)
				}
				model = next
			}

		case 3: // BatchPatch (with possible duplicate ids)
			n := 2 + r.Intn(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.Intn(48))
			}
			patch := randPatch()
			next := copyModel(model)
			for _, id := range ids {
				if cur, ok := next[id]; ok {
					next[id] = applyPatch(cur, patch)
				}
			}
			wantUniqueErr := hasUniqueViolation(next)

			err := db.BatchPatch(ids, patch)
			if wantUniqueErr {
				if err == nil || !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("step=%d BatchPatch expected ErrUniqueViolation, got: %v patch=%v ids=%v", step, err, patch, ids)
				}
			} else {
				if err != nil {
					t.Fatalf("step=%d BatchPatch unexpected err: %v patch=%v ids=%v", step, err, patch, ids)
				}
				model = next
			}

		case 4: // Delete
			id := uint64(1 + r.Intn(48))
			if err := db.Delete(id); err != nil {
				t.Fatalf("step=%d Delete(%d): %v", step, id, err)
			}
			delete(model, id)

		default: // BatchDelete (with possible duplicate ids)
			n := 1 + r.Intn(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.Intn(48))
			}
			if err := db.BatchDelete(ids); err != nil {
				t.Fatalf("step=%d BatchDelete(%v): %v", step, ids, err)
			}
			for _, id := range ids {
				delete(model, id)
			}
		}

		if step%17 == 0 || step == steps-1 {
			checkState(step)
		}
	}
}

func TestUnique_SharedAutoBatchRandomMixedWrites_ModelConsistency(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AnalyzeInterval:   -1,
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	type modelRec struct {
		Email string
		Code  int
		Tags  []string
	}

	cloneRec := func(src *UniqueTestRec) modelRec {
		return modelRec{
			Email: src.Email,
			Code:  src.Code,
			Tags:  slices.Clone(src.Tags),
		}
	}
	makeRec := func(i int) modelRec {
		tags := []string{"shared", fmt.Sprintf("g-%d", i%5)}
		if i%3 == 0 {
			tags = append(tags, "hot")
		}
		return modelRec{
			Email: fmt.Sprintf("shared-%03d@x", i),
			Code:  1000 + i,
			Tags:  tags,
		}
	}
	patchFor := func(rec modelRec) []Field {
		return []Field{
			{Name: "email", Value: rec.Email},
			{Name: "code", Value: rec.Code},
			{Name: "tags", Value: slices.Clone(rec.Tags)},
		}
	}

	model := make(map[uint64]modelRec, 48)
	for i := 1; i <= 12; i++ {
		id := uint64(i)
		rec := makeRec(i)
		if err := db.Set(id, &UniqueTestRec{
			Email: rec.Email,
			Code:  rec.Code,
			Tags:  slices.Clone(rec.Tags),
		}, NoBatch[uint64, UniqueTestRec]); err != nil {
			t.Fatalf("seed Set(%d): %v", id, err)
		}
		model[id] = rec
	}

	type event struct {
		id     uint64
		delete bool
		rec    modelRec
	}
	var (
		mu     sync.Mutex
		events []event
	)
	recordCommit := func(_ *bbolt.Tx, id uint64, _, newValue *UniqueTestRec) error {
		mu.Lock()
		if newValue == nil {
			events = append(events, event{id: id, delete: true})
		} else {
			events = append(events, event{id: id, rec: cloneRec(newValue)})
		}
		mu.Unlock()
		return nil
	}

	type op struct {
		kind  int
		id    uint64
		rec   modelRec
		patch []Field
	}
	const opCount = 128
	ops := make([]op, opCount)
	for i := range ops {
		switch i % 8 {
		case 0:
			rec := makeRec(2000 + i)
			rec.Email = model[1].Email
			ops[i] = op{kind: 0, id: uint64(13 + (i*5)%28), rec: rec}
		case 1:
			rec := makeRec(2000 + i)
			rec.Code = model[1].Code
			ops[i] = op{kind: 1, id: uint64(2 + (i % 11)), patch: patchFor(rec)}
		case 2, 5:
			ops[i] = op{kind: 0, id: uint64(2 + (i*7)%39), rec: makeRec(3000 + i)}
		case 3, 6:
			rec := makeRec(3000 + i)
			ops[i] = op{kind: 1, id: uint64(2 + (i*7)%39), patch: patchFor(rec)}
		default:
			ops[i] = op{kind: 2, id: uint64(20 + (i*5)%21)}
		}
	}

	before := db.AutoBatchStats()
	start := make(chan struct{})
	errCh := make(chan error, len(ops))
	var wg sync.WaitGroup
	for i := range ops {
		op := ops[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var err error
			switch op.kind {
			case 0:
				rec := UniqueTestRec{
					Email: op.rec.Email,
					Code:  op.rec.Code,
					Tags:  slices.Clone(op.rec.Tags),
				}
				err = db.Set(op.id, &rec, BeforeCommit(recordCommit))
			case 1:
				err = db.Patch(op.id, op.patch, BeforeCommit(recordCommit))
			default:
				err = db.Delete(op.id, BeforeCommit(recordCommit))
			}
			if err != nil && !errors.Is(err, ErrUniqueViolation) {
				errCh <- fmt.Errorf("kind=%d id=%d: %w", op.kind, op.id, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := range events {
		ev := events[i]
		if ev.delete {
			delete(model, ev.id)
		} else {
			model[ev.id] = ev.rec
		}
	}

	if got, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != uint64(len(model)) {
		t.Fatalf("Count = %d, want %d", got, len(model))
	}

	byEmail := make(map[string]uint64, len(model))
	byCode := make(map[int]uint64, len(model))
	for id, exp := range model {
		if prev, ok := byEmail[exp.Email]; ok && prev != id {
			t.Fatalf("model duplicate email %q: ids %d and %d", exp.Email, prev, id)
		}
		byEmail[exp.Email] = id
		if prev, ok := byCode[exp.Code]; ok && prev != id {
			t.Fatalf("model duplicate code %d: ids %d and %d", exp.Code, prev, id)
		}
		byCode[exp.Code] = id

		got, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got == nil || got.Email != exp.Email || got.Code != exp.Code || !slices.Equal(got.Tags, exp.Tags) {
			t.Fatalf("Get(%d) = %#v, want %#v", id, got, exp)
		}

		ids, err := db.QueryKeys(qx.Query(qx.EQ("email", exp.Email)))
		if err != nil {
			t.Fatalf("QueryKeys(email=%q): %v", exp.Email, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("email index mismatch for %q: got %v want [%d]", exp.Email, ids, id)
		}
		ids, err = db.QueryKeys(qx.Query(qx.EQ("code", exp.Code)))
		if err != nil {
			t.Fatalf("QueryKeys(code=%d): %v", exp.Code, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("code index mismatch for %d: got %v want [%d]", exp.Code, ids, id)
		}
	}
	for id := uint64(1); id <= 40; id++ {
		if _, ok := model[id]; ok {
			continue
		}
		got, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got != nil {
			t.Fatalf("Get(%d) = %#v, want nil", id, got)
		}
	}

	gotHot, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"hot"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has hot): %v", err)
	}
	wantHot := make([]uint64, 0, len(model))
	for id, rec := range model {
		if slices.Contains(rec.Tags, "hot") {
			wantHot = append(wantHot, id)
		}
	}
	slices.Sort(gotHot)
	slices.Sort(wantHot)
	if !slices.Equal(gotHot, wantHot) {
		t.Fatalf("hot tag index mismatch: got %v want %v", gotHot, wantHot)
	}

	after := db.AutoBatchStats()
	if after.MultiRequestBatches <= before.MultiRequestBatches {
		t.Fatalf("expected shared auto-batch path, before=%+v after=%+v", before, after)
	}
	if after.UniqueRejected <= before.UniqueRejected {
		t.Fatalf("expected queued unique rejects, before=%+v after=%+v", before, after)
	}
}

func TestUnique_DeleteThenReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2}); err != nil {
		t.Fatalf("Set(2) after delete should be allowed: %v", err)
	}
}

func TestUnique_ConcurrentSet_SingleWinner(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	const workers = 32

	var (
		wg      sync.WaitGroup
		success atomic.Int64
		winner  atomic.Uint64
	)
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		id := uint64(i + 1)
		code := i + 1
		wg.Add(1)
		go func(id uint64, code int) {
			defer wg.Done()
			err := db.Set(id, &UniqueTestRec{
				Email: "same@x",
				Code:  code,
			})
			if err == nil {
				success.Add(1)
				winner.Store(id)
				return
			}
			if !errors.Is(err, ErrUniqueViolation) {
				errCh <- fmt.Errorf("unexpected set error for id=%d: %w", id, err)
			}
		}(id, code)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if got := success.Load(); got != 1 {
		t.Fatalf("expected exactly one successful insert, got %d", got)
	}

	total, err := db.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected total count=1, got %d", total)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "same@x")))
	if err != nil {
		t.Fatalf("QueryKeys(email=same@x): %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected exactly one id for duplicate email race, got %v", ids)
	}

	gotWinner := winner.Load()
	if ids[0] != gotWinner {
		t.Fatalf("winner mismatch: set winner=%d query winner=%d", gotWinner, ids[0])
	}

	v, err := db.Get(gotWinner)
	if err != nil {
		t.Fatalf("Get(winner): %v", err)
	}
	if v == nil || v.Email != "same@x" {
		t.Fatalf("unexpected winner payload: %#v", v)
	}
}

func TestUnique_BatchPatch_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{1, 2}
	err := db.BatchPatch(ids, []Field{{Name: "email", Value: "dup@x"}}, PatchStrict)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "a@x" || v1.Code != 1 {
		t.Fatalf("id=1 must stay unchanged after rejected BatchPatch(..., PatchStrict), got: %#v", v1)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "b@x" || v2.Code != 2 {
		t.Fatalf("id=2 must stay unchanged after rejected BatchPatch(..., PatchStrict), got: %#v", v2)
	}
}

func TestUnique_BatchPatch_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(3, &UniqueTestRec{Email: "c@x", Code: 3}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ids := []uint64{2, 3}
	err := db.BatchPatch(ids, []Field{{Name: "code", Value: 1}}, PatchStrict)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "b@x" || v2.Code != 2 {
		t.Fatalf("id=2 must stay unchanged after rejected BatchPatch(..., PatchStrict), got: %#v", v2)
	}
	v3, err := db.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if v3 == nil || v3.Email != "c@x" || v3.Code != 3 {
		t.Fatalf("id=3 must stay unchanged after rejected BatchPatch(..., PatchStrict), got: %#v", v3)
	}

	owners, err := db.QueryKeys(qx.Query(qx.EQ("code", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(code=1): %v", err)
	}
	if len(owners) != 1 || owners[0] != 1 {
		t.Fatalf("unexpected owners for code=1 after failed BatchPatch(..., PatchStrict): %v", owners)
	}
}

func TestUnique_BatchPatch_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "email", Value: "tmp@x"}}, PatchStrict); err != nil {
		t.Fatalf("Patch(1, ..., PatchStrict)->tmp: %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "email", Value: "x@x"}}, PatchStrict); err != nil {
		t.Fatalf("Patch(2, ..., PatchStrict)->x: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "email", Value: "y@x"}}, PatchStrict); err != nil {
		t.Fatalf("Patch(1, ..., PatchStrict)->y: %v", err)
	}
	v1, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Email != "y@x" || v2.Email != "x@x" {
		t.Fatalf("swap result mismatch: v1=%q v2=%q", v1.Email, v2.Email)
	}
}

func TestUnique_OptNilReleasesAndReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)
	s := "same"

	err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: &s})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation on Opt duplicate, got: %v", err)
	}
	if err = db.Patch(1, []Field{{Name: "opt", Value: nil}}, PatchStrict); err != nil {
		t.Fatalf("Patch(1, ..., PatchStrict) opt=nil: %v", err)
	}
	if err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s}); err != nil {
		t.Fatalf("Set(2) after releasing opt should be allowed: %v", err)
	}
}
