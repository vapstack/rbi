package rbi

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/qx"
)

func TestUnique_QueryEQ_Max1Equivalent(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	qNoLimit := qx.Query(qx.EQ("email", "a@x"))
	qLimit1 := qx.Query(qx.EQ("email", "a@x")).Max(1)

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
	).Max(1)

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
	opts := optsWithDefaults(&Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
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
			q:       qx.Query(qx.EQ("email", "a@x")).Max(1),
			wantLen: 1,
		},
		{
			name:    "miss_no_limit",
			q:       qx.Query(qx.EQ("email", "missing@x")),
			wantLen: 0,
		},
		{
			name:    "miss_limit_1",
			q:       qx.Query(qx.EQ("email", "missing@x")).Max(1),
			wantLen: 0,
		},
		{
			name: "unsatisfiable_conjunction_limit_1",
			q: qx.Query(
				qx.EQ("email", "a@x"),
				qx.HAS("tags", []string{"missing"}),
			).Max(1),
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

func TestUnique_Count_WithUniqueAnchor(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	c1, err := db.Count(qx.Query(qx.EQ("email", "a@x")))
	if err != nil {
		t.Fatalf("Count(eq unique): %v", err)
	}
	if c1 != 1 {
		t.Fatalf("expected count=1, got %d", c1)
	}

	c2, err := db.Count(qx.Query(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 1),
	))
	if err != nil {
		t.Fatalf("Count(and hit): %v", err)
	}
	if c2 != 1 {
		t.Fatalf("expected count=1, got %d", c2)
	}

	c3, err := db.Count(qx.Query(
		qx.EQ("email", "a@x"),
		qx.EQ("code", 2),
	))
	if err != nil {
		t.Fatalf("Count(and miss): %v", err)
	}
	if c3 != 0 {
		t.Fatalf("expected count=0, got %d", c3)
	}
}

func TestUnique_Set_DuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_Set_DuplicateRejected_WithSnapshotDelta(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_Set_SameRecordUpdateAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_Patch_ConflictingUpdateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.PatchStrict(2, []Field{{Name: "email", Value: "a@x"}})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_NilAllowedMultipleTimes(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_OptDuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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
		db, _ := openTempDBUint64Unique(t, nil)

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
		db, _ := openTempDBUint64Unique(t, nil)

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

func TestUnique_BatchDeleteThenSet_ReusesFreedValueInSameTx(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	newVal := &UniqueTestRec{Email: "a@x", Code: 2}
	enc := getEncodeBuf()
	if err := db.encode(newVal, enc); err != nil {
		releaseEncodeBuf(enc)
		t.Fatalf("encode set payload: %v", err)
	}
	payload := append([]byte(nil), enc.Bytes()...)
	releaseEncodeBuf(enc)

	delReq := &combineRequest[uint64, UniqueTestRec]{
		op:   combineDelete,
		id:   1,
		done: make(chan error, 1),
	}
	setReq := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         2,
		setValue:   newVal,
		setPayload: payload,
		done:       make(chan error, 1),
	}

	db.executeCombinedBatch([]*combineRequest[uint64, UniqueTestRec]{delReq, setReq})

	if err := <-delReq.done; err != nil {
		t.Fatalf("batch delete request failed: %v", err)
	}
	if err := <-setReq.done; err != nil {
		t.Fatalf("batch set request failed: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 != nil {
		t.Fatalf("id=1 must be deleted, got: %#v", v1)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "a@x" || v2.Code != 2 {
		t.Fatalf("id=2 unexpected value after batch reuse: %#v", v2)
	}

	idsA, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x")))
	if err != nil {
		t.Fatalf("QueryKeys(email=a@x): %v", err)
	}
	if len(idsA) != 1 || idsA[0] != 2 {
		t.Fatalf("unexpected owners for email=a@x: %v", idsA)
	}
}

func TestUnique_BatchPartialReject_PreservesAcceptedOpsAndIndex(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(3, &UniqueTestRec{Email: "c@x", Code: 3}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	badVal := &UniqueTestRec{Email: "a@x", Code: 3}
	badEnc := getEncodeBuf()
	if err := db.encode(badVal, badEnc); err != nil {
		releaseEncodeBuf(badEnc)
		t.Fatalf("encode bad payload: %v", err)
	}
	badPayload := append([]byte(nil), badEnc.Bytes()...)
	releaseEncodeBuf(badEnc)

	goodVal := &UniqueTestRec{Email: "d@x", Code: 2}
	goodEnc := getEncodeBuf()
	if err := db.encode(goodVal, goodEnc); err != nil {
		releaseEncodeBuf(goodEnc)
		t.Fatalf("encode good payload: %v", err)
	}
	goodPayload := append([]byte(nil), goodEnc.Bytes()...)
	releaseEncodeBuf(goodEnc)

	badReq := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         3,
		setValue:   badVal,
		setPayload: badPayload,
		done:       make(chan error, 1),
	}
	goodReq := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         2,
		setValue:   goodVal,
		setPayload: goodPayload,
		done:       make(chan error, 1),
	}

	db.executeCombinedBatch([]*combineRequest[uint64, UniqueTestRec]{badReq, goodReq})

	if err := <-badReq.done; err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bad request must fail with ErrUniqueViolation, got: %v", err)
	}
	if err := <-goodReq.done; err != nil {
		t.Fatalf("good request must succeed, got: %v", err)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "d@x" {
		t.Fatalf("id=2 must be updated to d@x, got: %#v", v2)
	}

	v3, err := db.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if v3 == nil || v3.Email != "c@x" {
		t.Fatalf("id=3 must stay unchanged after rejected op, got: %#v", v3)
	}

	checkOwner := func(email string, want uint64) {
		t.Helper()
		ids, qerr := db.QueryKeys(qx.Query(qx.EQ("email", email)))
		if qerr != nil {
			t.Fatalf("QueryKeys(email=%s): %v", email, qerr)
		}
		if len(ids) != 1 || ids[0] != want {
			t.Fatalf("unexpected owner for %s: got=%v want=[%d]", email, ids, want)
		}
	}
	checkOwner("a@x", 1)
	checkOwner("c@x", 3)
	checkOwner("d@x", 2)
}

func TestUnique_ExecuteBatch_MixedOps_MatchesSequentialModel(t *testing.T) {
	dbBatch, _ := openTempDBUint64Unique(t, nil)
	dbSeq, _ := openTempDBUint64Unique(t, &Options{BatchMax: 1})

	const idSpace = 32

	cloneUniqueRec := func(v *UniqueTestRec) *UniqueTestRec {
		if v == nil {
			return nil
		}
		cp := *v
		cp.Tags = slices.Clone(v.Tags)
		if v.Opt != nil {
			s := *v.Opt
			cp.Opt = &s
		}
		return &cp
	}
	cloneFieldValue := func(v any) any {
		switch x := v.(type) {
		case []string:
			return slices.Clone(x)
		default:
			return x
		}
	}
	cloneFields := func(in []Field) []Field {
		out := make([]Field, len(in))
		for i := range in {
			out[i].Name = in[i].Name
			out[i].Value = cloneFieldValue(in[i].Value)
		}
		return out
	}
	errClass := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case errors.Is(err, ErrUniqueViolation):
			return "unique"
		case errors.Is(err, ErrRecordNotFound):
			return "not_found"
		default:
			return err.Error()
		}
	}
	compareState := func(step int) {
		t.Helper()

		cntBatch, err := dbBatch.Count(nil)
		if err != nil {
			t.Fatalf("step=%d batch Count: %v", step, err)
		}
		cntSeq, err := dbSeq.Count(nil)
		if err != nil {
			t.Fatalf("step=%d seq Count: %v", step, err)
		}
		if cntBatch != cntSeq {
			t.Fatalf("step=%d count mismatch: batch=%d seq=%d", step, cntBatch, cntSeq)
		}

		for id := uint64(1); id <= idSpace; id++ {
			vb, err := dbBatch.Get(id)
			if err != nil {
				t.Fatalf("step=%d batch Get(%d): %v", step, id, err)
			}
			vs, err := dbSeq.Get(id)
			if err != nil {
				t.Fatalf("step=%d seq Get(%d): %v", step, id, err)
			}
			if vb == nil && vs == nil {
				continue
			}
			if vb == nil || vs == nil {
				t.Fatalf("step=%d id=%d nil mismatch: batch=%#v seq=%#v", step, id, vb, vs)
			}
			if !reflect.DeepEqual(*vb, *vs) {
				t.Fatalf("step=%d id=%d value mismatch\nbatch=%#v\nseq=%#v", step, id, vb, vs)
			}
		}

		emailPool := []string{"u0@x", "u1@x", "u2@x", "u3@x", "u4@x", "u5@x", "u6@x", "u7@x", "u8@x", "u9@x"}
		for _, email := range emailPool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("email", email)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(email=%q): %v", step, email, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("email", email)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(email=%q): %v", step, email, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d email index mismatch for %q: batch=%v seq=%v", step, email, ib, is)
			}
		}
		for code := 1; code <= 14; code++ {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("code", code)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(code=%d): %v", step, code, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("code", code)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(code=%d): %v", step, code, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d code index mismatch for %d: batch=%v seq=%v", step, code, ib, is)
			}
		}
	}

	seed := []*UniqueTestRec{
		{Email: "u0@x", Code: 1, Tags: []string{"t0"}},
		{Email: "u1@x", Code: 2, Tags: []string{"t1"}},
		{Email: "u2@x", Code: 3, Tags: []string{"t2"}},
		{Email: "u3@x", Code: 4, Tags: []string{"t3"}},
	}
	for i, v := range seed {
		id := uint64(i + 1)
		if err := dbBatch.Set(id, cloneUniqueRec(v)); err != nil {
			t.Fatalf("seed batch Set(%d): %v", id, err)
		}
		if err := dbSeq.Set(id, cloneUniqueRec(v)); err != nil {
			t.Fatalf("seed seq Set(%d): %v", id, err)
		}
	}
	compareState(0)

	type batchOp struct {
		kind  uint8 // 0=set, 1=patch_if_exists, 2=delete
		id    uint64
		value *UniqueTestRec
		patch []Field
	}

	r := rand.New(rand.NewSource(20260325))
	emailPool := []string{"u0@x", "u1@x", "u2@x", "u3@x", "u4@x", "u5@x", "u6@x", "u7@x", "u8@x", "u9@x"}
	tagPool := []string{"t0", "t1", "t2", "t3", "t4", "t5"}

	for step := 1; step <= 140; step++ {
		n := 2 + r.Intn(6)
		used := make(map[uint64]struct{}, n)
		ops := make([]batchOp, 0, n)

		for len(ops) < n {
			id := uint64(1 + r.Intn(idSpace))
			if _, ok := used[id]; ok {
				continue
			}
			used[id] = struct{}{}

			switch r.Intn(3) {
			case 0:
				ops = append(ops, batchOp{
					kind: 0,
					id:   id,
					value: &UniqueTestRec{
						Email: emailPool[r.Intn(len(emailPool))],
						Code:  1 + r.Intn(14),
						Tags: []string{
							tagPool[r.Intn(len(tagPool))],
							tagPool[r.Intn(len(tagPool))],
						},
					},
				})
			case 1:
				var patch []Field
				switch r.Intn(3) {
				case 0:
					patch = []Field{{Name: "email", Value: emailPool[r.Intn(len(emailPool))]}}
				case 1:
					patch = []Field{{Name: "code", Value: 1 + r.Intn(14)}}
				default:
					patch = []Field{{Name: "tags", Value: []string{tagPool[r.Intn(len(tagPool))], tagPool[r.Intn(len(tagPool))]}}}
				}
				ops = append(ops, batchOp{
					kind:  1,
					id:    id,
					patch: patch,
				})
			default:
				ops = append(ops, batchOp{
					kind: 2,
					id:   id,
				})
			}
		}

		reqs := make([]*combineRequest[uint64, UniqueTestRec], 0, len(ops))
		for _, op := range ops {
			switch op.kind {
			case 0:
				buf := getEncodeBuf()
				if err := dbBatch.encode(op.value, buf); err != nil {
					releaseEncodeBuf(buf)
					t.Fatalf("step=%d encode set value: %v", step, err)
				}
				payload := append([]byte(nil), buf.Bytes()...)
				releaseEncodeBuf(buf)
				reqs = append(reqs, &combineRequest[uint64, UniqueTestRec]{
					op:         combineSet,
					id:         op.id,
					setValue:   cloneUniqueRec(op.value),
					setPayload: payload,
					done:       make(chan error, 1),
				})
			case 1:
				reqs = append(reqs, &combineRequest[uint64, UniqueTestRec]{
					op:                 combinePatch,
					id:                 op.id,
					patch:              cloneFields(op.patch),
					patchIgnoreUnknown: true,
					patchAllowMissing:  true,
					done:               make(chan error, 1),
				})
			default:
				reqs = append(reqs, &combineRequest[uint64, UniqueTestRec]{
					op:   combineDelete,
					id:   op.id,
					done: make(chan error, 1),
				})
			}
		}

		dbBatch.executeCombinedBatch(reqs)

		batchErrs := make([]error, len(reqs))
		for i := range reqs {
			batchErrs[i] = <-reqs[i].done
		}

		seqErrs := make([]error, len(ops))
		for i, op := range ops {
			switch op.kind {
			case 0:
				seqErrs[i] = dbSeq.Set(op.id, cloneUniqueRec(op.value))
			case 1:
				seqErrs[i] = dbSeq.PatchIfExists(op.id, cloneFields(op.patch))
			default:
				seqErrs[i] = dbSeq.Delete(op.id)
			}
		}

		for i := range ops {
			if errClass(batchErrs[i]) != errClass(seqErrs[i]) {
				t.Fatalf(
					"step=%d op=%d kind=%d id=%d error class mismatch: batch=%v seq=%v",
					step, i, ops[i].kind, ops[i].id, batchErrs[i], seqErrs[i],
				)
			}
		}

		compareState(step)
	}
}

func TestUnique_RandomMixedWrites_ModelConsistency(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, &Options{BatchMax: 1})

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

		cnt, err := db.Count(nil)
		if err != nil {
			t.Fatalf("step=%d Count(nil): %v", step, err)
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
		if step > 0 && step%23 == 0 {
			if err := db.RebuildIndex(); err != nil {
				t.Fatalf("step=%d RebuildIndex: %v", step, err)
			}
		}

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

		case 2: // PatchIfExists
			id := uint64(1 + r.Intn(48))
			patch := randPatch()
			next := copyModel(model)
			if cur, ok := next[id]; ok {
				next[id] = applyPatch(cur, patch)
			}
			wantUniqueErr := hasUniqueViolation(next)

			err := db.PatchIfExists(id, patch)
			if wantUniqueErr {
				if err == nil || !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("step=%d PatchIfExists expected ErrUniqueViolation, got: %v patch=%v", step, err, patch)
				}
			} else {
				if err != nil {
					t.Fatalf("step=%d PatchIfExists unexpected err: %v patch=%v", step, err, patch)
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

func TestUnique_DeleteThenReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

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
	db, _ := openTempDBUint64Unique(t, nil)

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

	total, err := db.Count(nil)
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
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{1, 2}
	err := db.BatchPatchStrict(ids, []Field{{Name: "email", Value: "dup@x"}})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "a@x" || v1.Code != 1 {
		t.Fatalf("id=1 must stay unchanged after rejected BatchPatchStrict, got: %#v", v1)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "b@x" || v2.Code != 2 {
		t.Fatalf("id=2 must stay unchanged after rejected BatchPatchStrict, got: %#v", v2)
	}
}

func TestUnique_BatchPatch_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

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
	err := db.BatchPatchStrict(ids, []Field{{Name: "code", Value: 1}})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "b@x" || v2.Code != 2 {
		t.Fatalf("id=2 must stay unchanged after rejected BatchPatchStrict, got: %#v", v2)
	}
	v3, err := db.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if v3 == nil || v3.Email != "c@x" || v3.Code != 3 {
		t.Fatalf("id=3 must stay unchanged after rejected BatchPatchStrict, got: %#v", v3)
	}

	owners, err := db.QueryKeys(qx.Query(qx.EQ("code", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(code=1): %v", err)
	}
	if len(owners) != 1 || owners[0] != 1 {
		t.Fatalf("unexpected owners for code=1 after failed BatchPatchStrict: %v", owners)
	}
}

func TestUnique_BatchPatch_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.PatchStrict(1, []Field{{Name: "email", Value: "tmp@x"}}); err != nil {
		t.Fatalf("PatchStrict(1)->tmp: %v", err)
	}
	if err := db.PatchStrict(2, []Field{{Name: "email", Value: "x@x"}}); err != nil {
		t.Fatalf("PatchStrict(2)->x: %v", err)
	}
	if err := db.PatchStrict(1, []Field{{Name: "email", Value: "y@x"}}); err != nil {
		t.Fatalf("PatchStrict(1)->y: %v", err)
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
	db, _ := openTempDBUint64Unique(t, nil)
	s := "same"

	err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: &s})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation on Opt duplicate, got: %v", err)
	}
	if err = db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict(1) opt=nil: %v", err)
	}
	if err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s}); err != nil {
		t.Fatalf("Set(2) after releasing opt should be allowed: %v", err)
	}
}
