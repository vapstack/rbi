package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

type RecU struct {
	Email string   `db:"email" rbi:"unique"`
	Code  int      `db:"code"  rbi:"unique"`
	Opt   *string  `db:"opt"   rbi:"unique"`
	Tags  []string `db:"tags"`
}

func openTempDBUint64Unique(t *testing.T, opts *Options[uint64, RecU]) (*DB[uint64, RecU], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_unique.db")
	db, err := Open[uint64, RecU](path, 0o600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func TestUnique_Set_DuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	err := db.Set(2, &RecU{Email: "a@x", Code: 2})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_Set_SameRecordUpdateAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1) same value: %v", err)
	}
}

func TestUnique_Patch_ConflictingUpdateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
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

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
}

func TestUnique_OptDuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	v := "same"

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1, Opt: &v}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	err := db.Set(2, &RecU{Email: "b@x", Code: 2, Opt: &v})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	ids := []uint64{1, 2}
	vals := []*RecU{
		{Email: "a@x", Code: 1},
		{Email: "a@x", Code: 2},
	}

	err := db.SetMany(ids, vals)
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	ids := []uint64{2, 3}
	vals := []*RecU{
		{Email: "b@x", Code: 2},
		{Email: "a@x", Code: 3},
	}

	err := db.SetMany(ids, vals)
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	ids := []uint64{1, 2}
	vals := []*RecU{
		{Email: "y@x", Code: 10},
		{Email: "x@x", Code: 20},
	}

	if err := db.SetMany(ids, vals); err != nil {
		t.Fatalf("SetMany swap should be allowed, got: %v", err)
	}
}

func TestUnique_DeleteThenReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}

	if err := db.Set(2, &RecU{Email: "a@x", Code: 2}); err != nil {
		t.Fatalf("Set(2) after delete should be allowed: %v", err)
	}
}

func TestUnique_Smoke_HasUniqueWorks(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)
	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	err := db.Set(2, &RecU{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation, got: %v", err)
	}
	_ = fmt.Sprintf("%v", db)
}

func TestUnique_PatchMany_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	ids := []uint64{1, 2}
	err := db.PatchManyStrict(ids, []Field{{Name: "email", Value: "dup@x"}})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_PatchMany_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(3, &RecU{Email: "c@x", Code: 3}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}
	ids := []uint64{2, 3}
	err := db.PatchManyStrict(ids, []Field{{Name: "code", Value: 1}})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_PatchMany_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &RecU{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &RecU{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
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
	v1, _ := db.Get(1)
	v2, _ := db.Get(2)
	if v1.Email != "y@x" || v2.Email != "x@x" {
		t.Fatalf("swap result mismatch: v1=%q v2=%q", v1.Email, v2.Email)
	}
}

func TestUnique_OptNilReleasesAndReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)
	s := "same"

	if err := db.Set(1, &RecU{Email: "a@x", Code: 1, Opt: &s}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	err := db.Set(2, &RecU{Email: "b@x", Code: 2, Opt: &s})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation on Opt duplicate, got: %v", err)
	}
	if err = db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict(1) opt=nil: %v", err)
	}
	if err = db.Set(2, &RecU{Email: "b@x", Code: 2, Opt: &s}); err != nil {
		t.Fatalf("Set(2) after releasing opt should be allowed: %v", err)
	}
}
