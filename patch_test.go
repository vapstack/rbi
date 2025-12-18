package rbi

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/vapstack/qx"
)

func TestDisableIndexing_RebuildIndex(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.DisableIndexing()
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set (noindex): %v", err)
	}

	_, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
	if !errors.Is(err, ErrIndexDisabled) {
		t.Fatalf("expected ErrIndexDisabled, got: %v", err)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	db.EnableIndexing()

	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
	if err != nil {
		t.Fatalf("QueryKeys after rebuild: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d (%v)", len(ids), ids)
	}
}

func TestPatchStrict_StructTags_NumConversion(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{
		Name:     "alice",
		Age:      10,
		Tags:     []string{"go"},
		FullName: "Alice A.",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "age", Value: 42.0}}); err != nil {
		t.Fatalf("PatchStrict age float->int: %v", err)
	}
	v, _ := db.Get(1)
	if v.Age != 42 {
		t.Fatalf("age not patched: got %d", v.Age)
	}

	if err := db.PatchStrict(1, []Field{{Name: "fullName", Value: "Alice Alpha"}}); err != nil {
		t.Fatalf("PatchStrict json tag: %v", err)
	}
	v, _ = db.Get(1)
	if v.FullName != "Alice Alpha" {
		t.Fatalf("full name not patched: got %q", v.FullName)
	}

	err := db.PatchStrict(1, []Field{{Name: "age", Value: 1.25}})
	if err == nil {
		t.Fatalf("expected error on float->int with fraction")
	}
	v, _ = db.Get(1)
	if v.Age != 42 {
		t.Fatalf("age changed despite failed patch: got %d", v.Age)
	}
}

func TestPatchStrict_NilRules(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	s := "opt"
	if err := db.Set(1, &Rec{
		Name: "alice", Age: 10, Opt: &s,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict opt=nil: %v", err)
	}
	v, _ := db.Get(1)
	if v.Opt != nil {
		t.Fatalf("expected opt=nil after patch")
	}

	err := db.PatchStrict(1, []Field{{Name: "age", Value: nil}})
	if err == nil {
		t.Fatalf("expected error for age=nil")
	}
}

func TestMakePatch_PreCommit_DeepCopy_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"a"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var patch []Field
	fn := db.MakePatch(&patch)

	orig := []string{"x", "y"}
	if err := db.PatchStrict(1, []Field{{Name: "tags", Value: orig}}, fn); err != nil {
		t.Fatalf("PatchStrict: %v", err)
	}

	orig[0] = "MUTATED"

	var gotTags []string
	for _, f := range patch {
		if f.Name == "tags" {
			gotTags, _ = f.Value.([]string)
		}
	}
	if len(gotTags) != 2 || gotTags[0] != "x" || gotTags[1] != "y" {
		t.Fatalf("expected deep-copied tags [x y], got %#v", gotTags)
	}
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	db, err := Open[uint64, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err = db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open[uint64, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open(reopen): %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}

	st := db2.Stats()
	if st.Keys != 2 {
		t.Fatalf("expected Stats.Keys=2, got %d", st.Keys)
	}
}
