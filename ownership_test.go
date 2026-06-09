package rbi

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func TestOwnershipE2E_ModelAfterChunkedPatchDeleteTransitions(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 420
	ids := make([]uint64, 0, rows)
	vals := make([]*Rec, 0, rows)
	model := make(map[uint64]Rec, rows)
	for i := 1; i <= rows; i++ {
		id := uint64(i)
		rec := &Rec{
			Meta:     Meta{Country: fmt.Sprintf("country-%02d", i%11)},
			Name:     fmt.Sprintf("name-%04d", i),
			Age:      20 + i%50,
			Score:    float64(i % 100),
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("tag-%04d", i), fmt.Sprintf("lane-%02d", i%7)},
			FullName: fmt.Sprintf("full-%04d", i),
		}
		ids = append(ids, id)
		vals = append(vals, rec)
		model[id] = ownershipE2ECopyRec(rec)
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	for i := range vals {
		vals[i].Name = "poison-name"
		vals[i].FullName = "poison-full"
		for j := range vals[i].Tags {
			vals[i].Tags[j] = "poison-tag"
		}
	}

	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("name", "name-0200")), func(rec Rec) bool {
		return rec.Name == "name-0200"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"tag-0200"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "tag-0200")
	})

	cohortTags := []string{"owned", "shared"}
	cohortPatch := []Field{
		{Name: "name", Value: "cohort"},
		{Name: "tags", Value: cohortTags},
	}
	if err := db.BatchPatch(ids, cohortPatch); err != nil {
		t.Fatalf("BatchPatch(cohort): %v", err)
	}
	cohortTags[0] = "poison-owned"
	cohortTags[1] = "poison-shared"
	for id, rec := range model {
		rec.Name = "cohort"
		rec.Tags = []string{"owned", "shared"}
		model[id] = rec
	}

	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("name", "name-0200")), func(rec Rec) bool {
		return rec.Name == "name-0200"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"tag-0200"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "tag-0200")
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("full_name", "full-0200")), func(rec Rec) bool {
		return rec.FullName == "full-0200"
	})

	emptyIDs := []uint64{2, 200, 418}
	if err := db.BatchPatch(emptyIDs, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(empty tags): %v", err)
	}
	for _, id := range emptyIDs {
		rec := model[id]
		rec.Tags = []string{}
		model[id] = rec
	}

	singleTags := []string{"owned"}
	singleIDs := []uint64{3, 201, 419}
	if err := db.BatchPatch(singleIDs, []Field{{Name: "tags", Value: singleTags}}); err != nil {
		t.Fatalf("BatchPatch(single tags): %v", err)
	}
	singleTags[0] = "poison-single"
	for _, id := range singleIDs {
		rec := model[id]
		rec.Tags = []string{"owned"}
		model[id] = rec
	}

	deleteIDs := []uint64{1, 2, 2, 3, 419, 420}
	if err := db.BatchDelete(deleteIDs); err != nil {
		t.Fatalf("BatchDelete(neighbors): %v", err)
	}
	for _, id := range deleteIDs {
		delete(model, id)
	}

	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("full_name", "full-0002")), func(rec Rec) bool {
		return rec.FullName == "full-0002"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("full_name", "full-0200")), func(rec Rec) bool {
		return rec.FullName == "full-0200"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"owned"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "owned")
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{})), func(rec Rec) bool {
		return len(rec.Tags) == 0
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"owned"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"owned"})
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"owned", "shared"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"owned", "shared"})
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("name", "cohort")), func(rec Rec) bool {
		return rec.Name == "cohort"
	})
}

func TestOwnershipE2E_StringKeyModelAfterPatchDeleteTransitions(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	const rows = 128
	keys := make([]string, 0, rows)
	vals := make([]*Rec, 0, rows)
	model := make(map[string]Rec, rows)
	for i := 1; i <= rows; i++ {
		key := fmt.Sprintf("key-%04d", i)
		rec := &Rec{
			Meta:     Meta{Country: fmt.Sprintf("country-%02d", i%5)},
			Name:     fmt.Sprintf("str-name-%04d", i),
			Age:      30 + i%40,
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("str-tag-%04d", i)},
			FullName: fmt.Sprintf("str-full-%04d", i),
		}
		keys = append(keys, key)
		vals = append(vals, rec)
		model[key] = ownershipE2ECopyRec(rec)
	}
	if err := db.BatchSet(keys, vals); err != nil {
		t.Fatalf("BatchSet(seed string): %v", err)
	}
	for i := range vals {
		vals[i].Name = "poison-string-name"
		vals[i].FullName = "poison-string-full"
		for j := range vals[i].Tags {
			vals[i].Tags[j] = "poison-string-tag"
		}
	}

	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("name", "str-name-0064")), func(rec Rec) bool {
		return rec.Name == "str-name-0064"
	})

	tags := []string{"str-owned", "str-shared"}
	if err := db.BatchPatch(keys, []Field{
		{Name: "name", Value: "string-cohort"},
		{Name: "tags", Value: tags},
	}); err != nil {
		t.Fatalf("BatchPatch(string cohort): %v", err)
	}
	tags[0] = "poison-string-owned"
	tags[1] = "poison-string-shared"
	for key, rec := range model {
		rec.Name = "string-cohort"
		rec.Tags = []string{"str-owned", "str-shared"}
		model[key] = rec
	}

	emptyKeys := []string{"key-0004", "key-0064"}
	if err := db.BatchPatch(emptyKeys, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(string empty tags): %v", err)
	}
	for _, key := range emptyKeys {
		rec := model[key]
		rec.Tags = []string{}
		model[key] = rec
	}

	deleteKeys := []string{"key-0001", "key-0004", "key-0128", "key-0128"}
	if err := db.BatchDelete(deleteKeys); err != nil {
		t.Fatalf("BatchDelete(string neighbors): %v", err)
	}
	for _, key := range deleteKeys {
		delete(model, key)
	}

	ownershipE2EAssertStringScan(t, db, model)
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("name", "str-name-0064")), func(rec Rec) bool {
		return rec.Name == "str-name-0064"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"str-tag-0064"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "str-tag-0064")
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("full_name", "str-full-0004")), func(rec Rec) bool {
		return rec.FullName == "str-full-0004"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("full_name", "str-full-0064")), func(rec Rec) bool {
		return rec.FullName == "str-full-0064"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("tags", []string{})), func(rec Rec) bool {
		return len(rec.Tags) == 0
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"str-owned"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "str-owned")
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("name", "string-cohort")), func(rec Rec) bool {
		return rec.Name == "string-cohort"
	})
}

func TestOwnershipE2E_ReopenModelAfterPatchDeleteTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ownership_reopen.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
		if raw != nil {
			_ = raw.Close()
		}
	})

	const rows = 420
	ids := make([]uint64, 0, rows)
	vals := make([]*Rec, 0, rows)
	model := make(map[uint64]Rec, rows)
	for i := 1; i <= rows; i++ {
		id := uint64(i)
		rec := &Rec{
			Meta:     Meta{Country: fmt.Sprintf("persist-country-%02d", i%13)},
			Name:     fmt.Sprintf("persist-name-%04d", i),
			Age:      25 + i%60,
			Score:    float64(i % 90),
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("persist-tag-%04d", i), fmt.Sprintf("persist-lane-%02d", i%9)},
			FullName: fmt.Sprintf("persist-full-%04d", i),
		}
		ids = append(ids, id)
		vals = append(vals, rec)
		model[id] = ownershipE2ECopyRec(rec)
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}
	for i := range vals {
		vals[i].Name = "poison-persist-name"
		vals[i].FullName = "poison-persist-full"
		for j := range vals[i].Tags {
			vals[i].Tags[j] = "poison-persist-tag"
		}
	}

	tags := []string{"persist-owned", "persist-shared"}
	if err := db.BatchPatch(ids, []Field{
		{Name: "name", Value: "persist-cohort"},
		{Name: "tags", Value: tags},
	}); err != nil {
		t.Fatalf("BatchPatch(cohort): %v", err)
	}
	tags[0] = "poison-persist-owned"
	tags[1] = "poison-persist-shared"
	for id, rec := range model {
		rec.Name = "persist-cohort"
		rec.Tags = []string{"persist-owned", "persist-shared"}
		model[id] = rec
	}

	emptyIDs := []uint64{5, 200, 417}
	if err := db.BatchPatch(emptyIDs, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(empty tags): %v", err)
	}
	for _, id := range emptyIDs {
		rec := model[id]
		rec.Tags = []string{}
		model[id] = rec
	}

	singleTags := []string{"persist-owned"}
	singleIDs := []uint64{6, 201, 418}
	if err := db.BatchPatch(singleIDs, []Field{{Name: "tags", Value: singleTags}}); err != nil {
		t.Fatalf("BatchPatch(single tags): %v", err)
	}
	singleTags[0] = "poison-single"
	for _, id := range singleIDs {
		rec := model[id]
		rec.Tags = []string{"persist-owned"}
		model[id] = rec
	}

	deleteIDs := []uint64{1, 2, 2, 3, 419, 420}
	if err := db.BatchDelete(deleteIDs); err != nil {
		t.Fatalf("BatchDelete(neighbors): %v", err)
	}
	for _, id := range deleteIDs {
		delete(model, id)
	}

	ownershipE2EAssertReopenModel(t, db, model)

	sidecar := db.rbiFile
	if err := db.Close(); err != nil {
		t.Fatalf("Close before persisted reopen: %v", err)
	}
	db = nil
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close before persisted reopen: %v", err)
	}
	raw = nil

	db, raw = openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	ownershipE2EAssertReopenModel(t, db, model)
	if err := db.Close(); err != nil {
		t.Fatalf("Close before rebuild reopen: %v", err)
	}
	db = nil
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close before rebuild reopen: %v", err)
	}
	raw = nil

	if err := os.Remove(sidecar); err != nil {
		t.Fatalf("remove persisted index sidecar: %v", err)
	}
	db, raw = openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	ownershipE2EAssertReopenModel(t, db, model)
}

func TestOwnershipE2E_StringKeyReopenModelAfterPatchDeleteTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ownership_string_reopen.db")
	db, raw := openBoltAndNew[string, Rec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
		if raw != nil {
			_ = raw.Close()
		}
	})

	const rows = 160
	keys := make([]string, 0, rows)
	vals := make([]*Rec, 0, rows)
	model := make(map[string]Rec, rows)
	for i := 1; i <= rows; i++ {
		key := fmt.Sprintf("persist-key-%04d", i)
		rec := &Rec{
			Meta:     Meta{Country: fmt.Sprintf("persist-str-country-%02d", i%7)},
			Name:     fmt.Sprintf("persist-str-name-%04d", i),
			Age:      35 + i%30,
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("persist-str-tag-%04d", i), fmt.Sprintf("persist-str-lane-%02d", i%5)},
			FullName: fmt.Sprintf("persist-str-full-%04d", i),
		}
		keys = append(keys, key)
		vals = append(vals, rec)
		model[key] = ownershipE2ECopyRec(rec)
	}
	if err := db.BatchSet(keys, vals); err != nil {
		t.Fatalf("BatchSet(seed string reopen): %v", err)
	}
	for i := range vals {
		vals[i].Name = "poison-persist-string-name"
		vals[i].FullName = "poison-persist-string-full"
		for j := range vals[i].Tags {
			vals[i].Tags[j] = "poison-persist-string-tag"
		}
	}

	tags := []string{"persist-str-owned", "persist-str-shared"}
	if err := db.BatchPatch(keys, []Field{
		{Name: "name", Value: "persist-string-cohort"},
		{Name: "tags", Value: tags},
	}); err != nil {
		t.Fatalf("BatchPatch(string cohort): %v", err)
	}
	tags[0] = "poison-persist-string-owned"
	tags[1] = "poison-persist-string-shared"
	for key, rec := range model {
		rec.Name = "persist-string-cohort"
		rec.Tags = []string{"persist-str-owned", "persist-str-shared"}
		model[key] = rec
	}

	emptyKeys := []string{"persist-key-0004", "persist-key-0064"}
	if err := db.BatchPatch(emptyKeys, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(string empty tags): %v", err)
	}
	for _, key := range emptyKeys {
		rec := model[key]
		rec.Tags = []string{}
		model[key] = rec
	}

	singleTags := []string{"persist-str-owned"}
	singleKeys := []string{"persist-key-0005", "persist-key-0065"}
	if err := db.BatchPatch(singleKeys, []Field{{Name: "tags", Value: singleTags}}); err != nil {
		t.Fatalf("BatchPatch(string single tags): %v", err)
	}
	singleTags[0] = "poison-persist-string-single"
	for _, key := range singleKeys {
		rec := model[key]
		rec.Tags = []string{"persist-str-owned"}
		model[key] = rec
	}

	deleteKeys := []string{"persist-key-0001", "persist-key-0004", "persist-key-0160", "persist-key-0160"}
	if err := db.BatchDelete(deleteKeys); err != nil {
		t.Fatalf("BatchDelete(string neighbors): %v", err)
	}
	for _, key := range deleteKeys {
		delete(model, key)
	}

	ownershipE2EAssertStringReopenModel(t, db, model)

	sidecar := db.rbiFile
	if err := db.Close(); err != nil {
		t.Fatalf("Close before string persisted reopen: %v", err)
	}
	db = nil
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close before string persisted reopen: %v", err)
	}
	raw = nil

	db, raw = openBoltAndNew[string, Rec](t, path, Options{AnalyzeInterval: -1})
	ownershipE2EAssertStringReopenModel(t, db, model)
	if err := db.Close(); err != nil {
		t.Fatalf("Close before string rebuild reopen: %v", err)
	}
	db = nil
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close before string rebuild reopen: %v", err)
	}
	raw = nil

	if err := os.Remove(sidecar); err != nil {
		t.Fatalf("remove string persisted index sidecar: %v", err)
	}
	db, raw = openBoltAndNew[string, Rec](t, path, Options{AnalyzeInterval: -1})
	ownershipE2EAssertStringReopenModel(t, db, model)
}

func ownershipE2ECopyRec(src *Rec) Rec {
	out := *src
	out.Tags = slices.Clone(src.Tags)
	return out
}

func ownershipE2EAssertQuery(t *testing.T, db *DB[uint64, Rec], model map[uint64]Rec, q *qx.QX, match func(Rec) bool) {
	t.Helper()

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	slices.Sort(got)

	want := make([]uint64, 0)
	for id, rec := range model {
		if match(rec) {
			want = append(want, id)
		}
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("QueryKeys(%+v) got=%v want=%v", q, got, want)
	}

	count, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q.Filter, err)
	}
	if count != uint64(len(want)) {
		t.Fatalf("Count(%+v)=%d want=%d", q.Filter, count, len(want))
	}
}

func ownershipE2EAssertStringQuery(t *testing.T, db *DB[string, Rec], model map[string]Rec, q *qx.QX, match func(Rec) bool) {
	t.Helper()

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	slices.Sort(got)

	want := make([]string, 0)
	for key, rec := range model {
		if match(rec) {
			want = append(want, key)
		}
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("QueryKeys(%+v) got=%v want=%v", q, got, want)
	}

	count, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q.Filter, err)
	}
	if count != uint64(len(want)) {
		t.Fatalf("Count(%+v)=%d want=%d", q.Filter, count, len(want))
	}
}

func ownershipE2EAssertStringScan(t *testing.T, db *DB[string, Rec], model map[string]Rec) {
	t.Helper()

	got := make([]string, 0, len(model))
	if err := db.SeqScan("", func(key string, _ *Rec) (bool, error) {
		got = append(got, key)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	slices.Sort(got)

	want := make([]string, 0, len(model))
	for key := range model {
		want = append(want, key)
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("SeqScan got=%v want=%v", got, want)
	}
}

func ownershipE2EAssertReopenModel(t *testing.T, db *DB[uint64, Rec], model map[uint64]Rec) {
	t.Helper()

	if got, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != uint64(len(model)) {
		t.Fatalf("Count=%d want=%d", got, len(model))
	}
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("name", "persist-cohort")), func(rec Rec) bool {
		return rec.Name == "persist-cohort"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("name", "persist-name-0200")), func(rec Rec) bool {
		return rec.Name == "persist-name-0200"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("full_name", "persist-full-0002")), func(rec Rec) bool {
		return rec.FullName == "persist-full-0002"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("full_name", "persist-full-0200")), func(rec Rec) bool {
		return rec.FullName == "persist-full-0200"
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"persist-tag-0200"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "persist-tag-0200")
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"persist-owned"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "persist-owned")
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{})), func(rec Rec) bool {
		return len(rec.Tags) == 0
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"persist-owned"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"persist-owned"})
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"persist-owned", "persist-shared"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"persist-owned", "persist-shared"})
	})
	ownershipE2EAssertQuery(t, db, model, qx.Query(qx.EQ("country", "persist-country-05")), func(rec Rec) bool {
		return rec.Country == "persist-country-05"
	})
}

func ownershipE2EAssertStringReopenModel(t *testing.T, db *DB[string, Rec], model map[string]Rec) {
	t.Helper()

	if got, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != uint64(len(model)) {
		t.Fatalf("Count=%d want=%d", got, len(model))
	}
	ownershipE2EAssertStringScan(t, db, model)
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("name", "persist-string-cohort")), func(rec Rec) bool {
		return rec.Name == "persist-string-cohort"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("name", "persist-str-name-0064")), func(rec Rec) bool {
		return rec.Name == "persist-str-name-0064"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("full_name", "persist-str-full-0004")), func(rec Rec) bool {
		return rec.FullName == "persist-str-full-0004"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("full_name", "persist-str-full-0064")), func(rec Rec) bool {
		return rec.FullName == "persist-str-full-0064"
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"persist-str-tag-0064"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "persist-str-tag-0064")
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.HASANY("tags", []string{"persist-str-owned"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "persist-str-owned")
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("tags", []string{})), func(rec Rec) bool {
		return len(rec.Tags) == 0
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"persist-str-owned"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"persist-str-owned"})
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("tags", []string{"persist-str-owned", "persist-str-shared"})), func(rec Rec) bool {
		return ownershipE2ESameStringSet(rec.Tags, []string{"persist-str-owned", "persist-str-shared"})
	})
	ownershipE2EAssertStringQuery(t, db, model, qx.Query(qx.EQ("country", "persist-str-country-05")), func(rec Rec) bool {
		return rec.Country == "persist-str-country-05"
	})
}

func ownershipE2ESameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !slices.Contains(b, a[i]) {
			return false
		}
	}
	return true
}
