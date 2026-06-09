package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vapstack/rbi/rbierrors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

func TestTransparentMode_DisablesIndexedAPIsAndUsesDirectBoltSeqScans(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transparent_scan.db")

	db, raw := openBoltAndNew[string, noIndexRec](t, path)
	defer func() {
		_ = db.Close()
		_ = raw.Close()
	}()

	for _, key := range []string{"k-02", "k-10", "k-01"} {
		if err := db.Set(key, &noIndexRec{Name: key, Age: len(key)}); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	if _, err := db.Query(qx.Query()); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("Query(all) err=%v want %v", err, rbierrors.ErrNoIndex)
	}
	if _, err := db.QueryKeys(qx.Query()); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("QueryKeys(all) err=%v want %v", err, rbierrors.ErrNoIndex)
	}
	if _, err := db.Count(); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("Count() err=%v want %v", err, rbierrors.ErrNoIndex)
	}
	if err := db.RefreshPlannerStats(); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("RefreshPlannerStats err=%v want %v", err, rbierrors.ErrNoIndex)
	}
	if got := db.PlannerStats(); got.Version != 0 || got.FieldCount != 0 || got.AnalyzeInterval != 0 || got.TraceSampleEvery != 0 || len(got.Fields) != 0 {
		t.Fatalf("PlannerStats in transparent mode=%+v want zero planner payload", got)
	}

	var seq []string
	if err := db.SeqScan("k-02", func(id string, v *noIndexRec) (bool, error) {
		seq = append(seq, fmt.Sprintf("%s:%s", id, v.Name))
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if !slices.Equal(seq, []string{"k-02:k-02", "k-10:k-10"}) {
		t.Fatalf("SeqScan=%v", seq)
	}

	var rawSeq []string
	if err := scanRawBolt(t, db, "k-02", func(id string, raw []byte) (bool, error) {
		rawSeq = append(rawSeq, id)
		if len(raw) == 0 {
			t.Fatal("raw bbolt scan returned empty payload")
		}
		return true, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan: %v", err)
	}
	if !slices.Equal(rawSeq, []string{"k-02", "k-10"}) {
		t.Fatalf("raw bbolt scan=%v", rawSeq)
	}

	if err := raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		old := bucket.Get([]byte("k-02"))
		oldIdx := keycodec.U64FromBytes(old[:8])
		if err := bucket.Delete([]byte("k-02")); err != nil {
			return err
		}
		m := tx.Bucket(db.strmapBucket)
		var mapKey [8]byte
		if err := m.Delete(keycodec.U64BytesWithBuf(oldIdx, &mapKey)); err != nil {
			return err
		}
		buf := new(bytes.Buffer)
		if err := db.encode(&noIndexRec{Name: "k-03", Age: 4}, buf); err != nil {
			return err
		}
		idx, err := m.NextSequence()
		if err != nil {
			return err
		}
		if err = m.Put(keycodec.U64BytesWithBuf(idx, &mapKey), []byte("k-03")); err != nil {
			return err
		}
		payload := keycodec.AppendU64Bytes(nil, idx)
		payload = append(payload, buf.Bytes()...)
		return bucket.Put([]byte("k-03"), payload)
	}); err != nil {
		t.Fatalf("out-of-band mutate: %v", err)
	}

	seq = seq[:0]
	if err := db.SeqScan("k-02", func(id string, v *noIndexRec) (bool, error) {
		seq = append(seq, fmt.Sprintf("%s:%s", id, v.Name))
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(after mutate): %v", err)
	}
	if !slices.Equal(seq, []string{"k-03:k-03", "k-10:k-10"}) {
		t.Fatalf("SeqScan(after mutate)=%v want [k-03:k-03 k-10:k-10]", seq)
	}

	rawSeq = rawSeq[:0]
	if err := scanRawBolt(t, db, "k-02", func(id string, raw []byte) (bool, error) {
		rawSeq = append(rawSeq, id)
		if len(raw) == 0 {
			t.Fatal("raw bbolt scan after mutate returned empty payload")
		}
		return true, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan after mutate: %v", err)
	}
	if !slices.Equal(rawSeq, []string{"k-03", "k-10"}) {
		t.Fatalf("raw bbolt scan after mutate=%v want [k-03 k-10]", rawSeq)
	}

	var scanSeq []string
	if err := db.ScanKeys("", func(id string) (bool, error) {
		scanSeq = append(scanSeq, id)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(scanSeq, []string{"k-01", "k-03", "k-10"}) {
		t.Fatalf("ScanKeys=%v want [k-01 k-03 k-10]", scanSeq)
	}
}

func TestTransparentMode_StringOverwriteSkipsOldPayloadDecode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transparent_string_overwrite.db")

	db, raw := openBoltAndNew[string, noIndexRec](t, path)
	defer func() {
		_ = db.Close()
		_ = raw.Close()
	}()

	if err := db.Set("k", &noIndexRec{Name: "old", Age: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var oldIdx uint64
	if err := raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		stored := bucket.Get([]byte("k"))
		oldIdx = keycodec.U64FromBytes(stored[:8])
		bad := keycodec.AppendU64Bytes(nil, oldIdx)
		bad = append(bad, 0xc1)
		return bucket.Put([]byte("k"), bad)
	}); err != nil {
		t.Fatalf("corrupt old payload: %v", err)
	}

	if err := db.Set("k", &noIndexRec{Name: "fresh", Age: 2}); err != nil {
		t.Fatalf("overwrite corrupt old payload: %v", err)
	}

	got, err := db.Get("k")
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if got == nil || got.Name != "fresh" || got.Age != 2 {
		t.Fatalf("Get(k)=%#v want fresh", got)
	}

	if err := raw.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		idx := keycodec.U64FromBytes(bucket.Get([]byte("k"))[:8])
		if idx != oldIdx {
			return fmt.Errorf("idx changed: got=%d want=%d", idx, oldIdx)
		}
		m := tx.Bucket(db.strmapBucket)
		if m == nil {
			return fmt.Errorf("string map bucket does not exist")
		}
		var mapKey [8]byte
		key := m.Get(keycodec.U64BytesWithBuf(idx, &mapKey))
		if !slices.Equal(key, []byte("k")) {
			return fmt.Errorf("map[%d]=%q want k", idx, key)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify durable id: %v", err)
	}
}

func TestIndexTags_OptInSupportRBI(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opt_in_tags.db")
	db, raw := openBoltAndNew[uint64, optInTaggedRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &optInTaggedRec{
		Index:    "index-1",
		Unique:   100,
		Disabled: "disabled-1",
		Untagged: "u1",
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &optInTaggedRec{
		Index:    "index-2",
		Unique:   200,
		Disabled: "disabled-2",
		Untagged: "u2",
	}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	tests := []struct {
		field string
		value any
		want  []uint64
	}{
		{field: "index", value: "index-2", want: []uint64{2}},
		{field: "unique", value: 200, want: []uint64{2}},
	}
	for _, tc := range tests {
		ids, err := db.QueryKeys(qx.Query(qx.EQ(tc.field, tc.value)))
		if err != nil {
			t.Fatalf("QueryKeys(%s): %v", tc.field, err)
		}
		if !slices.Equal(ids, tc.want) {
			t.Fatalf("QueryKeys(%s)=%v want %v", tc.field, ids, tc.want)
		}
	}
	for _, field := range []string{"untagged", "disabled"} {
		if _, err := db.QueryKeys(qx.Query(qx.EQ(field, "ignored"))); err == nil {
			t.Fatalf("QueryKeys(%s) must fail for non-indexed field", field)
		}
	}

	if err := db.Set(3, &optInTaggedRec{Unique: 200}); !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("duplicate unique rbi tag err=%v want %v", err, rbierrors.ErrUniqueViolation)
	}
}

func TestIndexTags_OptInLeavesUntaggedStructTransparent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opt_in_untagged.db")
	db, raw := openBoltAndNew[uint64, optInNoTagRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if _, err := db.QueryKeys(qx.Query()); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("QueryKeys err=%v want %v", err, rbierrors.ErrNoIndex)
	}
}

func TestIndexTags_InvalidActiveTagValueFailsFast(t *testing.T) {
	dir := t.TempDir()

	rawRBI, err := bbolt.Open(filepath.Join(dir, "invalid_rbi.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open raw invalid_rbi: %v", err)
	}
	defer func() { _ = rawRBI.Close() }()

	_, err = New[uint64, invalidRBITagRec](rawRBI, testOptions(Options{}))
	if err == nil || !strings.Contains(err.Error(), `invalid index tag value "autp"`) {
		t.Fatalf("invalid rbi tag err=%v", err)
	}

	rawRBIAuto, err := bbolt.Open(filepath.Join(dir, "removed_rbi_auto.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open raw removed_rbi_auto: %v", err)
	}
	defer func() { _ = rawRBIAuto.Close() }()

	_, err = New[uint64, removedRBIAutoTagRec](rawRBIAuto, testOptions(Options{}))
	if err == nil || !strings.Contains(err.Error(), `invalid index tag value "auto"`) {
		t.Fatalf("removed rbi auto tag err=%v", err)
	}
}

func TestIndexTags_EmbeddedParentIndexDoesNotEnableSharedFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "embedded_parent_index.db")
	db, raw := openBoltAndNew[uint64, embeddedEnabledByParentRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &embeddedEnabledByParentRec{
		EmbeddedSharedFields: EmbeddedSharedFields{Name: "alice", Email: "alice@example.com"},
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &embeddedEnabledByParentRec{
		EmbeddedSharedFields: EmbeddedSharedFields{Name: "bob", Email: "alice@example.com"},
	}); !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("duplicate embedded unique err=%v want %v", err, rbierrors.ErrUniqueViolation)
	}
	if _, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice"))); err == nil {
		t.Fatal("embedded parent index tag must not enable untagged child field")
	}
}

func TestIndexTags_EmbeddedParentDisableSuppressesSharedFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "embedded_parent_disable.db")
	db, raw := openBoltAndNew[uint64, embeddedDisabledByParentRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &embeddedDisabledByParentRec{
		EmbeddedSharedFields: EmbeddedSharedFields{Name: "alice", Email: "dup@example.com"},
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &embeddedDisabledByParentRec{
		EmbeddedSharedFields: EmbeddedSharedFields{Name: "bob", Email: "dup@example.com"},
	}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if _, err := db.QueryKeys(qx.Query()); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("QueryKeys err=%v want %v", err, rbierrors.ErrNoIndex)
	}
}

func TestIndexTags_DBTagDashDoesNotDisableIndexing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db_dash_does_not_disable_index.db")
	db, raw := openBoltAndNew[uint64, dbDashDoesNotDisableIndexRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &dbDashDoesNotDisableIndexRec{Name: "alice"}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	ids, err := db.QueryKeys(qx.Query(qx.EQ("Name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(Name): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(Name)=%v want [1]", ids)
	}
}

func TestIndexOptions_OverrideTagsAndResolveGoAndDBNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "options_index.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	db, err := New[uint64, optionsIndexRec](raw, testOptions(Options{
		Index: map[string]IndexKind{
			"Name":     IndexDefault,
			"score_db": IndexUnique,
			"Amount":   IndexMeasure,
		},
	}))
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &optionsIndexRec{Name: "alice", Email: "ignored", Score: 10, Amount: 100}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &optionsIndexRec{Name: "bob", Email: "ignored", Score: 10, Amount: 200}); !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("duplicate unique from Options.Index err=%v want %v", err, rbierrors.ErrUniqueViolation)
	}
	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(name): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(name)=%v want [1]", ids)
	}
	if _, err := db.QueryKeys(qx.Query(qx.EQ("email", "ignored"))); err == nil {
		t.Fatal("non-nil Options.Index must ignore rbi tag on email")
	}
	result, err := db.Aggregate(qx.Aggregate(qx.SUM("amount").AS("amount_sum")))
	if err != nil {
		t.Fatalf("Aggregate amount: %v", err)
	}
	requireAggregateInt(t, result.Rows[0][0], 100)
}

func TestIndexOptions_DBTagsResolveWhenEmbeddedGoNamesCollide(t *testing.T) {
	dir := t.TempDir()

	rawDBTags, err := bbolt.Open(filepath.Join(dir, "options_embedded_db_tags.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open db-tags: %v", err)
	}
	db, err := New[uint64, optionsIndexEmbeddedCollisionRec](rawDBTags, testOptions(Options{
		Index: map[string]IndexKind{
			"left_id":  IndexDefault,
			"right_id": IndexDefault,
		},
	}))
	if err != nil {
		_ = rawDBTags.Close()
		t.Fatalf("New with db-tag keys: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = rawDBTags.Close()
	})
	if err := db.Set(1, &optionsIndexEmbeddedCollisionRec{
		OptionsIndexLeftEmbeddedRec:  OptionsIndexLeftEmbeddedRec{ID: 10},
		OptionsIndexRightEmbeddedRec: OptionsIndexRightEmbeddedRec{ID: 20},
	}); err != nil {
		t.Fatalf("Set db-tag collision record: %v", err)
	}
	ids, err := db.QueryKeys(qx.Query(qx.EQ("left_id", 10)))
	if err != nil {
		t.Fatalf("QueryKeys(left_id): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(left_id)=%v want [1]", ids)
	}
	ids, err = db.QueryKeys(qx.Query(qx.EQ("right_id", 20)))
	if err != nil {
		t.Fatalf("QueryKeys(right_id): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(right_id)=%v want [1]", ids)
	}

	rawGoName, err := bbolt.Open(filepath.Join(dir, "options_embedded_go_name.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open go-name: %v", err)
	}
	defer func() { _ = rawGoName.Close() }()
	_, err = New[uint64, optionsIndexEmbeddedCollisionRec](rawGoName, testOptions(Options{
		Index: map[string]IndexKind{"ID": IndexDefault},
	}))
	if err == nil || !strings.Contains(err.Error(), `ambiguous Go field name "ID"`) {
		t.Fatalf("ambiguous Go-name Options.Index err=%v", err)
	}
}

func TestIndexOptions_EmptyMapDisablesTags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "options_empty.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	db, err := New[uint64, optionsIndexRec](raw, testOptions(Options{Index: map[string]IndexKind{}}))
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if _, err := db.QueryKeys(qx.Query()); !errors.Is(err, rbierrors.ErrNoIndex) {
		t.Fatalf("QueryKeys err=%v want %v", err, rbierrors.ErrNoIndex)
	}
}

func TestIndexOptions_InvalidReferencesFailFast(t *testing.T) {
	dir := t.TempDir()

	rawUnknown, err := bbolt.Open(filepath.Join(dir, "options_unknown.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open unknown: %v", err)
	}
	defer func() { _ = rawUnknown.Close() }()
	_, err = New[uint64, optionsIndexRec](rawUnknown, testOptions(Options{
		Index: map[string]IndexKind{"Missing": IndexDefault},
	}))
	if err == nil || !strings.Contains(err.Error(), `unknown index field "Missing"`) {
		t.Fatalf("unknown Options.Index field err=%v", err)
	}

	rawDuplicate, err := bbolt.Open(filepath.Join(dir, "options_duplicate.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open duplicate: %v", err)
	}
	defer func() { _ = rawDuplicate.Close() }()
	_, err = New[uint64, optionsIndexRec](rawDuplicate, testOptions(Options{
		Index: map[string]IndexKind{
			"Score":    IndexDefault,
			"score_db": IndexDefault,
		},
	}))
	if err == nil || !strings.Contains(err.Error(), `indexed more than once`) {
		t.Fatalf("duplicate Options.Index field err=%v", err)
	}

	rawInvalidKind, err := bbolt.Open(filepath.Join(dir, "options_invalid_kind.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open invalid kind: %v", err)
	}
	defer func() { _ = rawInvalidKind.Close() }()
	_, err = New[uint64, optionsIndexRec](rawInvalidKind, testOptions(Options{
		Index: map[string]IndexKind{"Name": IndexKind(99)},
	}))
	if err == nil || !strings.Contains(err.Error(), `invalid IndexKind 99`) {
		t.Fatalf("invalid Options.Index kind err=%v", err)
	}
}

func TestIndexTags_MultiValueTagFailsFast(t *testing.T) {
	dir := t.TempDir()
	raw, err := bbolt.Open(filepath.Join(dir, "multi_value_rbi.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open raw multi_value_rbi: %v", err)
	}
	defer func() { _ = raw.Close() }()

	_, err = New[uint64, multiValueRBITagRec](raw, testOptions(Options{}))
	if err == nil || !strings.Contains(err.Error(), `invalid index tag value "index,unique"`) {
		t.Fatalf("multi-value rbi tag err=%v", err)
	}
}

func TestIndexTags_MeasureMetadataIsSeparateFromOrdinaryIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_tagged.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	db, err := New[uint64, measureTaggedRec](raw, testOptions(Options{}))
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &measureTaggedRec{Status: "ok", Amount: 42}); err != nil {
		t.Fatalf("Set measure record: %v", err)
	}
	requireMeasureTaggedSum(t, db, 42)
	if err := db.Set(2, &measureTaggedRec{Status: "ok", Amount: 100}); err != nil {
		t.Fatalf("Set second measure record: %v", err)
	}
	requireMeasureTaggedSum(t, db, 142)
	statusQ := qx.Query(qx.EQ("status", "ok"))
	ids, err := db.QueryKeys(statusQ)
	if err != nil {
		t.Fatalf("QueryKeys(status): %v", err)
	}
	if !queryIDsEqual(statusQ, ids, []uint64{1, 2}) {
		t.Fatalf("QueryKeys(status)=%v want [1 2]", ids)
	}
	if err := db.Set(1, &measureTaggedRec{Status: "ok", Amount: 43}); err != nil {
		t.Fatalf("Update measure record: %v", err)
	}
	requireMeasureTaggedSum(t, db, 143)
	if err := db.Patch(2, []Field{{Name: "amount", Value: int64(55)}}); err != nil {
		t.Fatalf("Patch measure record: %v", err)
	}
	requireMeasureTaggedSum(t, db, 98)
	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete measure record: %v", err)
	}
	requireMeasureTaggedSum(t, db, 55)
	requireMeasureTaggedSum(t, db, 55)
	if _, err := db.QueryKeys(qx.Query(qx.EQ("amount", int64(100)))); err == nil {
		t.Fatal("measure field must not be queryable through ordinary planner")
	}
}

func TestIndexTags_MeasureOnlyDBKeepsSnapshotMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_only.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	db, err := New[uint64, measureOnlyRec](raw, testOptions(Options{}))
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &measureOnlyRec{Amount: 7}); err != nil {
		t.Fatalf("Set measure-only record: %v", err)
	}
	requireMeasureOnlySum(t, db, 7)
	if err := db.Set(1, &measureOnlyRec{Amount: 8}); err != nil {
		t.Fatalf("Update measure-only record: %v", err)
	}
	requireMeasureOnlySum(t, db, 8)
	if _, err := db.QueryKeys(qx.Query(qx.EQ("amount", int64(8)))); err == nil {
		t.Fatal("measure-only field must not be queryable through ordinary planner")
	}
}

func TestPlannerAnalyzeScheduler_MeasureOnlyRefreshesUniverse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_only_analyze.db")
	db, raw := openBoltAndNew[uint64, measureOnlyRec](t, path, Options{AnalyzeInterval: 5 * time.Millisecond})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	start := db.PlannerStats()
	if start.UniverseCardinality != 0 {
		t.Fatalf("initial planner universe=%d want 0", start.UniverseCardinality)
	}
	if err := db.Set(1, &measureOnlyRec{Amount: 7}); err != nil {
		t.Fatalf("Set measure-only record: %v", err)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		stats := db.PlannerStats()
		if stats.Version > start.Version && stats.UniverseCardinality == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}

	stats := db.PlannerStats()
	t.Fatalf("measure-only analyzer did not refresh universe: start_version=%d version=%d universe=%d", start.Version, stats.Version, stats.UniverseCardinality)
}

func TestIndexTags_MeasureRejectsUnsupportedType(t *testing.T) {
	dir := t.TempDir()
	raw, err := bbolt.Open(filepath.Join(dir, "measure_string.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = raw.Close() }()

	_, err = New[uint64, invalidMeasureStringRec](raw, testOptions(Options{}))
	if err == nil || !strings.Contains(err.Error(), `measure field Name has unsupported type`) {
		t.Fatalf("invalid measure field err=%v", err)
	}
}

func TestReflectExt_NewRejectsNamedNativeTimePointerType(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	_, err := New[uint64, reflectNamedTimePtrRec](raw, testOptions(Options{}))
	if err == nil {
		t.Fatalf("expected New to reject named *time.Time indexed field")
	}
	if !strings.Contains(err.Error(), "cannot index field When") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type noIndexRec struct {
	Name string `rbi:"-"`
	Age  int    `rbi:"-"`
}

type optInTaggedRec struct {
	Index    string `db:"index"    rbi:"index"`
	Unique   int    `db:"unique"   rbi:"unique"`
	Disabled string `db:"disabled" rbi:"-"`
	Untagged string `db:"untagged"`
}

type optInNoTagRec struct {
	Name string `db:"name"`
	Age  int    `db:"age"`
}

type invalidRBITagRec struct {
	Name string `db:"name" rbi:"autp"`
}

type removedRBIAutoTagRec struct {
	Name string `db:"name" rbi:"auto"`
}

type multiValueRBITagRec struct {
	Name string `db:"name" rbi:"index,unique"`
}

type dbDashDoesNotDisableIndexRec struct {
	Name string `db:"-" rbi:"index"`
}

type EmbeddedSharedFields struct {
	Name  string `db:"name"`
	Email string `db:"email" rbi:"unique"`
}

type embeddedEnabledByParentRec struct {
	EmbeddedSharedFields `rbi:"index"`
}

type embeddedDisabledByParentRec struct {
	EmbeddedSharedFields `rbi:"-"`
}

type optionsIndexRec struct {
	Name   string `db:"name" rbi:"-"`
	Email  string `db:"email" rbi:"index"`
	Score  int    `db:"score_db"`
	Amount int64  `db:"amount" rbi:"measure"`
}

type OptionsIndexLeftEmbeddedRec struct {
	ID int `db:"left_id"`
}

type OptionsIndexRightEmbeddedRec struct {
	ID int `db:"right_id"`
}

type optionsIndexEmbeddedCollisionRec struct {
	OptionsIndexLeftEmbeddedRec
	OptionsIndexRightEmbeddedRec
}

type measureTaggedRec struct {
	Status string `db:"status" rbi:"index"`
	Amount int64  `db:"amount" rbi:"measure"`
}

type measureOnlyRec struct {
	Amount int64 `db:"amount" rbi:"measure"`
}

type invalidMeasureStringRec struct {
	Name string `db:"name" rbi:"measure"`
}

func requireMeasureTaggedSum(t *testing.T, db *DB[uint64, measureTaggedRec], want int64) {
	t.Helper()
	result, err := db.Aggregate(qx.Aggregate(qx.SUM("amount").AS("amount_sum")))
	if err != nil {
		t.Fatalf("Aggregate amount: %v", err)
	}
	requireAggregateInt(t, result.Rows[0][0], want)
}

func requireMeasureOnlySum(t *testing.T, db *DB[uint64, measureOnlyRec], want int64) {
	t.Helper()
	result, err := db.Aggregate(qx.Aggregate(qx.SUM("amount").AS("amount_sum")))
	if err != nil {
		t.Fatalf("Aggregate amount: %v", err)
	}
	requireAggregateInt(t, result.Rows[0][0], want)
}
