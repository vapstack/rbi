package persist

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
)

func TestReadSidecarStringRoundTrip(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	const want = "plain-string"
	if err := writeSidecarString(writer, want); err != nil {
		t.Fatalf("writeSidecarString: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := readSidecarString(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("readSidecarString: %v", err)
	}
	if got != want {
		t.Fatalf("readSidecarString mismatch: got %q", got)
	}
}

func TestReadFieldRejectsOutOfRangePersistedIndexKind(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writeSidecarString(writer, "field"); err != nil {
		t.Fatalf("write name: %v", err)
	}
	if err := writeSidecarBool(writer, false); err != nil {
		t.Fatalf("write unique: %v", err)
	}
	if err := writeSidecarUvarint(writer, 256); err != nil {
		t.Fatalf("write index kind: %v", err)
	}
	if err := writeSidecarUvarint(writer, uint64(reflect.Int)); err != nil {
		t.Fatalf("write kind: %v", err)
	}
	if err := writeSidecarBool(writer, false); err != nil {
		t.Fatalf("write ptr: %v", err)
	}
	if err := writeSidecarBool(writer, false); err != nil {
		t.Fatalf("write slice: %v", err)
	}
	if err := writeSidecarBool(writer, false); err != nil {
		t.Fatalf("write use vi: %v", err)
	}
	if err := writeSidecarString(writer, "field"); err != nil {
		t.Fatalf("write db name: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write index len: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, _, err := readField(bufio.NewReader(&buf))
	if err == nil || !strings.Contains(err.Error(), "invalid IndexKind 256") {
		t.Fatalf("readField err=%v, want invalid IndexKind 256", err)
	}
}

func TestReadFieldCompatibilityRejectsDuplicateNames(t *testing.T) {
	field := &schema.Field{
		IndexKind: schema.IndexDefault,
		Kind:      reflect.Int,
		DBName:    "field",
		Index:     []int{0},
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writeSidecarUvarint(writer, 2); err != nil {
		t.Fatalf("write count: %v", err)
	}
	if err := writeField(writer, "field", field); err != nil {
		t.Fatalf("write field 1: %v", err)
	}
	if err := writeField(writer, "field", field); err != nil {
		t.Fatalf("write field 2: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readFieldCompatibility(bufio.NewReader(&buf), map[string]*schema.Field{"field": field})
	if err == nil || !strings.Contains(err.Error(), `duplicate field "field"`) {
		t.Fatalf("readFieldCompatibility err=%v, want duplicate field", err)
	}
}

func TestReadSectionsRejectDuplicateNames(t *testing.T) {
	ids := (posting.List{}).BuildAdded(1)
	fieldStorage := indexdata.NewNilFieldStorageOwned(ids)
	defer fieldStorage.Release()

	measureStorage := persistTestMeasureStorage()
	defer measureStorage.Release()

	tests := []struct {
		name string
		err  string
		read func(*bufio.Reader) error
	}{
		{
			name: "regular",
			err:  `duplicate regular index field "field"`,
			read: func(reader *bufio.Reader) error {
				_, err := readIndexSections(reader, map[string]bool{"field": true}, "regular index")
				return err
			},
		},
		{
			name: "nil",
			err:  `duplicate nil index field "field"`,
			read: func(reader *bufio.Reader) error {
				_, err := readIndexSections(reader, map[string]bool{"field": true}, "nil index")
				return err
			},
		},
		{
			name: "len",
			err:  `duplicate len index field "field"`,
			read: func(reader *bufio.Reader) error {
				_, err := readIndexSections(reader, map[string]bool{"field": true}, "len index")
				return err
			},
		},
		{
			name: "measure",
			err:  `duplicate measure index field "measure"`,
			read: func(reader *bufio.Reader) error {
				_, err := readMeasureIndexSections(reader, map[string]bool{"measure": true})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			if err := writeSidecarUvarint(writer, 2); err != nil {
				t.Fatalf("write count: %v", err)
			}
			if tt.name == "measure" {
				if err := writeSidecarString(writer, "measure"); err != nil {
					t.Fatalf("write measure name 1: %v", err)
				}
				if err := measureStorage.WriteInto(writer); err != nil {
					t.Fatalf("write measure storage 1: %v", err)
				}
				if err := writeSidecarString(writer, "measure"); err != nil {
					t.Fatalf("write measure name 2: %v", err)
				}
				if err := measureStorage.WriteInto(writer); err != nil {
					t.Fatalf("write measure storage 2: %v", err)
				}
			} else {
				if err := writeSidecarString(writer, "field"); err != nil {
					t.Fatalf("write field name 1: %v", err)
				}
				if err := fieldStorage.WriteInto(writer); err != nil {
					t.Fatalf("write field storage 1: %v", err)
				}
				if err := writeSidecarString(writer, "field"); err != nil {
					t.Fatalf("write field name 2: %v", err)
				}
				if err := fieldStorage.WriteInto(writer); err != nil {
					t.Fatalf("write field storage 2: %v", err)
				}
			}
			if err := writer.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			err := tt.read(bufio.NewReader(&buf))
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("section err=%v, want %q", err, tt.err)
			}
		})
	}
}

func TestLoadPayloadSkipsIncompatibleFieldStorage(t *testing.T) {
	current := persistTestField("keep", 0)
	incompatibleCurrent := persistTestField("drop", 1)
	storedIncompatible := persistTestField("drop", 1)
	storedIncompatible.DBName = "drop_old"
	rt := &schema.Runtime{
		Fields: map[string]*schema.Field{
			"keep": current,
			"drop": incompatibleCurrent,
		},
		MeasureFields: map[string]*schema.Field{},
		Indexed: []schema.IndexedFieldAccessor{
			{Ordinal: 0, Name: "keep", Field: current},
			{Ordinal: 1, Name: "drop", Field: incompatibleCurrent},
		},
		IndexedByName: schema.IndexedFieldMap{
			"keep": {Ordinal: 0, Name: "keep", Field: current},
			"drop": {Ordinal: 1, Name: "drop", Field: incompatibleCurrent},
		},
	}

	keepStorage := indexdata.NewNilFieldStorageOwned((posting.List{}).BuildAdded(1))
	defer keepStorage.Release()
	dropStorage := indexdata.NewNilFieldStorageOwned((posting.List{}).BuildAdded(2))
	defer dropStorage.Release()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	writePersistTestUniverse(t, writer)
	if err := strmap.WriteSnapshot(writer, nil); err != nil {
		t.Fatalf("write strmap: %v", err)
	}
	if err := writeSidecarUvarint(writer, 2); err != nil {
		t.Fatalf("write field count: %v", err)
	}
	if err := writeField(writer, "drop", storedIncompatible); err != nil {
		t.Fatalf("write drop field: %v", err)
	}
	if err := writeField(writer, "keep", current); err != nil {
		t.Fatalf("write keep field: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write measure fields: %v", err)
	}
	if err := writeSidecarUvarint(writer, 2); err != nil {
		t.Fatalf("write regular count: %v", err)
	}
	if err := writeSidecarString(writer, "drop"); err != nil {
		t.Fatalf("write drop section name: %v", err)
	}
	if err := dropStorage.WriteInto(writer); err != nil {
		t.Fatalf("write drop storage: %v", err)
	}
	if err := writeSidecarString(writer, "keep"); err != nil {
		t.Fatalf("write keep section name: %v", err)
	}
	if err := keepStorage.WriteInto(writer); err != nil {
		t.Fatalf("write keep storage: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write nil section count: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write len section count: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write measure section count: %v", err)
	}
	if err := writePlannerStatsSnapshot(writer, &qexec.PlannerStatsSnapshot{
		Version: 1,
		Fields: map[string]qexec.PlannerFieldStats{
			"keep": {DistinctKeys: 1, NonEmptyKeys: 1, TotalBucketCard: 1},
		},
	}); err != nil {
		t.Fatalf("write planner stats: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	result, err := loadPayload(bufio.NewReader(&buf), rt, 0)
	if err != nil {
		t.Fatalf("loadPayload: %v", err)
	}
	defer result.Storage.Release()
	if _, ok := result.SkipFields["keep"]; !ok {
		t.Fatalf("compatible field was not marked loaded")
	}
	if _, ok := result.SkipFields["drop"]; ok {
		t.Fatalf("incompatible field was marked loaded")
	}
	if result.Storage.Index[0].KeyCount() == 0 {
		t.Fatalf("compatible storage was not loaded")
	}
	if result.Storage.Index[1].KeyCount() != 0 {
		t.Fatalf("incompatible storage was retained")
	}
}

func TestLoadCorruptedFieldStorageWrapsInvalidSentinel(t *testing.T) {
	errInvalid := errors.New("invalid sentinel")
	errStale := errors.New("stale sentinel")
	field := persistTestField("field", 0)
	rt := &schema.Runtime{
		Fields:        map[string]*schema.Field{"field": field},
		MeasureFields: map[string]*schema.Field{},
		Indexed:       []schema.IndexedFieldAccessor{{Ordinal: 0, Name: "field", Field: field}},
		IndexedByName: schema.IndexedFieldMap{"field": {Ordinal: 0, Name: "field", Field: field}},
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writer.WriteByte(persistedIndexVersion); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := writeSidecarUvarint(writer, 11); err != nil {
		t.Fatalf("write seq: %v", err)
	}
	writePersistTestUniverse(t, writer)
	if err := strmap.WriteSnapshot(writer, nil); err != nil {
		t.Fatalf("write strmap: %v", err)
	}
	if err := writeFields(writer, rt.Fields); err != nil {
		t.Fatalf("write fields: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write measure fields: %v", err)
	}
	if err := writeSidecarUvarint(writer, 1); err != nil {
		t.Fatalf("write regular count: %v", err)
	}
	if err := writeSidecarString(writer, "field"); err != nil {
		t.Fatalf("write field name: %v", err)
	}
	if err := writer.WriteByte(99); err != nil {
		t.Fatalf("write corrupt storage tag: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	file := filepath.Join(t.TempDir(), "corrupt.rbi")
	if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	_, err := Load(LoadConfig{
		File:            file,
		DBPath:          "test.db",
		Bucket:          []byte("bucket"),
		CurrentSeq:      11,
		Schema:          rt,
		StrMapCompactAt: 0,
		Errors:          Errors{Stale: errStale, Invalid: errInvalid},
	})
	if !errors.Is(err, errInvalid) {
		t.Fatalf("Load err=%v, want invalid sentinel", err)
	}
}

func TestLoadedFieldAndMeasureStorageRelease(t *testing.T) {
	field := persistTestField("field", 0)
	measureField := persistTestField("measure", 0)
	rt := &schema.Runtime{
		Fields:        map[string]*schema.Field{"field": field},
		MeasureFields: map[string]*schema.Field{"measure": measureField},
		Indexed:       []schema.IndexedFieldAccessor{{Ordinal: 0, Name: "field", Field: field}},
		IndexedByName: schema.IndexedFieldMap{"field": {Ordinal: 0, Name: "field", Field: field}},
		Measures:      []schema.MeasureFieldAccessor{{Ordinal: 0, Name: "measure", Field: measureField}},
		MeasuresByName: schema.MeasureFieldMap{
			"measure": {Ordinal: 0, Name: "measure", Field: measureField},
		},
	}
	fieldStorage := indexdata.NewNilFieldStorageOwned((posting.List{}).BuildAdded(1))
	defer fieldStorage.Release()

	measureStorage := persistTestMeasureStorage()
	defer measureStorage.Release()

	universe := (posting.List{}).BuildAdded(1)
	defer universe.Release()

	snap := &snapshot.View{
		Index:              []indexdata.FieldStorage{fieldStorage},
		NilIndex:           []indexdata.FieldStorage{},
		LenIndex:           []indexdata.FieldStorage{},
		Measure:            []indexdata.MeasureStorage{measureStorage},
		IndexedFieldByName: rt.IndexedByName,
		Universe:           universe,
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := storePayload(writer, rt, snap, &qexec.PlannerStatsSnapshot{
		Version: 1,
		Fields: map[string]qexec.PlannerFieldStats{
			"field": {DistinctKeys: 1, NonEmptyKeys: 1, TotalBucketCard: 1},
		},
	}); err != nil {
		t.Fatalf("storePayload: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	result, err := loadPayload(bufio.NewReader(&buf), rt, 0)
	if err != nil {
		t.Fatalf("loadPayload: %v", err)
	}
	if result.Storage.Index[0].KeyCount() == 0 {
		t.Fatalf("loaded field storage is empty")
	}
	if result.Storage.Measure[0].Rows() != 1 {
		t.Fatalf("loaded measure rows=%d, want 1", result.Storage.Measure[0].Rows())
	}
	result.Storage.Release()
}

func TestLoadRejectsStaleSequence(t *testing.T) {
	errInvalid := errors.New("invalid sentinel")
	errStale := errors.New("stale sentinel")

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writer.WriteByte(persistedIndexVersion); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := writeSidecarUvarint(writer, 10); err != nil {
		t.Fatalf("write seq: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	file := filepath.Join(t.TempDir(), "stale.rbi")
	if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	_, err := Load(LoadConfig{
		File:            file,
		DBPath:          "test.db",
		Bucket:          []byte("bucket"),
		CurrentSeq:      11,
		Schema:          &schema.Runtime{},
		StrMapCompactAt: 0,
		Errors:          Errors{Stale: errStale, Invalid: errInvalid},
	})
	if !errors.Is(err, errStale) {
		t.Fatalf("Load err=%v, want stale sentinel", err)
	}
	if !strings.Contains(err.Error(), "bucket sequence mismatch") {
		t.Fatalf("Load err=%v, want sequence mismatch diagnostic", err)
	}
}

func TestLoadRejectsUnsupportedVersion(t *testing.T) {
	errInvalid := errors.New("invalid sentinel")
	errStale := errors.New("stale sentinel")
	file := filepath.Join(t.TempDir(), "unsupported.rbi")
	if err := os.WriteFile(file, []byte{99}, 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	_, err := Load(LoadConfig{
		File:            file,
		DBPath:          "test.db",
		Bucket:          []byte("bucket"),
		CurrentSeq:      1,
		Schema:          &schema.Runtime{},
		StrMapCompactAt: 0,
		Errors:          Errors{Stale: errStale, Invalid: errInvalid},
	})
	if !errors.Is(err, errInvalid) {
		t.Fatalf("Load err=%v, want invalid sentinel", err)
	}
	if !strings.Contains(err.Error(), "unsupported persisted index version: 99") {
		t.Fatalf("Load err=%v, want version diagnostic", err)
	}
}

func TestStoreRemovesTempFileOnRenameFailure(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "sidecar.rbi")
	if err := os.Mkdir(final, 0o700); err != nil {
		t.Fatalf("mkdir final: %v", err)
	}

	err := Store(StoreConfig{
		File:      final,
		BucketSeq: 1,
		Schema:    &schema.Runtime{},
		Snapshot:  &snapshot.View{},
	})
	if err == nil {
		t.Fatalf("Store succeeded, want rename error")
	}
	if _, statErr := os.Stat(final + ".temp"); !os.IsNotExist(statErr) {
		t.Fatalf("temp file stat err=%v, want not exist", statErr)
	}
	if info, statErr := os.Stat(final); statErr != nil || !info.IsDir() {
		t.Fatalf("final dir stat info=%v err=%v", info, statErr)
	}
}

func TestStoreLoadRoundTrip(t *testing.T) {
	nameField := persistTestField("name", 0)
	tagsField := persistTestField("tags", 1)
	tagsField.Slice = true
	amountField := persistTestField("amount", 0)
	rt := &schema.Runtime{
		Fields: map[string]*schema.Field{
			"name": nameField,
			"tags": tagsField,
		},
		MeasureFields: map[string]*schema.Field{
			"amount": amountField,
		},
		Indexed: []schema.IndexedFieldAccessor{
			{Ordinal: 0, Name: "name", Field: nameField},
			{Ordinal: 1, Name: "tags", Field: tagsField},
		},
		IndexedByName: schema.IndexedFieldMap{
			"name": {Ordinal: 0, Name: "name", Field: nameField},
			"tags": {Ordinal: 1, Name: "tags", Field: tagsField},
		},
		Measures: []schema.MeasureFieldAccessor{
			{Ordinal: 0, Name: "amount", Field: amountField},
		},
		MeasuresByName: schema.MeasureFieldMap{
			"amount": {Ordinal: 0, Name: "amount", Field: amountField},
		},
	}

	nameStorage := persistTestRegularStorage("alice", 1)
	defer nameStorage.Release()
	tagsStorage := persistTestRegularStorage("go", 1)
	defer tagsStorage.Release()
	nilStorage := indexdata.NewNilFieldStorageOwned((posting.List{}).BuildAdded(2))
	defer nilStorage.Release()
	universe := (posting.List{}).BuildAdded(1)
	universe = universe.BuildAdded(2)
	defer universe.Release()
	lengths := indexdata.GetLenPostingMap(2)
	lengths[0] = (posting.List{}).BuildAdded(2)
	lengths[1] = (posting.List{}).BuildAdded(1)
	lenStorage, _ := indexdata.NewLenFieldStorageFromMapOwned(universe, lengths)
	indexdata.ReleaseLenPostingMap(lengths)
	defer lenStorage.Release()
	measureStorage := persistTestMeasureStorage()
	defer measureStorage.Release()

	snap := &snapshot.View{
		Seq:                77,
		Index:              []indexdata.FieldStorage{nameStorage, tagsStorage},
		NilIndex:           []indexdata.FieldStorage{nilStorage, {}},
		LenIndex:           []indexdata.FieldStorage{{}, lenStorage},
		Measure:            []indexdata.MeasureStorage{measureStorage},
		IndexedFieldByName: rt.IndexedByName,
		Universe:           universe,
	}
	stats := &qexec.PlannerStatsSnapshot{
		Version:             5,
		GeneratedAt:         time.Date(2026, 5, 28, 1, 2, 3, 4, time.FixedZone("MSK", 3*60*60)),
		UniverseCardinality: 2,
		Fields: map[string]qexec.PlannerFieldStats{
			"name": {DistinctKeys: 1, NonEmptyKeys: 1, TotalBucketCard: 1},
			"tags": {DistinctKeys: 1, NonEmptyKeys: 1, TotalBucketCard: 1},
		},
	}

	file := filepath.Join(t.TempDir(), "roundtrip.rbi")
	if err := Store(StoreConfig{
		File:         file,
		BucketSeq:    77,
		Schema:       rt,
		Snapshot:     snap,
		PlannerStats: stats,
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := os.Stat(file + ".temp"); !os.IsNotExist(err) {
		t.Fatalf("temp file stat err=%v, want not exist", err)
	}

	result, err := Load(LoadConfig{
		File:            file,
		DBPath:          "test.db",
		Bucket:          []byte("bucket"),
		CurrentSeq:      77,
		Schema:          rt,
		StrMapCompactAt: 0,
		Errors: Errors{
			Stale:   errors.New("stale sentinel"),
			Invalid: errors.New("invalid sentinel"),
		},
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer result.Storage.Release()
	if !result.LenLoaded {
		t.Fatalf("LenLoaded=false, want true")
	}
	if _, ok := result.SkipFields["name"]; !ok {
		t.Fatalf("name field was not marked loaded")
	}
	if _, ok := result.SkipFields["tags"]; !ok {
		t.Fatalf("tags field was not marked loaded")
	}
	if _, ok := result.SkipMeasureFields["amount"]; !ok {
		t.Fatalf("amount measure was not marked loaded")
	}
	if result.Storage.Index[0].KeyCount() == 0 || result.Storage.Index[1].KeyCount() == 0 {
		t.Fatalf("regular field storage was not loaded")
	}
	if result.Storage.NilIndex[0].KeyCount() == 0 {
		t.Fatalf("nil field storage was not loaded")
	}
	if result.Storage.LenIndex[1].KeyCount() == 0 {
		t.Fatalf("len field storage was not loaded")
	}
	if result.Storage.Measure[0].Rows() != 1 {
		t.Fatalf("measure rows=%d, want 1", result.Storage.Measure[0].Rows())
	}
	if result.PlannerStats == nil || result.PlannerStats.Version != 5 {
		t.Fatalf("planner stats=%+v, want version 5", result.PlannerStats)
	}
	if result.PlannerStats.GeneratedAt.Location() != time.UTC {
		t.Fatalf("planner stats GeneratedAt location=%v, want UTC", result.PlannerStats.GeneratedAt.Location())
	}
}

func TestLoadRecoversPanicWithDiagnostic(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writer.WriteByte(persistedIndexVersion); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := writeSidecarUvarint(writer, 1); err != nil {
		t.Fatalf("write seq: %v", err)
	}
	writePersistTestUniverse(t, writer)
	if err := strmap.WriteSnapshot(writer, nil); err != nil {
		t.Fatalf("write strmap: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	file := filepath.Join(t.TempDir(), "panic.rbi")
	if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	_, err := Load(LoadConfig{
		File:            file,
		DBPath:          "test.db",
		Bucket:          []byte("bucket"),
		CurrentSeq:      1,
		StrMapCompactAt: 0,
		Errors: Errors{
			Stale:   errors.New("stale sentinel"),
			Invalid: errors.New("invalid sentinel"),
		},
	})
	if err == nil {
		t.Fatalf("Load succeeded, want recovered panic")
	}
	if !strings.Contains(err.Error(), "stage=panic") || !strings.Contains(err.Error(), "persisted index file=") {
		t.Fatalf("Load err=%v, want panic diagnostic context", err)
	}
}

func TestPlannerStatsSnapshotCodecRoundTrip(t *testing.T) {
	generatedAt := time.Unix(10, 123).UTC()
	in := &qexec.PlannerStatsSnapshot{
		Version:             7,
		GeneratedAt:         generatedAt,
		UniverseCardinality: 100,
		Fields: map[string]qexec.PlannerFieldStats{
			"age": {
				DistinctKeys:    4,
				NonEmptyKeys:    4,
				TotalBucketCard: 20,
				MaxBucketCard:   9,
				P50BucketCard:   5,
				P95BucketCard:   9,
			},
			"old": {
				DistinctKeys:    1,
				NonEmptyKeys:    1,
				TotalBucketCard: 1,
				MaxBucketCard:   1,
				P50BucketCard:   1,
				P95BucketCard:   1,
			},
		},
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writePlannerStatsSnapshot(writer, in); err != nil {
		t.Fatalf("writePlannerStatsSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	out, err := readPlannerStatsSnapshot(bufio.NewReader(&buf), map[string]bool{"age": true})
	if err != nil {
		t.Fatalf("readPlannerStatsSnapshot: %v", err)
	}
	if out.Version != in.Version || !out.GeneratedAt.Equal(generatedAt) || out.UniverseCardinality != in.UniverseCardinality {
		t.Fatalf("snapshot header mismatch: got=%+v", out)
	}
	age, ok := out.Fields["age"]
	if !ok {
		t.Fatalf("missing compatible field stats")
	}
	if _, ok = out.Fields["old"]; ok {
		t.Fatalf("incompatible field stats retained")
	}
	if age.AvgBucketCard != 5 {
		t.Fatalf("AvgBucketCard=%v, want 5", age.AvgBucketCard)
	}
}

func TestPlannerStatsSnapshotCodecRejectsMissingCompatibleField(t *testing.T) {
	in := &qexec.PlannerStatsSnapshot{
		Version: 1,
		Fields: map[string]qexec.PlannerFieldStats{
			"age": {DistinctKeys: 1, NonEmptyKeys: 1, TotalBucketCard: 1},
		},
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writePlannerStatsSnapshot(writer, in); err != nil {
		t.Fatalf("writePlannerStatsSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readPlannerStatsSnapshot(bufio.NewReader(&buf), map[string]bool{"age": true, "name": true})
	if err == nil || !strings.Contains(err.Error(), `missing planner stats field "name"`) {
		t.Fatalf("readPlannerStatsSnapshot err=%v, want missing field", err)
	}
}

func TestPlannerStatsSnapshotCodecRejectsDuplicateCompatibleField(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writeSidecarUvarint(writer, 1); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write generated_at: %v", err)
	}
	if err := writeSidecarUvarint(writer, 0); err != nil {
		t.Fatalf("write universe: %v", err)
	}
	if err := writeSidecarUvarint(writer, 2); err != nil {
		t.Fatalf("write field count: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := writeSidecarString(writer, "age"); err != nil {
			t.Fatalf("write field name %d: %v", i, err)
		}
		if err := writePlannerFieldStats(writer, qexec.PlannerFieldStats{
			DistinctKeys:    1,
			NonEmptyKeys:    1,
			TotalBucketCard: 1,
		}); err != nil {
			t.Fatalf("write field stats %d: %v", i, err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readPlannerStatsSnapshot(bufio.NewReader(&buf), map[string]bool{"age": true})
	if err == nil || !strings.Contains(err.Error(), `duplicate planner stats field "age"`) {
		t.Fatalf("readPlannerStatsSnapshot err=%v, want duplicate field", err)
	}
}

func persistTestField(name string, ordinal int) *schema.Field {
	return &schema.Field{
		Name:      name,
		IndexKind: schema.IndexDefault,
		Kind:      reflect.Int,
		DBName:    name,
		Index:     []int{ordinal},
	}
}

func persistTestMeasureStorage() indexdata.MeasureStorage {
	entries := indexdata.GetMeasureEntrySlice(1)
	entries = append(entries, indexdata.MeasureEntry{ID: 1, Value: 10})
	return indexdata.NewMeasureStorageFromEntriesOwned(entries)
}

func persistTestRegularStorage(key string, id uint64) indexdata.FieldStorage {
	m := indexdata.GetPostingMap()
	m[key] = (posting.List{}).BuildAdded(id)
	return indexdata.NewRegularFieldStorageFromPostingMapOwned(m, false)
}

func writePersistTestUniverse(t *testing.T, writer *bufio.Writer) {
	t.Helper()
	universe := (posting.List{}).BuildAdded(1)
	defer universe.Release()
	if err := universe.WriteTo(writer); err != nil {
		t.Fatalf("write universe: %v", err)
	}
}

func TestPlannerStatsSnapshotCodecNilPayload(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writePlannerStatsSnapshot(writer, nil); err != nil {
		t.Fatalf("writePlannerStatsSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	out, err := readPlannerStatsSnapshot(bufio.NewReader(&buf), nil)
	if err != nil {
		t.Fatalf("readPlannerStatsSnapshot: %v", err)
	}
	if out != nil {
		t.Fatalf("decoded nil payload as %+v", out)
	}
}
