package rebuild

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type rebuildTestRec struct {
	Name  string   `db:"name" rbi:"index"`
	Tags  []string `db:"tags" rbi:"index"`
	Score uint64   `db:"score" rbi:"measure"`
}

type rebuildNoIndexRec struct {
	Name string
}

func openRebuildTestBolt(t *testing.T, bucket []byte) *bbolt.DB {
	t.Helper()

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "rebuild.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err = db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucket)
		return e
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return db
}

func putRebuildTestRec(t *testing.T, db *bbolt.DB, bucket []byte, id uint64, rec rebuildTestRec) {
	t.Helper()

	data, err := msgpack.Marshal(&rec)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	if err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(id, &key), data)
	}); err != nil {
		t.Fatalf("put record: %v", err)
	}
}

func putRebuildTestStringRec(t *testing.T, db *bbolt.DB, bucket []byte, mapBucket []byte, key string, rec rebuildTestRec) uint64 {
	t.Helper()

	data, err := msgpack.Marshal(&rec)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	var idx uint64
	if err = db.Update(func(tx *bbolt.Tx) error {
		m := tx.Bucket(mapBucket)
		var err error
		idx, err = m.NextSequence()
		if err != nil {
			return err
		}
		var mapKey [8]byte
		if err = m.Put(keycodec.U64BytesWithBuf(idx, &mapKey), keycodec.StringBytes(key)); err != nil {
			return err
		}
		value := keycodec.AppendU64Bytes(nil, idx)
		value = append(value, data...)
		return tx.Bucket(bucket).Put(keycodec.StringBytes(key), value)
	}); err != nil {
		t.Fatalf("put string record: %v", err)
	}
	return idx
}

func createRebuildStringMap(t testing.TB, db *bbolt.DB, bucket []byte) []byte {
	t.Helper()

	mapBucket := rebuildStringMapBucket(bucket)
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(mapBucket)
		return err
	}); err != nil {
		t.Fatalf("create string map: %v", err)
	}
	return mapBucket
}

func rebuildStringMapBucket(bucket []byte) []byte {
	return append(append([]byte(nil), bucket...), ".rbimap"...)
}

func compileRebuildTestSchema(t *testing.T) *schema.Schema {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(rebuildTestRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func compileRebuildNoIndexSchema(t *testing.T) *schema.Schema {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(rebuildNoIndexRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func newRebuildTestState(s *schema.Schema) State {
	slotCount := len(s.Indexed)
	measureCount := len(s.Measures)
	lenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(lenZeroComplement)
	return State{
		Index:             indexdata.GetFieldStorageSlice(slotCount)[:slotCount],
		NilIndex:          indexdata.GetFieldStorageSlice(slotCount)[:slotCount],
		LenIndex:          indexdata.GetFieldStorageSlice(slotCount)[:slotCount],
		LenZeroComplement: lenZeroComplement,
		Measure:           indexdata.GetMeasureStorageSlice(measureCount)[:measureCount],
	}
}

func decodeRebuildTestRec(data []byte) (unsafe.Pointer, error) {
	rec := new(rebuildTestRec)
	if err := msgpack.Unmarshal(data, rec); err != nil {
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func releaseRebuildTestRec(ptr unsafe.Pointer) {
	*(*rebuildTestRec)(ptr) = rebuildTestRec{}
}

func baseRebuildTestConfig(db *bbolt.DB, bucket []byte, s *schema.Schema) Config {
	return Config{
		Bolt:       db,
		DataBucket: bucket,
		Schema:     s,
		Decode:     decodeRebuildTestRec,
		Release:    releaseRebuildTestRec,
	}
}

func TestBuildMaterializesUint64FieldsLenAndMeasure(t *testing.T) {
	bucket := []byte("rebuild_full")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: "alice", Tags: []string{"go", "db"}, Score: 10})
	putRebuildTestRec(t, db, bucket, 2, rebuildTestRec{Name: "bob", Score: 20})

	result, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !result.Publish || !result.Stats || result.LenLoaded {
		t.Fatalf("unexpected result flags: publish=%v stats=%v lenLoaded=%v", result.Publish, result.Stats, result.LenLoaded)
	}
	if result.Storage.Universe.Cardinality() != 2 {
		t.Fatalf("universe cardinality=%d want 2", result.Storage.Universe.Cardinality())
	}

	nameOrd := rt.IndexedByName["name"].Ordinal
	ids := indexdata.NewFieldIndexViewFromStorage(result.Storage.Index[nameOrd]).LookupPostingRetained("alice")
	if !ids.Contains(1) || ids.Contains(2) {
		t.Fatalf("name index for alice mismatch")
	}
	ids.Release()

	tagsOrd := rt.IndexedByName["tags"].Ordinal
	ids = indexdata.NewFieldIndexViewFromStorage(result.Storage.LenIndex[tagsOrd]).LookupPostingRetainedKey(keycodec.FromU64(2))
	if !ids.Contains(1) || ids.Contains(2) {
		t.Fatalf("tags len=2 index mismatch")
	}
	ids.Release()

	scoreOrd := rt.MeasuresByName["score"].Ordinal
	if got, ok := result.Storage.Measure[scoreOrd].Lookup(2); !ok || got != 20 {
		t.Fatalf("score measure for id=2 got=%d ok=%v want=20/true", got, ok)
	}
}

func TestBuildRejectsTooLongIndexedString(t *testing.T) {
	bucket := []byte("rebuild_string_limit")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: strings.Repeat("x", indexdata.FieldStringRefMax+1)})

	_, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected indexed string limit error")
	}
	msg := err.Error()
	for _, want := range []string{`field="name"`, "indexed string value len", "exceeds limit"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("indexed string limit error missing %q: %v", want, err)
		}
	}
}

func TestBuildDecodeReleaseCallbackContract(t *testing.T) {
	bucket := []byte("rebuild_callbacks")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	const rows = 16
	for i := uint64(1); i <= rows; i++ {
		putRebuildTestRec(t, db, bucket, i, rebuildTestRec{Name: fmt.Sprintf("user_%d", i), Score: i})
	}

	var decodes atomic.Int64
	var releases atomic.Int64
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Decode = func(data []byte) (unsafe.Pointer, error) {
		decodes.Add(1)
		return decodeRebuildTestRec(data)
	}
	cfg.Release = func(ptr unsafe.Pointer) {
		releases.Add(1)
		releaseRebuildTestRec(ptr)
	}
	_, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := decodes.Load(); got != rows {
		t.Fatalf("decode calls=%d want=%d", got, rows)
	}
	if got := releases.Load(); got != rows {
		t.Fatalf("release calls=%d want=%d", got, rows)
	}
}

func TestBuildStorageSurvivesDecodedRecordReleaseAndPoison(t *testing.T) {
	bucket := []byte("rebuild_decode_release_poison")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	if err := db.Update(func(tx *bbolt.Tx) error {
		var key [8]byte
		return tx.Bucket(bucket).Put(keycodec.U64BytesWithBuf(1, &key), []byte{1})
	}); err != nil {
		t.Fatalf("put raw record: %v", err)
	}

	var bufs [][]byte
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Decode = func([]byte) (unsafe.Pointer, error) {
		name := []byte("release-owned-name")
		tag := []byte("release-owned-tag")
		bufs = [][]byte{name, tag}
		rec := &rebuildTestRec{
			Name:  unsafe.String(unsafe.SliceData(name), len(name)),
			Tags:  []string{unsafe.String(unsafe.SliceData(tag), len(tag))},
			Score: 99,
		}
		return unsafe.Pointer(rec), nil
	}
	cfg.Release = func(ptr unsafe.Pointer) {
		*(*rebuildTestRec)(ptr) = rebuildTestRec{}
		for _, buf := range bufs {
			for i := range buf {
				buf[i] = 0xa5
			}
		}
	}

	result, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer result.Storage.Release()

	nameOrd := rt.IndexedByName["name"].Ordinal
	ids := indexdata.NewFieldIndexViewFromStorage(result.Storage.Index[nameOrd]).LookupPostingRetained("release-owned-name")
	if !ids.Contains(1) {
		t.Fatalf("name index lost decoded string after release")
	}
	ids.Release()

	tagsOrd := rt.IndexedByName["tags"].Ordinal
	ids = indexdata.NewFieldIndexViewFromStorage(result.Storage.Index[tagsOrd]).LookupPostingRetained("release-owned-tag")
	if !ids.Contains(1) {
		t.Fatalf("tags index lost decoded string after release")
	}
	ids.Release()

	ids = indexdata.NewFieldIndexViewFromStorage(result.Storage.LenIndex[tagsOrd]).LookupPostingRetainedKey(keycodec.FromU64(1))
	if !ids.Contains(1) {
		t.Fatalf("tags len index lost decoded string record after release")
	}
	ids.Release()

	scoreOrd := rt.MeasuresByName["score"].Ordinal
	if got, ok := result.Storage.Measure[scoreOrd].Lookup(1); !ok || got != 99 {
		t.Fatalf("score measure got=%d ok=%v want=99/true", got, ok)
	}
}

func TestBuildPartialPreservesSkippedFieldAndMeasureSlots(t *testing.T) {
	bucket := []byte("rebuild_partial")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: "alice", Tags: []string{"go"}, Score: 10})
	full, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("full Build: %v", err)
	}

	state := State{
		Index:             full.Storage.Index,
		NilIndex:          full.Storage.NilIndex,
		LenIndex:          full.Storage.LenIndex,
		LenZeroComplement: full.Storage.LenZeroComplement,
		Measure:           full.Storage.Measure,
		Universe:          full.Storage.Universe,
	}
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Current = &snapshot.View{}
	cfg.SkipFields = map[string]struct{}{"name": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}

	partial, err := Build(cfg, state)
	if err != nil {
		t.Fatalf("partial Build: %v", err)
	}
	nameOrd := rt.IndexedByName["name"].Ordinal
	if partial.Storage.Index[nameOrd] != state.Index[nameOrd] {
		t.Fatalf("skipped name storage was not preserved")
	}
	scoreOrd := rt.MeasuresByName["score"].Ordinal
	if partial.Storage.Measure[scoreOrd] != state.Measure[scoreOrd] {
		t.Fatalf("skipped score measure storage was not preserved")
	}
}

func TestBuildPartialPublishRetainsSkippedStorage(t *testing.T) {
	bucket := []byte("rebuild_partial_publish")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: "alice", Tags: []string{"go"}, Score: 10})
	base, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("base Build: %v", err)
	}
	manager := snapshot.NewRegistry(false)
	prev := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, base.Storage)
	manager.Publish(prev)

	putRebuildTestRec(t, db, bucket, 2, rebuildTestRec{Name: "bob", Tags: []string{"db"}, Score: 20})
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Current = prev
	cfg.SkipFields = map[string]struct{}{"name": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}
	partial, err := Build(cfg, State{
		Index:             prev.Index,
		NilIndex:          prev.NilIndex,
		LenIndex:          prev.LenIndex,
		LenZeroComplement: prev.LenZeroComplement,
		Measure:           prev.Measure,
		Universe:          prev.Universe,
	})
	if err != nil {
		t.Fatalf("partial Build: %v", err)
	}
	nameOrd := rt.IndexedByName["name"].Ordinal
	prevNameStorage := prev.Index[nameOrd]
	scoreOrd := rt.MeasuresByName["score"].Ordinal
	prevScoreStorage := prev.Measure[scoreOrd]
	next := snapshot.NewView(2, prev, rt, snapshot.CacheConfig{}, partial.Storage)
	manager.Publish(next)

	if next.Index[nameOrd] != prevNameStorage {
		t.Fatalf("skipped name storage was not shared into next snapshot")
	}
	ids := indexdata.NewFieldIndexViewFromStorage(next.Index[nameOrd]).LookupPostingRetained("alice")
	if !ids.Contains(1) {
		t.Fatalf("next snapshot lost skipped name storage after publish")
	}
	ids.Release()
	if next.Measure[scoreOrd] != prevScoreStorage {
		t.Fatalf("skipped measure storage was not shared into next snapshot")
	}
	if got, ok := next.Measure[scoreOrd].Lookup(1); !ok || got != 10 {
		t.Fatalf("next snapshot lost skipped measure storage: got=%d ok=%v", got, ok)
	}
}

func TestBuildPartialPreservesSkippedLenStorageAndComplementFlag(t *testing.T) {
	bucket := []byte("rebuild_partial_len_preserve")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	for i := uint64(1); i <= 90; i++ {
		rec := rebuildTestRec{Name: fmt.Sprintf("user_%d", i)}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		putRebuildTestRec(t, db, bucket, i, rec)
	}
	full, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("full Build: %v", err)
	}
	tagsOrd := rt.IndexedByName["tags"].Ordinal
	if !full.Storage.LenZeroComplement[tagsOrd] {
		t.Fatalf("expected full build to use zero-complement for tags")
	}

	state := State{
		Index:             full.Storage.Index,
		NilIndex:          full.Storage.NilIndex,
		LenIndex:          full.Storage.LenIndex,
		LenZeroComplement: full.Storage.LenZeroComplement,
		Measure:           full.Storage.Measure,
		Universe:          full.Storage.Universe,
	}
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Current = &snapshot.View{}
	cfg.SkipFields = map[string]struct{}{"tags": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}

	partial, err := Build(cfg, state)
	if err != nil {
		t.Fatalf("partial Build: %v", err)
	}
	if partial.Storage.LenIndex[tagsOrd] != state.LenIndex[tagsOrd] {
		t.Fatalf("skipped tags len storage was not preserved")
	}
	if !partial.Storage.LenZeroComplement[tagsOrd] {
		t.Fatalf("skipped tags zero-complement flag was not preserved")
	}
}

func TestBuildPartialClearsStaleLenZeroComplementFlagForRebuiltField(t *testing.T) {
	bucket := []byte("rebuild_partial_len_clear")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: "alice", Tags: []string{"go"}, Score: 10})
	state := newRebuildTestState(rt)
	tagsOrd := rt.IndexedByName["tags"].Ordinal
	state.LenZeroComplement[tagsOrd] = true

	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.Current = &snapshot.View{}
	cfg.SkipFields = map[string]struct{}{"name": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}

	result, err := Build(cfg, state)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if result.Storage.LenZeroComplement[tagsOrd] {
		t.Fatalf("rebuilt tags zero-complement flag remained set")
	}
}

func TestBuildNoActivePaths(t *testing.T) {
	rt := compileRebuildTestSchema(t)
	skipFields := map[string]struct{}{"name": {}, "tags": {}}
	skipMeasures := map[string]struct{}{"score": {}}

	state := newRebuildTestState(rt)
	state.LenLoaded = true
	result, err := Build(Config{
		Schema:            rt,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasures,
	}, state)
	if err != nil {
		t.Fatalf("Build len-loaded no-active: %v", err)
	}
	if result.Publish || result.Stats || result.LenLoaded {
		t.Fatalf("unexpected len-loaded no-active result: publish=%v stats=%v lenLoaded=%v", result.Publish, result.Stats, result.LenLoaded)
	}
	if result.Storage.Index != nil || result.Storage.NilIndex != nil || result.Storage.LenIndex != nil || result.Storage.Measure != nil || !result.Storage.Universe.IsEmpty() {
		t.Fatalf("no-publish result transferred storage: %+v", result.Storage)
	}

	bucket := []byte("rebuild_no_active")
	db := openRebuildTestBolt(t, bucket)
	putRebuildTestRec(t, db, bucket, 1, rebuildTestRec{Name: "alice", Tags: []string{"go", "db"}, Score: 10})
	full, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("full Build: %v", err)
	}
	state = State{
		Index:     full.Storage.Index,
		NilIndex:  full.Storage.NilIndex,
		Measure:   full.Storage.Measure,
		Universe:  full.Storage.Universe,
		LenLoaded: false,
	}
	result, err = Build(Config{
		Schema:            rt,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasures,
	}, state)
	if err != nil {
		t.Fatalf("Build missing-len no-active: %v", err)
	}
	if !result.Publish || result.Stats || result.LenLoaded {
		t.Fatalf("unexpected missing-len no-active result: publish=%v stats=%v lenLoaded=%v", result.Publish, result.Stats, result.LenLoaded)
	}
	tagsOrd := rt.IndexedByName["tags"].Ordinal
	if result.Storage.LenIndex[tagsOrd].KeyCount() == 0 {
		t.Fatalf("expected no-active missing-len path to rebuild tags len index")
	}
	view := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, result.Storage)
	ids := indexdata.NewFieldIndexViewFromStorage(view.LenIndex[tagsOrd]).LookupPostingRetainedKey(keycodec.FromU64(2))
	if !ids.Contains(1) {
		t.Fatalf("no-active missing-len snapshot cannot query tags len=2")
	}
	ids.Release()
}

func TestBuildScanErrorDiagnostics(t *testing.T) {
	bucket := []byte("rebuild_error")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	if err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), []byte{0xff})
	}); err != nil {
		t.Fatalf("put corrupt record: %v", err)
	}

	_, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected decode error")
	}
	msg := err.Error()
	for _, want := range []string{"scan error", "id=1", "idx=1", "value_len=1", "value_prefix_hex=ff"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("decode error missing %q: %v", want, err)
		}
	}
}

func TestBuildRejectsInvalidUint64KeySize(t *testing.T) {
	bucket := []byte("rebuild_bad_key")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)

	if err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Put([]byte{1, 2, 3}, []byte{0xff})
	}); err != nil {
		t.Fatalf("put bad key: %v", err)
	}

	_, err := Build(baseRebuildTestConfig(db, bucket, rt), newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected invalid key error")
	}
	if msg := err.Error(); !strings.Contains(msg, "invalid uint64 key size") || !strings.Contains(msg, "key_len=3") {
		t.Fatalf("unexpected invalid key error: %v", err)
	}
}

func TestBuildStringKeysUseDurableIDs(t *testing.T) {
	bucket := []byte("rebuild_strings")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	putRebuildTestStringRec(t, db, bucket, mapBucket, "user-1", rebuildTestRec{Name: "alice", Tags: []string{"go"}, Score: 10})

	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.StrKey = true
	cfg.StrMapBucket = mapBucket
	cfg.StrKeyIndex = true
	result, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer result.Storage.Release()
	if result.Storage.Universe.Cardinality() != 1 {
		t.Fatalf("universe cardinality=%d want 1", result.Storage.Universe.Cardinality())
	}
	if !result.Storage.Universe.Contains(1) {
		t.Fatalf("universe does not contain durable id 1")
	}
	ids := indexdata.NewFieldIndexViewFromStorage(result.Storage.KeyIndex).LookupPostingRetained("user-1")
	if ids.Cardinality() != 1 || !ids.Contains(1) {
		t.Fatalf("key index for user-1 mismatch")
	}
	ids.Release()
}

func TestBuildStringKeyOnlyIndexScansKeysWithoutDecode(t *testing.T) {
	bucket := []byte("rebuild_key_only")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildNoIndexSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	bID := putRebuildTestStringRec(t, db, bucket, mapBucket, "b", rebuildTestRec{Name: "ignored-b"})
	aaID := putRebuildTestStringRec(t, db, bucket, mapBucket, "aa", rebuildTestRec{Name: "ignored-aa"})
	cID := putRebuildTestStringRec(t, db, bucket, mapBucket, "c", rebuildTestRec{Name: "ignored-c"})

	result, err := Build(Config{
		Bolt:         db,
		DataBucket:   bucket,
		StrMapBucket: mapBucket,
		Schema:       rt,
		StrKey:       true,
		StrKeyIndex:  true,
		Decode: func([]byte) (unsafe.Pointer, error) {
			t.Fatalf("decode called")
			return nil, nil
		},
		Release: func(unsafe.Pointer) {
			t.Fatalf("release called")
		},
	}, newRebuildTestState(rt))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer result.Storage.Release()

	if !result.Publish || !result.Stats || result.LenLoaded {
		t.Fatalf("unexpected result flags: publish=%v stats=%v lenLoaded=%v", result.Publish, result.Stats, result.LenLoaded)
	}

	ov := indexdata.NewFieldIndexViewFromStorage(result.Storage.KeyIndex)
	if ov.KeyCount() != 3 || ov.Rows() != 3 {
		t.Fatalf("key index shape keys=%d rows=%d", ov.KeyCount(), ov.Rows())
	}

	wantKeys := []string{"aa", "b", "c"}
	wantIDs := []uint64{aaID, bID, cID}
	for i := range wantKeys {
		if got := ov.KeyAt(i).UnsafeString(); got != wantKeys[i] {
			t.Fatalf("key[%d]=%q want %q", i, got, wantKeys[i])
		}
		ids := ov.LookupPostingRetained(wantKeys[i])
		if ids.Cardinality() != 1 || !ids.Contains(wantIDs[i]) {
			t.Fatalf("posting[%q] cardinality=%d contains_id=%v", wantKeys[i], ids.Cardinality(), ids.Contains(wantIDs[i]))
		}
		ids.Release()
		if !result.Storage.Universe.Contains(wantIDs[i]) {
			t.Fatalf("universe does not contain durable id %d", wantIDs[i])
		}
	}
}

func TestBuildStringKeyOnlyPreservesLoadedUniverse(t *testing.T) {
	bucket := []byte("rebuild_key_only_preserve_universe")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildNoIndexSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	aID := putRebuildTestStringRec(t, db, bucket, mapBucket, "a", rebuildTestRec{Name: "ignored-a"})
	bID := putRebuildTestStringRec(t, db, bucket, mapBucket, "b", rebuildTestRec{Name: "ignored-b"})
	loadedUniverse := posting.BuildFromSorted([]uint64{aID, bID})
	state := newRebuildTestState(rt)
	state.Universe = loadedUniverse

	result, err := Build(Config{
		Bolt:         db,
		DataBucket:   bucket,
		StrMapBucket: mapBucket,
		Schema:       rt,
		StrKey:       true,
		StrKeyIndex:  true,
		Decode: func([]byte) (unsafe.Pointer, error) {
			t.Fatalf("decode called")
			return nil, nil
		},
		Release: func(unsafe.Pointer) {
			t.Fatalf("release called")
		},
	}, state)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer result.Storage.Release()

	if !result.Storage.Universe.SharesPayload(loadedUniverse) {
		t.Fatalf("key-only rebuild replaced loaded compatible universe")
	}
	if result.Storage.Universe.Cardinality() != 2 || !result.Storage.Universe.Contains(aID) || !result.Storage.Universe.Contains(bID) {
		t.Fatalf("preserved universe cardinality=%d contains a/b=%v/%v", result.Storage.Universe.Cardinality(), result.Storage.Universe.Contains(aID), result.Storage.Universe.Contains(bID))
	}
	ov := indexdata.NewFieldIndexViewFromStorage(result.Storage.KeyIndex)
	if ov.KeyCount() != 2 || ov.Rows() != 2 {
		t.Fatalf("key index shape keys=%d rows=%d", ov.KeyCount(), ov.Rows())
	}
}

func TestBuildStringKeyOnlyRejectsMalformedValuePrefix(t *testing.T) {
	bucket := []byte("rebuild_key_only_bad_prefix")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildNoIndexSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(keycodec.StringBytes("bad-key"), []byte{0xff})
	}); err != nil {
		t.Fatalf("put malformed string record: %v", err)
	}

	_, err := Build(Config{
		Bolt:         db,
		DataBucket:   bucket,
		StrMapBucket: mapBucket,
		Schema:       rt,
		StrKey:       true,
		StrKeyIndex:  true,
		Decode: func([]byte) (unsafe.Pointer, error) {
			t.Fatalf("decode called")
			return nil, nil
		},
	}, newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected malformed value prefix error")
	}
	msg := err.Error()
	for _, want := range []string{"scan error", `id="bad-key"`, "value_len=1", "value_prefix_hex=ff", "value shorter"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("malformed prefix error missing %q: %v", want, err)
		}
	}
}

func TestBuildStringKeyOnlyRejectsZeroStringID(t *testing.T) {
	bucket := []byte("rebuild_key_only_zero_idx")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildNoIndexSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(keycodec.StringBytes("zero-key"), make([]byte, 8))
	}); err != nil {
		t.Fatalf("put zero-id string record: %v", err)
	}

	_, err := Build(Config{
		Bolt:         db,
		DataBucket:   bucket,
		StrMapBucket: mapBucket,
		Schema:       rt,
		StrKey:       true,
		StrKeyIndex:  true,
		Decode: func([]byte) (unsafe.Pointer, error) {
			t.Fatalf("decode called")
			return nil, nil
		},
	}, newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected zero string id error")
	}
	if msg := err.Error(); !strings.Contains(msg, `id="zero-key"`) || !strings.Contains(msg, "zero string id") {
		t.Fatalf("unexpected zero string id error: %v", err)
	}
}

func TestBuildStringKeyOnlyRejectsStringMapSequenceBelowLiveID(t *testing.T) {
	bucket := []byte("rebuild_key_only_bad_sequence")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildNoIndexSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(keycodec.StringBytes("orphan-key"), keycodec.AppendU64Bytes(nil, 7))
	}); err != nil {
		t.Fatalf("put orphan string record: %v", err)
	}

	_, err := Build(Config{
		Bolt:         db,
		DataBucket:   bucket,
		StrMapBucket: mapBucket,
		Schema:       rt,
		StrKey:       true,
		StrKeyIndex:  true,
		Decode: func([]byte) (unsafe.Pointer, error) {
			t.Fatalf("decode called")
			return nil, nil
		},
	}, newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected string map sequence error")
	}
	if msg := err.Error(); !strings.Contains(msg, "string map sequence 0 lower than max live idx 7") {
		t.Fatalf("unexpected string map sequence error: %v", err)
	}
}

func TestMaterializeStringKeyMeasureRunsSortByNumericID(t *testing.T) {
	rt := compileRebuildTestSchema(t)
	score := rt.MeasuresByName["score"]
	buf := indexdata.GetMeasureEntrySlice(0)
	buf = append(buf,
		indexdata.MeasureEntry{ID: 2, Value: 20},
		indexdata.MeasureEntry{ID: 1, Value: 10},
	)

	localMeasures := make([][][]indexdata.MeasureEntry, 1)
	localMeasures[0] = make([][]indexdata.MeasureEntry, len(rt.Measures))
	localMeasures[0][score.Ordinal] = buf

	result := materialize(
		Config{Schema: rt, StrKey: true},
		newRebuildTestState(rt),
		nil,
		[]schema.MeasureFieldAccessor{score},
		&buildData{localMeasureStates: localMeasures},
		time.Now(),
	)
	defer result.Storage.Release()

	if got, ok := result.Storage.Measure[score.Ordinal].Lookup(1); !ok || got != 10 {
		t.Fatalf("score measure for id=1 got=%d ok=%v want=10/true", got, ok)
	}
	if got, ok := result.Storage.Measure[score.Ordinal].Lookup(2); !ok || got != 20 {
		t.Fatalf("score measure for id=2 got=%d ok=%v want=20/true", got, ok)
	}
}

func TestBuildStringKeyScanFailureDoesNotMutateMappings(t *testing.T) {
	bucket := []byte("rebuild_string_error")
	db := openRebuildTestBolt(t, bucket)
	rt := compileRebuildTestSchema(t)
	mapBucket := createRebuildStringMap(t, db, bucket)

	if err := db.Update(func(tx *bbolt.Tx) error {
		m := tx.Bucket(mapBucket)
		idx, err := m.NextSequence()
		if err != nil {
			return err
		}
		var mapKey [8]byte
		if err = m.Put(keycodec.U64BytesWithBuf(idx, &mapKey), keycodec.StringBytes("bad-key")); err != nil {
			return err
		}
		value := keycodec.AppendU64Bytes(nil, idx)
		value = append(value, 0xff)
		return tx.Bucket(bucket).Put(keycodec.StringBytes("bad-key"), value)
	}); err != nil {
		t.Fatalf("put corrupt string record: %v", err)
	}

	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.StrKey = true
	cfg.StrMapBucket = mapBucket
	_, err := Build(cfg, newRebuildTestState(rt))
	if err == nil {
		t.Fatalf("expected string-key decode error")
	}
	if msg := err.Error(); !strings.Contains(msg, "scan error") || !strings.Contains(msg, `id="bad-key"`) {
		t.Fatalf("unexpected string-key scan error: %v", err)
	}
	if seq := readRebuildStringMapSequence(t, db, mapBucket); seq != 1 {
		t.Fatalf("string map sequence = %d, want 1", seq)
	}
}

func readRebuildStringMapSequence(t testing.TB, db *bbolt.DB, mapBucket []byte) uint64 {
	t.Helper()

	var seq uint64
	if err := db.View(func(tx *bbolt.Tx) error {
		seq = tx.Bucket(mapBucket).Sequence()
		return nil
	}); err != nil {
		t.Fatalf("read string map sequence: %v", err)
	}
	return seq
}
