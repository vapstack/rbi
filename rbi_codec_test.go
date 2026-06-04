package rbi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type codecRec struct {
	Name string `db:"name" rbi:"index"`
	Age  int    `db:"age" rbi:"index"`
}

type codecSparseRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
	Opt  *string  `db:"opt" rbi:"index"`
}

type codecBadValueDecodeRec struct {
	Name string `db:"name" rbi:"index"`
}

const (
	codecFieldName = 1 << iota
	codecFieldTags
	codecFieldOpt
)

func (r *codecRec) EncodeRBI(w io.Writer) error {
	if _, err := io.WriteString(w, "RBI1"); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(r.Name))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, r.Name); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, int64(r.Age))
}

func (r *codecRec) DecodeRBI(reader io.Reader) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if string(header[:]) != "RBI1" {
		return io.ErrUnexpectedEOF
	}
	var nameLen uint32
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(reader, name); err != nil {
		return err
	}
	var age int64
	if err := binary.Read(reader, binary.BigEndian, &age); err != nil {
		return err
	}
	r.Name = string(name)
	r.Age = int(age)
	return nil
}

func (r *codecSparseRec) EncodeRBI(w io.Writer) error {
	var flags byte
	if r.Name != "" {
		flags |= codecFieldName
	}
	if len(r.Tags) != 0 {
		flags |= codecFieldTags
	}
	if r.Opt != nil {
		flags |= codecFieldOpt
	}
	if err := binary.Write(w, binary.BigEndian, flags); err != nil {
		return err
	}
	if flags&codecFieldName != 0 {
		if err := writeCodecString(w, r.Name); err != nil {
			return err
		}
	}
	if flags&codecFieldTags != 0 {
		if err := binary.Write(w, binary.BigEndian, uint32(len(r.Tags))); err != nil {
			return err
		}
		for i := range r.Tags {
			if err := writeCodecString(w, r.Tags[i]); err != nil {
				return err
			}
		}
	}
	if flags&codecFieldOpt != 0 {
		return writeCodecString(w, *r.Opt)
	}
	return nil
}

func (r *codecSparseRec) DecodeRBI(reader io.Reader) error {
	// Intentionally update only fields present in the payload. If decode reuses a
	// dirty pooled struct without zeroing it first, absent fields leak through.
	var flags byte
	if err := binary.Read(reader, binary.BigEndian, &flags); err != nil {
		return err
	}
	if flags&codecFieldName != 0 {
		name, err := readCodecString(reader)
		if err != nil {
			return err
		}
		r.Name = name
	}
	if flags&codecFieldTags != 0 {
		var n uint32
		if err := binary.Read(reader, binary.BigEndian, &n); err != nil {
			return err
		}
		r.Tags = r.Tags[:0]
		for i := uint32(0); i < n; i++ {
			tag, err := readCodecString(reader)
			if err != nil {
				return err
			}
			r.Tags = append(r.Tags, tag)
		}
	}
	if flags&codecFieldOpt != 0 {
		v, err := readCodecString(reader)
		if err != nil {
			return err
		}
		r.Opt = &v
	}
	return nil
}

func (r codecBadValueDecodeRec) EncodeRBI(w io.Writer) error {
	return nil
}

func (r codecBadValueDecodeRec) DecodeRBI(reader io.Reader) error {
	return nil
}

func writeCodecString(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readCodecString(r io.Reader) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func openTempDBUint64CodecRec(t *testing.T, options ...Options) (*DB[uint64, codecRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec.db")
	db, raw := openBoltAndNew[uint64, codecRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func openTempDBUint64CodecSparseRec(t *testing.T, options ...Options) (*DB[uint64, codecSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec_sparse.db")
	db, raw := openBoltAndNew[uint64, codecSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func TestCodec_SetBeforeStoreSnapshotUsesCodecMethods(t *testing.T) {
	db, _ := openTempDBUint64CodecRec(t)

	if err := db.Set(1, &codecRec{Name: "alice", Age: 10}, BeforeStore(func(_ uint64, oldValue, newValue *codecRec) error {
		if oldValue != nil {
			t.Fatalf("unexpected old value: %#v", oldValue)
		}
		newValue.Age = 11
		return nil
	})); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer db.ReleaseRecords(got)

	if got.Name != "alice" || got.Age != 11 {
		t.Fatalf("unexpected record: %#v", got)
	}

	if err = db.Bolt().View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.BucketName())
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		var key [8]byte
		payload := b.Get(keycodec.U64BytesWithBuf(1, &key))
		if len(payload) == 0 {
			return fmt.Errorf("stored payload missing")
		}
		if !bytes.HasPrefix(payload, []byte("RBI1")) {
			return fmt.Errorf("expected Codec payload prefix, got %x", payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("Bolt().View: %v", err)
	}
}

func TestCodec_NewRejectsValueReceiverDecodeRBI(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	_, err := New[uint64, codecBadValueDecodeRec](raw, testOptions(Options{}))
	if err == nil {
		t.Fatalf("expected New to reject value-receiver DecodeRBI")
	}
	if !strings.Contains(err.Error(), "DecodeRBI must have pointer receiver") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCodec_NewBuildIndexUsesCodecDecode(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_bucket"
	var payload bytes.Buffer
	if err := (&codecRec{Name: "alice", Age: 10}).EncodeRBI(&payload); err != nil {
		t.Fatalf("EncodeRBI: %v", err)
	}

	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), payload.Bytes())
	}); err != nil {
		t.Fatalf("seed custom payload: %v", err)
	}

	db, err := New[uint64, codecRec](raw, testOptions(Options{BucketName: bucket}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = db.Close() }()

	got, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected query result after reopen: %v", got)
	}

	rec, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if rec == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer db.ReleaseRecords(rec)

	if rec.Name != "alice" || rec.Age != 10 {
		t.Fatalf("unexpected record after reopen: %#v", rec)
	}
}

func TestCodec_RecordPool_ReusedDecodeLeaksAbsentFields(t *testing.T) {
	db, _ := openTempDBUint64CodecSparseRec(t, Options{AutoBatchMax: 1})

	opt := "sticky"
	if err := db.Set(1, &codecSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &codecSparseRec{Name: "second"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		db.ReleaseRecords(first)

		second, err := db.Get(2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			db.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		db.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
}

func TestCodec_RecordPoolReleaseZeroesCustomCodecRecord(t *testing.T) {
	db, _ := openTempDBUint64CodecSparseRec(t)

	opt := "sticky"
	rec := db.recPool.Get()
	rec.Name = "dirty"
	rec.Tags = []string{"go", "db"}
	rec.Opt = &opt

	db.ReleaseRecords(rec)
	if rec.Name != "" || rec.Tags != nil || rec.Opt != nil {
		t.Fatalf("released custom codec record was not zeroed: %#v", rec)
	}
}

/**/

// guard to deny *posting.List

const postingImportPath = "github.com/vapstack/rbi/internal/posting"

func TestPostingListProductionValueOnlyGuard(t *testing.T) {
	root := testRepoRoot(t)
	fset := token.NewFileSet()
	var failures []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		aliases := postingImportAliases(file)
		inPostingPackage := strings.Contains(path, string(filepath.Separator)+"internal"+string(filepath.Separator)+"posting"+string(filepath.Separator))
		fileFailures := checkPostingListValueOnlyFile(fset, file, aliases, inPostingPackage)
		failures = append(failures, fileFailures...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(failures) > 0 {
		t.Fatalf("posting.List production guard failed:\n%s", strings.Join(failures, "\n"))
	}
}

func testRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func postingImportAliases(file *ast.File) map[string]struct{} {
	aliases := make(map[string]struct{})
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if path != postingImportPath {
			continue
		}
		name := "posting"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		aliases[name] = struct{}{}
	}
	return aliases
}

func checkPostingListValueOnlyFile(
	fset *token.FileSet,
	file *ast.File,
	postingAliases map[string]struct{},
	inPostingPackage bool,
) []string {
	var failures []string

	report := func(pos token.Pos, msg string) {
		failures = append(failures, fset.Position(pos).String()+": "+msg)
	}

	var checkFieldList func(fl *ast.FieldList)
	var checkTypeExpr func(expr ast.Expr)

	checkFieldList = func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, field := range fl.List {
			checkTypeExpr(field.Type)
		}
	}

	checkTypeExpr = func(expr ast.Expr) {
		switch v := expr.(type) {
		case nil:
			return
		case *ast.StarExpr:
			if isPostingListType(v.X, postingAliases, inPostingPackage) {
				report(v.Pos(), "production code must not use *posting.List or *List")
			}
			checkTypeExpr(v.X)
		case *ast.ArrayType:
			checkTypeExpr(v.Elt)
		case *ast.Ellipsis:
			checkTypeExpr(v.Elt)
		case *ast.MapType:
			checkTypeExpr(v.Key)
			checkTypeExpr(v.Value)
		case *ast.ChanType:
			checkTypeExpr(v.Value)
		case *ast.StructType:
			checkFieldList(v.Fields)
		case *ast.InterfaceType:
			checkFieldList(v.Methods)
		case *ast.FuncType:
			checkFieldList(v.Params)
			checkFieldList(v.Results)
		case *ast.ParenExpr:
			checkTypeExpr(v.X)
		case *ast.IndexExpr:
			checkTypeExpr(v.X)
			checkTypeExpr(v.Index)
		case *ast.IndexListExpr:
			checkTypeExpr(v.X)
			for _, idx := range v.Indices {
				checkTypeExpr(idx)
			}
		}
	}

	for _, decl := range file.Decls {
		switch v := decl.(type) {
		case *ast.FuncDecl:
			checkFieldList(v.Recv)
			checkTypeExpr(v.Type)
		case *ast.GenDecl:
			for _, spec := range v.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					checkTypeExpr(s.Type)
				case *ast.ValueSpec:
					checkTypeExpr(s.Type)
				}
			}
		}
	}

	return failures
}

func isPostingListType(expr ast.Expr, postingAliases map[string]struct{}, inPostingPackage bool) bool {
	switch v := expr.(type) {
	case *ast.Ident:
		return inPostingPackage && v.Name == "List"
	case *ast.SelectorExpr:
		pkg, ok := v.X.(*ast.Ident)
		if !ok || v.Sel.Name != "List" {
			return false
		}
		_, ok = postingAliases[pkg.Name]
		return ok
	default:
		return false
	}
}

/**/

type pooledSparseRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
	Opt  *string  `db:"opt"  rbi:"index"`
}

func (r *pooledSparseRec) MarshalMsgpack() ([]byte, error) {
	m := make(map[string]any, 3)
	if r.Name != "" {
		m["name"] = r.Name
	}
	if len(r.Tags) != 0 {
		m["tags"] = r.Tags
	}
	if r.Opt != nil {
		m["opt"] = *r.Opt
	}
	return msgpack.Marshal(m)
}

func (r *pooledSparseRec) UnmarshalMsgpack(b []byte) error {
	// Intentionally update only fields present in the payload. If decode reuses a
	// dirty pooled struct without zeroing it first, absent fields leak through.
	var tmp struct {
		Name *string   `msgpack:"name"`
		Tags *[]string `msgpack:"tags"`
		Opt  *string   `msgpack:"opt"`
	}
	if err := msgpack.Unmarshal(b, &tmp); err != nil {
		return err
	}
	if tmp.Name != nil {
		r.Name = *tmp.Name
	}
	if tmp.Tags != nil {
		r.Tags = append(r.Tags[:0], *tmp.Tags...)
	}
	if tmp.Opt != nil {
		v := *tmp.Opt
		r.Opt = &v
	}
	return nil
}

func openTempDBUint64PooledSparseRec(t *testing.T, options ...Options) (*DB[uint64, pooledSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_pooled_sparse.db")
	db, raw := openBoltAndNew[uint64, pooledSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func TestRecordPool_ReusedDecodeLeaksAbsentFields(t *testing.T) {
	db, _ := openTempDBUint64PooledSparseRec(t, Options{AutoBatchMax: 1})

	opt := "sticky"
	if err := db.Set(1, &pooledSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &pooledSparseRec{Name: "second"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		db.ReleaseRecords(first)

		second, err := db.Get(2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			db.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		db.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
}

func TestRecordPoolReleaseZeroesMsgpackRecord(t *testing.T) {
	db, _ := openTempDBUint64PooledSparseRec(t)

	opt := "sticky"
	rec := db.recPool.Get()
	rec.Name = "dirty"
	rec.Tags = []string{"go", "db"}
	rec.Opt = &opt

	db.ReleaseRecords(rec)
	if rec.Name != "" || rec.Tags != nil || rec.Opt != nil {
		t.Fatalf("released msgpack record was not zeroed: %#v", rec)
	}
}
