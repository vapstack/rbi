package rbi

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	schemapkg "github.com/vapstack/rbi/internal/schema"
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

type codecSparseNameOnlyRec struct {
	Name string `db:"name"`
}

type codecNamedTime time.Time

type codecNamedTimeRec struct {
	When codecNamedTime `db:"when" rbi:"index"`
}

func openTempUint64CollectionCodecRec(t *testing.T, options ...Options) (*Collection[uint64, codecRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec.db")
	c, bolt := openBoltAndCollection[uint64, codecRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}

func openTempCollectionUint64CodecSparseRec(t *testing.T, options ...Options) (*Collection[uint64, codecSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_codec_sparse.db")
	c, bolt := openBoltAndCollection[uint64, codecSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}

func codecTestPayload[V any](t *testing.T, rec *V) []byte {
	t.Helper()
	rt, err := schemapkg.Compile(reflect.TypeFor[V](), schemapkg.Config{})
	if err != nil {
		t.Fatalf("Compile(%T): %v", *rec, err)
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(rec), &buf)
	return slices.Clone(buf.Bytes())
}

func TestCodec_SetOnChangeSnapshotUsesCompiledRuntime(t *testing.T) {
	c, _ := openTempUint64CollectionCodecRec(t)

	if err := writeSet(c, 1, &codecRec{Name: "alice", Age: 10}, OnChange(func(_ *Tx, _ uint64, oldValue, newValue *codecRec) error {
		if oldValue != nil {
			t.Fatalf("unexpected old value: %#v", oldValue)
		}
		newValue.Age = 11
		return nil
	})); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer c.ReleaseRecords(got)

	if got.Name != "alice" || got.Age != 11 {
		t.Fatalf("unexpected record: %#v", got)
	}

	if err = c.root.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		var key [8]byte
		payload := b.Get(keycodec.U64BytesWithBuf(1, &key))
		if len(payload) == 0 {
			return fmt.Errorf("stored payload missing")
		}
		if payload[0] != 1 {
			return fmt.Errorf("expected compiled codec version byte, got payload %x", payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("Bolt().View: %v", err)
	}
}

func TestCodec_RejectsNamedTimeIndexField(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, codecNamedTimeRec](raw, testOptions(Options{}))
	if err == nil {
		_ = c.Close()
		t.Fatal("New accepted named time index field")
	}
	if msg := err.Error(); !strings.Contains(msg, "named time field") || !strings.Contains(msg, "is not supported") {
		t.Fatalf("New named time index field err=%v", err)
	}
}

func TestCodec_BuildIndexUsesCompiledRuntimeDecode(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_bucket"
	payload := codecTestPayload(t, &codecRec{Name: "alice", Age: 10})

	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), payload)
	}); err != nil {
		t.Fatalf("seed payload: %v", err)
	}

	c, err := Open[uint64, codecRec](raw, testOptions(Options{BucketName: bucket}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	got, err := readQueryKeys(c, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected query result after reopen: %v", got)
	}

	rec, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if rec == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer c.ReleaseRecords(rec)

	if rec.Name != "alice" || rec.Age != 10 {
		t.Fatalf("unexpected record after reopen: %#v", rec)
	}
}

func TestCodec_BuildIndexRejectsOldPayloadFormat(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_old_payload"
	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), []byte("RBI1-old"))
	}); err != nil {
		t.Fatalf("seed old payload: %v", err)
	}

	c, err := Open[uint64, codecRec](raw, testOptions(Options{BucketName: bucket}))
	if err == nil {
		_ = c.Close()
		t.Fatal("Open accepted old payload format")
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("Open err=%v want unsupported version", err)
	}
}

func TestCodec_RecordPoolReusedDecodeZerosMissingFields(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	const bucket = "codec_sparse"
	opt := "sticky"
	first := codecTestPayload(t, &codecSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	})
	second := codecTestPayload(t, &codecSparseNameOnlyRec{Name: "second"})

	if err := raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var key [8]byte
		if err = b.Put(keycodec.U64BytesWithBuf(1, &key), first); err != nil {
			return err
		}
		return b.Put(keycodec.U64BytesWithBuf(2, &key), second)
	}); err != nil {
		t.Fatalf("seed sparse payloads: %v", err)
	}

	c, err := Open[uint64, codecSparseRec](raw, testOptions(Options{BucketName: bucket, BatchSoftLimit: 1}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = c.Close() }()

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := readGet(c, 1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		c.ReleaseRecords(first)

		second, err := readGet(c, 2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			c.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		c.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
}

func TestCodec_RecordPoolReleaseZeroesRecord(t *testing.T) {
	c, _ := openTempCollectionUint64CodecSparseRec(t)

	opt := "sticky"
	rec := c.recPool.Get()
	rec.Name = "dirty"
	rec.Tags = []string{"go", "db"}
	rec.Opt = &opt

	c.ReleaseRecords(rec)
	if rec.Name != "" || rec.Tags != nil || rec.Opt != nil {
		t.Fatalf("released record was not zeroed: %#v", rec)
	}
}

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
