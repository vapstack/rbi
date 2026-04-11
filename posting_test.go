package rbi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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
