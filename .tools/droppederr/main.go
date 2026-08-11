// droppederr reports error-handling sites where the root cause never reaches the logs.
//
// Two classes are reported:
//
//	DROPPED  an `if err != nil { ... }` block whose body never references err,
//	         so the root cause is discarded before any boundary can log it.
//	COLLIDE  a logAttrs call carrying a slog attr keyed "error" or "err": this
//	         library's convention is that the root cause is always keyed
//	         "cause" (see causeAttr in logattr.go) — a colliding key means a
//	         second attr can shadow the cause and silently swallow it in the
//	         emitted JSON, since duplicate keys collapse to the last value.
//
// Only uses the Go standard library — no external dependencies.
//
// Every finding is always reported; the exit status is 1 while any remain.
//
// Usage (run from the repo root):
//
//	go run ./.tools/droppederr/main.go ./...
//	go run ./.tools/droppederr/main.go ./internal/...
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	classDropped = "DROPPED"
	classCollide = "COLLIDE"
)

var collidingAttrKeys = map[string]bool{
	`"error"`: true,
	`"err"`:   true,
}

var skipDirs = map[string]bool{
	"vendor":       true,
	".git":         true,
	".tools":       true,
	"node_modules": true,
}

type finding struct {
	class    string
	file     string
	line     int
	function string
	detail   string
}

func main() {
	flag.Parse()

	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	files, err := collectGoFiles(patterns)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var findings []finding

	for _, path := range files {
		fs, err := scanFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v\n", path, err)

			continue
		}

		findings = append(findings, fs...)
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].file != findings[j].file {
			return findings[i].file < findings[j].file
		}

		return findings[i].line < findings[j].line
	})

	os.Exit(report(findings))
}

func report(findings []finding) int {
	counts := map[string]int{}

	for _, f := range findings {
		counts[f.class]++

		fmt.Printf("%s:%d: %s: %s drops the root error: %s\n", f.file, f.line, f.class, f.function, f.detail)
	}

	fmt.Printf("\ndroppederr: %d finding(s) — %d %s, %d %s\n",
		len(findings),
		counts[classDropped], classDropped,
		counts[classCollide], classCollide,
	)

	if len(findings) == 0 {
		return 0
	}

	return 1
}

func scanFile(path string) ([]finding, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}

	var findings []finding

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		findings = append(findings, scanFunc(fset, path, fn)...)
	}

	return findings, nil
}

func scanFunc(fset *token.FileSet, path string, fn *ast.FuncDecl) []finding {
	var findings []finding

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if f, found := collideFinding(fset, path, fn.Name.Name, call); found {
				findings = append(findings, f)
			}

			return true
		}

		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}

		f, found := droppedFinding(fset, path, fn.Name.Name, ifStmt)
		if found {
			findings = append(findings, f)
		}

		return true
	})

	return findings
}

func droppedFinding(fset *token.FileSet, path, function string, ifStmt *ast.IfStmt) (finding, bool) {
	errVar, ok := errNilCheck(ifStmt.Cond)
	if !ok {
		return finding{}, false
	}

	if identUsed(ifStmt.Body, errVar) {
		return finding{}, false
	}

	body := render(fset, ifStmt.Body)
	pos := fset.Position(ifStmt.Pos())

	return finding{
		class:    classDropped,
		file:     path,
		line:     pos.Line,
		function: function,
		detail:   fmt.Sprintf("%s unused in %s", errVar, firstLine(body)),
	}, true
}

// collideFinding walks the whole logAttrs call subtree — not just its direct
// arguments — because attrs are routinely assembled with append(...) (e.g.
// append(messageAttrs(message), causeAttr(err), slog.Any("error", err))...),
// which puts the colliding attr one level below the call's own argument list.
func collideFinding(fset *token.FileSet, path, function string, call *ast.CallExpr) (finding, bool) {
	if !isLogAttrsCall(call.Fun) {
		return finding{}, false
	}

	var (
		key   string
		found bool
	)

	ast.Inspect(call, func(n ast.Node) bool {
		if found {
			return false
		}

		attr, ok := n.(*ast.CallExpr)
		if !ok || attr == call || !isSlogAttr(attr.Fun) || len(attr.Args) == 0 {
			return true
		}

		lit, ok := attr.Args[0].(*ast.BasicLit)
		if !ok || !collidingAttrKeys[lit.Value] {
			return true
		}

		key = lit.Value
		found = true

		return false
	})

	if !found {
		return finding{}, false
	}

	pos := fset.Position(call.Pos())

	return finding{
		class:    classCollide,
		file:     path,
		line:     pos.Line,
		function: function,
		detail:   fmt.Sprintf("attr key %s collides with causeAttr's \"cause\" key, use \"cause\"", key),
	}, true
}

func errNilCheck(cond ast.Expr) (string, bool) {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok || bin.Op != token.NEQ {
		return "", false
	}

	nilIdent, ok := bin.Y.(*ast.Ident)
	if !ok || nilIdent.Name != "nil" {
		return "", false
	}

	errIdent, ok := bin.X.(*ast.Ident)
	if !ok || !strings.Contains(strings.ToLower(errIdent.Name), "err") {
		return "", false
	}

	return errIdent.Name, true
}

func identUsed(node ast.Node, name string) bool {
	used := false

	ast.Inspect(node, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok || ident.Name != name {
			return true
		}

		used = true

		return false
	})

	return used
}

func isLogAttrsCall(fun ast.Expr) bool {
	ident, ok := fun.(*ast.Ident)

	return ok && ident.Name == "logAttrs"
}

func isSlogAttr(fun ast.Expr) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, ok := sel.X.(*ast.Ident)

	return ok && pkg.Name == "slog"
}

func render(fset *token.FileSet, node ast.Node) string {
	var sb strings.Builder

	if err := printer.Fprint(&sb, fset, node); err != nil {
		return ""
	}

	return sb.String()
}

func firstLine(s string) string {
	for raw := range strings.SplitSeq(s, "\n") {
		line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "{"))
		if line == "" || line == "}" {
			continue
		}

		if len(line) > 80 {
			line = line[:80] + "..."
		}

		return line
	}

	return "<empty block>"
}

func collectGoFiles(patterns []string) ([]string, error) {
	var files []string

	seen := map[string]bool{}

	add := func(path string) {
		if seen[path] || !includePath(path) {
			return
		}

		seen[path] = true

		files = append(files, path)
	}

	for _, pattern := range patterns {
		dir := strings.TrimSuffix(strings.TrimSuffix(pattern, "/..."), "...")
		if dir == "" {
			dir = "."
		}

		info, err := os.Stat(dir)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", dir, err)
		}

		if !info.IsDir() {
			add(dir)

			continue
		}

		if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}

				return nil
			}

			add(path)

			return nil
		}); err != nil {
			return nil, fmt.Errorf("walk %s: %w", dir, err)
		}
	}

	sort.Strings(files)

	return files, nil
}

func includePath(path string) bool {
	if !strings.HasSuffix(path, ".go") {
		return false
	}

	if strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, "_mock.go") {
		return false
	}

	return true
}
