package redismq

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

type sourceLine struct {
	file string
	line int
	text string
}

var generatedHeaderRe = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.$`)

func moduleRoot(t *testing.T) string {
	t.Helper()

	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to determine caller for module root lookup")
	}

	dir := filepath.Dir(self)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found while walking up from guard_test.go")
		}

		dir = parent
	}
}

func isGeneratedFile(t *testing.T, path string) bool {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}

	for _, raw := range strings.Split(string(content), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}

		return generatedHeaderRe.MatchString(line)
	}

	return false
}

func isLoggerFallbackFile(file string) bool {
	return filepath.Base(file) == "logger.go"
}

func listGoSourceFiles(t *testing.T) []string {
	t.Helper()

	root := moduleRoot(t)

	var files []string

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			switch entry.Name() {
			case ".git":
				return filepath.SkipDir
			}

			return nil
		}

		name := entry.Name()

		if !strings.HasSuffix(name, ".go") {
			return nil
		}

		if strings.HasSuffix(name, "_test.go") {
			return nil
		}

		if isGeneratedFile(t, path) {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		files = append(files, filepath.ToSlash(rel))

		return nil
	})
	if err != nil {
		t.Fatalf("walk dir: %v", err)
	}

	return files
}

func readSourceLines(t *testing.T, file string) []sourceLine {
	t.Helper()

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read file %s: %v", file, err)
	}

	var lines []sourceLine

	for i, raw := range strings.Split(string(content), "\n") {
		stripped := raw
		if idx := strings.Index(stripped, "//"); idx >= 0 {
			stripped = stripped[:idx]
		}

		lines = append(lines, sourceLine{file: file, line: i + 1, text: stripped})
	}

	return lines
}

func TestNoPrintfLogging(t *testing.T) {
	forbidden := []string{"logger.Debugf(", "logger.Infof(", "logger.Warnf(", "logger.Errorf("}

	for _, file := range listGoSourceFiles(t) {
		if isLoggerFallbackFile(file) {
			continue
		}

		t.Run(file, func(t *testing.T) {
			for _, sl := range readSourceLines(t, file) {
				for _, f := range forbidden {
					if !strings.Contains(sl.text, f) {
						continue
					}

					t.Errorf("%s:%d: %s (use logAttrs(ctx, level, staticMessage, attrs...) instead of printf-style logging)",
						sl.file, sl.line, strings.TrimSpace(sl.text))
				}
			}
		})
	}
}

func TestNoContextBackground(t *testing.T) {
	for _, file := range listGoSourceFiles(t) {
		t.Run(file, func(t *testing.T) {
			for _, sl := range readSourceLines(t, file) {
				if !strings.Contains(sl.text, "context.Background()") {
					continue
				}

				t.Errorf("%s:%d: %s (thread the host-supplied root context instead of using context.Background())",
					sl.file, sl.line, strings.TrimSpace(sl.text))
			}
		})
	}
}

func TestNoInitFunc(t *testing.T) {
	for _, file := range listGoSourceFiles(t) {
		t.Run(file, func(t *testing.T) {
			for _, sl := range readSourceLines(t, file) {
				if !strings.HasPrefix(strings.TrimSpace(sl.text), "func init()") {
					continue
				}

				t.Errorf("%s:%d: %s (initialization must be explicit via the host's setup call sequence, not init())",
					sl.file, sl.line, strings.TrimSpace(sl.text))
			}
		})
	}
}

func TestNoPanic(t *testing.T) {
	for _, file := range listGoSourceFiles(t) {
		t.Run(file, func(t *testing.T) {
			for _, sl := range readSourceLines(t, file) {
				if !strings.Contains(sl.text, "panic(") {
					continue
				}

				t.Errorf("%s:%d: %s (return an error instead of panicking)",
					sl.file, sl.line, strings.TrimSpace(sl.text))
			}
		})
	}
}
