package redismq

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type sourceLine struct {
	file string
	line int
	text string
}

func listGoSourceFiles(t *testing.T) []string {
	t.Helper()

	var files []string

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			if entry.Name() == ".git" {
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

		files = append(files, filepath.ToSlash(path))

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
		if file == "logger.go" {
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

func TestAttrHelpersExist(t *testing.T) {
	t.Run("messageAttrs", func(t *testing.T) {
		m := &Message{Topic: "t", Tag: "tag1", MessageId: "id1", ReconsumeTimes: 1, ReconsumeMax: 3}

		attrs := messageAttrs(m)
		if len(attrs) == 0 {
			t.Fatal("expected non-empty attrs")
		}
	})

	t.Run("topicAttrs", func(t *testing.T) {
		attrs := topicAttrs("t1")
		if len(attrs) != 3 {
			t.Fatalf("expected 3 attrs, got %d", len(attrs))
		}
	})

	t.Run("causeAttr", func(t *testing.T) {
		attr := causeAttr(context.Canceled)
		if attr.Key != "cause" {
			t.Fatalf("expected key %q, got %q", "cause", attr.Key)
		}
	})

	t.Run("stackAttr", func(t *testing.T) {
		attr := stackAttr(1)
		if attr.Key != "stack" {
			t.Fatalf("expected key %q, got %q", "stack", attr.Key)
		}

		if _, ok := attr.Value.Any().([]string); !ok {
			t.Fatalf("expected []string stack value, got %T", attr.Value.Any())
		}
	})
}
