package logging

import (
	"context"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
)

type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())

	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

func sourceBasename(t *testing.T, pc uintptr) string {
	t.Helper()

	frames := runtime.CallersFrames([]uintptr{pc})
	frame, _ := frames.Next()

	return filepath.Base(frame.File)
}

func TestAdapterLogAttrsDirectCallerResolvesSourceToCallSite(t *testing.T) {
	handler := &captureHandler{}
	a := NewSlogAdapter(slog.New(handler))

	a.LogAttrs(context.Background(), slog.LevelWarn, "direct call")

	if len(handler.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(handler.records))
	}

	file := sourceBasename(t, handler.records[0].PC)

	if file != "adapter_test.go" {
		t.Fatalf("expected source file adapter_test.go, got %s", file)
	}
}

type fakeAttrLogger struct {
	calls int

	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (f *fakeAttrLogger) LogAttrs(_ context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	f.calls++
	f.level = level
	f.msg = msg
	f.attrs = attrs
}

func (f *fakeAttrLogger) Debugf(format string, args ...any) {}
func (f *fakeAttrLogger) Infof(format string, args ...any)  {}
func (f *fakeAttrLogger) Warnf(format string, args ...any)  {}
func (f *fakeAttrLogger) Errorf(format string, args ...any) {}

func TestNewAdapter(t *testing.T) {
	t.Run("self-wrap returns the same instance", func(t *testing.T) {
		original := NewSlogAdapter(slog.New(&captureHandler{}))

		wrapped := NewAdapter(original)

		if wrapped != original {
			t.Fatal("expected NewAdapter to return the same *Adapter instance when re-wrapping")
		}
	})

	t.Run("generic AttrLogger-capable host forwards LogRecord calls", func(t *testing.T) {
		fl := &fakeAttrLogger{}

		a := NewAdapter(fl)

		a.LogRecord(context.Background(), slog.LevelInfo, "hello", 0, []slog.Attr{slog.String("k", "v")})

		if fl.calls != 1 {
			t.Fatalf("expected 1 call, got %d", fl.calls)
		}

		if fl.msg != "hello" {
			t.Fatalf("expected msg %q, got %q", "hello", fl.msg)
		}
	})
}

type fakePrintfLogger struct {
	calls []string
}

func (f *fakePrintfLogger) Debugf(format string, args ...any) { f.calls = append(f.calls, "debug") }
func (f *fakePrintfLogger) Infof(format string, args ...any)  { f.calls = append(f.calls, "info") }
func (f *fakePrintfLogger) Warnf(format string, args ...any)  { f.calls = append(f.calls, "warn") }
func (f *fakePrintfLogger) Errorf(format string, args ...any) { f.calls = append(f.calls, "error") }

func TestAdapterLogRecordFallsBackToPrintfWhenNoAttrLogger(t *testing.T) {
	fl := &fakePrintfLogger{}

	a := NewAdapter(fl)

	a.LogRecord(context.Background(), slog.LevelWarn, "fallback msg", 0, nil)

	if len(fl.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fl.calls))
	}

	if fl.calls[0] != "warn" {
		t.Fatalf("expected warn, got %s", fl.calls[0])
	}
}

func TestAdapterPrintfMethodsDelegateToSlogLogger(t *testing.T) {
	handler := &captureHandler{}
	a := NewSlogAdapter(slog.New(handler))

	a.Debugf("d")
	a.Infof("i")
	a.Warnf("w")
	a.Errorf("e")

	if len(handler.records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(handler.records))
	}
}
