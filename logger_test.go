package go_redismq

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())

	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func TestLogAttrs(t *testing.T) {
	t.Run("attrs land as fields", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		handler := &captureHandler{}
		SetSlogLogger(slog.New(handler))

		logAttrs(context.Background(), slog.LevelWarn, "redismq: test",
			slog.String("topic", "t1"), slog.Int("reconsume_times", 2))

		if len(handler.records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(handler.records))
		}

		record := handler.records[0]

		if record.Message != "redismq: test" {
			t.Fatalf("expected message %q, got %q", "redismq: test", record.Message)
		}

		if record.Level != slog.LevelWarn {
			t.Fatalf("expected level %v, got %v", slog.LevelWarn, record.Level)
		}

		var gotTopic, gotReconsume bool

		record.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "topic":
				gotTopic = a.Value.String() == "t1"
			case "reconsume_times":
				gotReconsume = a.Value.Int64() == 2
			}

			return true
		})

		if !gotTopic {
			t.Fatal("expected topic=t1 attr")
		}

		if !gotReconsume {
			t.Fatal("expected reconsume_times=2 attr")
		}
	})

	t.Run("source is call site not logger.go", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		handler := &captureHandler{}
		SetSlogLogger(slog.New(handler))

		logAttrs(context.Background(), slog.LevelWarn, "redismq: source test")

		if len(handler.records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(handler.records))
		}

		record := handler.records[0]

		frames := runtime.CallersFrames([]uintptr{record.PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		if file == "logger.go" {
			t.Fatalf("record source resolved to logger.go instead of the real call site; check callerSkip in logger.go (currently %d)", callerSkip)
		}

		if file != "logger_test.go" {
			t.Fatalf("expected source file logger_test.go, got %s", file)
		}
	})

	t.Run("nil logger is a silent no-op", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		logger = nil

		logAttrs(context.Background(), slog.LevelWarn, "redismq: nil logger test")
	})
}

type fakeCall struct {
	method string
	format string
	args   []any
}

type fakeLogger struct {
	calls []fakeCall
}

func (f *fakeLogger) Debugf(format string, args ...any) {
	f.calls = append(f.calls, fakeCall{method: "Debugf", format: format, args: args})
}

func (f *fakeLogger) Infof(format string, args ...any) {
	f.calls = append(f.calls, fakeCall{method: "Infof", format: format, args: args})
}

func (f *fakeLogger) Warnf(format string, args ...any) {
	f.calls = append(f.calls, fakeCall{method: "Warnf", format: format, args: args})
}

func (f *fakeLogger) Errorf(format string, args ...any) {
	f.calls = append(f.calls, fakeCall{method: "Errorf", format: format, args: args})
}

func assertFallbackCall(t *testing.T, fl *fakeLogger, wantMethod string) {
	t.Helper()

	if len(fl.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fl.calls))
	}

	c := fl.calls[0]

	if c.method != wantMethod {
		t.Fatalf("expected %s to be called, got %s", wantMethod, c.method)
	}

	if len(c.args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(c.args))
	}

	line, ok := c.args[0].(string)
	if !ok {
		t.Fatalf("expected string arg, got %T", c.args[0])
	}

	if !strings.Contains(line, "redismq: msg") {
		t.Fatalf("expected rendered line to contain %q, got %q", "redismq: msg", line)
	}

	if !strings.Contains(line, "k=v") {
		t.Fatalf("expected rendered line to contain %q, got %q", "k=v", line)
	}
}

func TestFallbackPrintf(t *testing.T) {
	t.Run("debug level uses Debugf", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		fl := &fakeLogger{}
		SetLogger(fl)

		logAttrs(context.Background(), slog.LevelDebug, "redismq: msg", slog.String("k", "v"))

		assertFallbackCall(t, fl, "Debugf")
	})

	t.Run("info level uses Infof", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		fl := &fakeLogger{}
		SetLogger(fl)

		logAttrs(context.Background(), slog.LevelInfo, "redismq: msg", slog.String("k", "v"))

		assertFallbackCall(t, fl, "Infof")
	})

	t.Run("warn level uses Warnf", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		fl := &fakeLogger{}
		SetLogger(fl)

		logAttrs(context.Background(), slog.LevelWarn, "redismq: msg", slog.String("k", "v"))

		assertFallbackCall(t, fl, "Warnf")
	})

	t.Run("error level uses Errorf", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		fl := &fakeLogger{}
		SetLogger(fl)

		logAttrs(context.Background(), slog.LevelError, "redismq: msg", slog.String("k", "v"))

		assertFallbackCall(t, fl, "Errorf")
	})

	t.Run("default level falls back to Errorf", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		fl := &fakeLogger{}
		SetLogger(fl)

		logAttrs(context.Background(), slog.Level(99), "redismq: msg", slog.String("k", "v"))

		assertFallbackCall(t, fl, "Errorf")
	})
}

func TestCauseAttr(t *testing.T) {
	attr := causeAttr(errors.New("boom"))

	if attr.Key != "cause" {
		t.Fatalf("expected key %q, got %q", "cause", attr.Key)
	}

	if attr.Key == "error" {
		t.Fatalf("cause attr key must not be %q, got %q", "error", attr.Key)
	}
}

func TestMessageAttrs(t *testing.T) {
	t.Run("nil message returns nil", func(t *testing.T) {
		if messageAttrs(nil) != nil {
			t.Fatal("expected nil attrs for nil message")
		}
	})

	t.Run("populated message yields expected attrs", func(t *testing.T) {
		m := &Message{
			Topic:          "t",
			Tag:            "tag1",
			MessageId:      "id1",
			ReconsumeTimes: 1,
			ReconsumeMax:   3,
		}

		attrs := messageAttrs(m)

		if len(attrs) != 5 {
			t.Fatalf("expected 5 attrs, got %d", len(attrs))
		}

		if attrs[0].Key != "topic" {
			t.Fatalf("expected attrs[0].Key %q, got %q", "topic", attrs[0].Key)
		}

		if attrs[1].Key != "tag" {
			t.Fatalf("expected attrs[1].Key %q, got %q", "tag", attrs[1].Key)
		}

		if attrs[2].Key != "message_id" {
			t.Fatalf("expected attrs[2].Key %q, got %q", "message_id", attrs[2].Key)
		}

		if attrs[3].Key != "reconsume_times" {
			t.Fatalf("expected attrs[3].Key %q, got %q", "reconsume_times", attrs[3].Key)
		}

		if attrs[4].Key != "reconsume_max" {
			t.Fatalf("expected attrs[4].Key %q, got %q", "reconsume_max", attrs[4].Key)
		}
	})

	t.Run("message_key present when Key set", func(t *testing.T) {
		m := &Message{
			Topic:          "t",
			Tag:            "tag1",
			MessageId:      "id1",
			ReconsumeTimes: 1,
			ReconsumeMax:   3,
			Key:            "mykey",
		}

		attrs := messageAttrs(m)

		last := attrs[len(attrs)-1]

		if last.Key != "message_key" {
			t.Fatalf("expected last attr key %q, got %q", "message_key", last.Key)
		}

		if last.Value.String() != "mykey" {
			t.Fatalf("expected last attr value %q, got %q", "mykey", last.Value.String())
		}
	})
}

func TestStackAttr(t *testing.T) {
	attr := stackAttr(2)

	if attr.Key != "stack" {
		t.Fatalf("expected key %q, got %q", "stack", attr.Key)
	}

	stack, ok := attr.Value.Any().([]string)
	if !ok {
		t.Fatalf("expected []string stack value, got %T", attr.Value.Any())
	}

	if len(stack) == 0 {
		t.Fatal("expected non-empty stack")
	}

	if !strings.Contains(stack[0], "TestStackAttr") {
		t.Fatalf("expected first frame to contain %q, got %q", "TestStackAttr", stack[0])
	}
}
