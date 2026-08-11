package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestHandlerHandle(t *testing.T) {
	t.Run("writes record through to the underlying writer", func(t *testing.T) {
		var buf bytes.Buffer

		h := NewHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "test message", 0)
		record.AddAttrs(slog.String("key", "value"))

		err := h.Handle(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := buf.String()
		if !strings.Contains(out, "test message") {
			t.Fatalf("expected output to contain message, got %q", out)
		}

		if !strings.Contains(out, "key=value") {
			t.Fatalf("expected output to contain attrs, got %q", out)
		}
	})
}

func TestHandlerWithAttrs(t *testing.T) {
	t.Run("returns a new Handler preserving the goroutine-id cache", func(t *testing.T) {
		var buf bytes.Buffer

		h := NewHandler(&buf, nil)

		got := h.WithAttrs([]slog.Attr{slog.String("k", "v")})

		newHandler, ok := got.(*Handler)
		if !ok {
			t.Fatalf("expected *Handler, got %T", got)
		}

		if newHandler == h {
			t.Fatal("expected a new Handler instance")
		}

		if newHandler.goidCache == nil {
			t.Fatal("expected goidCache to be preserved")
		}
	})
}

func TestHandlerWithGroup(t *testing.T) {
	t.Run("returns a new Handler preserving the goroutine-id cache", func(t *testing.T) {
		var buf bytes.Buffer

		h := NewHandler(&buf, nil)

		got := h.WithGroup("group")

		newHandler, ok := got.(*Handler)
		if !ok {
			t.Fatalf("expected *Handler, got %T", got)
		}

		if newHandler == h {
			t.Fatal("expected a new Handler instance")
		}

		if newHandler.goidCache == nil {
			t.Fatal("expected goidCache to be preserved")
		}
	})
}

func TestHandlerEnabled(t *testing.T) {
	t.Run("delegates to the underlying slog.Handler", func(t *testing.T) {
		var buf bytes.Buffer

		h := NewHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})

		if h.Enabled(context.Background(), slog.LevelInfo) {
			t.Fatal("expected LevelInfo to be disabled when min level is Warn")
		}

		if !h.Enabled(context.Background(), slog.LevelError) {
			t.Fatal("expected LevelError to be enabled when min level is Warn")
		}
	})
}
