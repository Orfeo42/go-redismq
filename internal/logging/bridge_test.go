package logging

import (
	"context"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBridgeLogAttrsResolvesSourceToCallSite(t *testing.T) {
	t.Run("direct call site resolves to this test file", func(t *testing.T) {
		handler := &captureHandler{}
		current := NewSlogAdapter(slog.New(handler))

		bridge := NewBridge(func() Logger { return current })

		bridge.LogAttrs(context.Background(), slog.LevelWarn, "bridged call")

		if len(handler.records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(handler.records))
		}

		frames := runtime.CallersFrames([]uintptr{handler.records[0].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		if file != "bridge_test.go" {
			t.Fatalf("expected source file bridge_test.go, got %s", file)
		}
	})
}

func TestBridgeReResolvesLoggerOnEachCall(t *testing.T) {
	t.Run("logger swapped between calls is picked up live", func(t *testing.T) {
		firstHandler := &captureHandler{}
		secondHandler := &captureHandler{}

		var current Logger = NewSlogAdapter(slog.New(firstHandler))

		bridge := NewBridge(func() Logger { return current })

		bridge.LogAttrs(context.Background(), slog.LevelInfo, "first")

		current = NewSlogAdapter(slog.New(secondHandler))

		bridge.LogAttrs(context.Background(), slog.LevelInfo, "second")

		if len(firstHandler.records) != 1 {
			t.Fatalf("expected 1 record on first handler, got %d", len(firstHandler.records))
		}

		if len(secondHandler.records) != 1 {
			t.Fatalf("expected 1 record on second handler, got %d", len(secondHandler.records))
		}
	})
}

func TestBridgeNilLoggerIsSilentNoOp(t *testing.T) {
	t.Run("nil resolved logger does not panic", func(t *testing.T) {
		bridge := NewBridge(func() Logger { return nil })

		bridge.LogAttrs(context.Background(), slog.LevelWarn, "should be dropped")
	})
}
