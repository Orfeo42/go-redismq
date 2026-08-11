package redismq

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type traceCtxKey struct{}

func TestStampTraceIDRoundTrip(t *testing.T) {
	t.Run("survives toStreamAddArgsValues to passStreamMessage round trip", func(t *testing.T) {
		original := getTraceIDFromContext()

		t.Cleanup(func() { SetTraceIDFromContext(original) })

		SetTraceIDFromContext(func(_ context.Context) string { return "trace-abc" })

		message := &Message{Topic: "t", Tag: "tag1", Body: "body"}
		stampTraceID(context.Background(), message)

		args, err := message.toStreamAddArgsValues("stream")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		values, ok := args.Values.(map[string]any)
		if !ok {
			t.Fatalf("expected args.Values to be map[string]any, got %T", args.Values)
		}

		fresh := &Message{}
		fresh.passStreamMessage(context.Background(), values)

		if fresh.traceID() != "trace-abc" {
			t.Fatalf("expected trace id %q, got %q", "trace-abc", fresh.traceID())
		}
	})
}

func TestStampTraceID(t *testing.T) {
	t.Run("does not overwrite existing trace id", func(t *testing.T) {
		original := getTraceIDFromContext()

		t.Cleanup(func() { SetTraceIDFromContext(original) })

		SetTraceIDFromContext(func(_ context.Context) string { return "new-id" })

		message := &Message{}
		message.setTraceID("original-id")

		stampTraceID(context.Background(), message)

		if message.traceID() != "original-id" {
			t.Fatalf("expected trace id to stay %q, got %q", "original-id", message.traceID())
		}
	})

	t.Run("no-op when no hook registered", func(t *testing.T) {
		original := getTraceIDFromContext()

		t.Cleanup(func() { SetTraceIDFromContext(original) })

		SetTraceIDFromContext(func(_ context.Context) string { return "" })

		message := &Message{}
		stampTraceID(context.Background(), message)

		if message.traceID() != "" {
			t.Fatalf("expected empty trace id, got %q", message.traceID())
		}

		if _, ok := message.CustomData[traceIDKey]; ok {
			t.Fatal("expected CustomData to not contain trace id key")
		}
	})
}

func TestConsumeContext(t *testing.T) {
	t.Run("generates and stamps trace id when absent", func(t *testing.T) {
		message := &Message{}

		consumeContext(context.Background(), message)

		traceID := message.traceID()
		if traceID == "" {
			t.Fatal("expected non-empty trace id")
		}

		if _, err := uuid.Parse(traceID); err != nil {
			t.Fatalf("expected parseable uuid, got %q: %v", traceID, err)
		}
	})

	t.Run("preserves existing trace id", func(t *testing.T) {
		message := &Message{}
		message.setTraceID("existing-id")

		consumeContext(context.Background(), message)

		if message.traceID() != "existing-id" {
			t.Fatalf("expected trace id to stay %q, got %q", "existing-id", message.traceID())
		}
	})

	t.Run("invokes registered TraceIDToContext", func(t *testing.T) {
		original := getTraceIDToContext()

		t.Cleanup(func() { SetTraceIDToContext(original) })

		SetTraceIDToContext(func(ctx context.Context, traceID string) context.Context {
			return context.WithValue(ctx, traceCtxKey{}, traceID)
		})

		message := &Message{}
		message.setTraceID("ctx-id")

		ctx := consumeContext(context.Background(), message)

		got, ok := ctx.Value(traceCtxKey{}).(string)
		if !ok || got != "ctx-id" {
			t.Fatalf("expected ctx value %q, got %q (ok=%v)", "ctx-id", got, ok)
		}
	})
}

func TestMessageAttrsTraceID(t *testing.T) {
	t.Run("includes trace_id when present", func(t *testing.T) {
		message := &Message{Topic: "t", Tag: "tag1"}
		message.setTraceID("trace-1")

		attrs := messageAttrs(message)

		found := false

		for _, a := range attrs {
			if a.Key != "trace_id" {
				continue
			}

			found = true

			if a.Value.String() != "trace-1" {
				t.Fatalf("expected trace_id value %q, got %q", "trace-1", a.Value.String())
			}
		}

		if !found {
			t.Fatal("expected trace_id attr present")
		}
	})

	t.Run("omits trace_id when absent", func(t *testing.T) {
		message := &Message{Topic: "t", Tag: "tag1"}

		attrs := messageAttrs(message)

		for _, a := range attrs {
			if a.Key == "trace_id" {
				t.Fatal("expected no trace_id attr")
			}
		}
	})
}

func TestSendWithoutHookLeavesMessageUnstamped(t *testing.T) {
	t.Run("stampTraceID no-ops without a registered hook", func(t *testing.T) {
		original := getTraceIDFromContext()

		t.Cleanup(func() { SetTraceIDFromContext(original) })

		SetTraceIDFromContext(func(_ context.Context) string { return "" })

		message := &Message{Topic: "t", Tag: "tag1"}
		stampTraceID(context.Background(), message)

		if message.traceID() != "" {
			t.Fatalf("expected message to remain unstamped, got %q", message.traceID())
		}
	})
}
