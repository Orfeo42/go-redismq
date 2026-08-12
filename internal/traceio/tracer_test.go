package traceio

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

type traceCtxKey struct{}

func TestStampTraceID(t *testing.T) {
	t.Run("stamps trace id from hook when absent", func(t *testing.T) {
		tr := New()
		tr.SetTraceIDFromContext(func(_ context.Context) string { return "trace-abc" })

		message := &mqtype.Message{}
		tr.StampTraceID(context.Background(), message)

		if message.TraceID() != "trace-abc" {
			t.Fatalf("expected trace id %q, got %q", "trace-abc", message.TraceID())
		}
	})

	t.Run("does not overwrite existing trace id", func(t *testing.T) {
		tr := New()
		tr.SetTraceIDFromContext(func(_ context.Context) string { return "new-id" })

		message := &mqtype.Message{}
		message.SetTraceID("original-id")

		tr.StampTraceID(context.Background(), message)

		if message.TraceID() != "original-id" {
			t.Fatalf("expected trace id to stay %q, got %q", "original-id", message.TraceID())
		}
	})

	t.Run("no-op when hook returns empty", func(t *testing.T) {
		tr := New()

		message := &mqtype.Message{}
		tr.StampTraceID(context.Background(), message)

		if message.TraceID() != "" {
			t.Fatalf("expected empty trace id, got %q", message.TraceID())
		}
	})
}

func TestConsumeContext(t *testing.T) {
	t.Run("generates and stamps trace id when absent", func(t *testing.T) {
		tr := New()

		message := &mqtype.Message{}
		tr.ConsumeContext(context.Background(), message)

		traceID := message.TraceID()
		if traceID == "" {
			t.Fatal("expected non-empty trace id")
		}

		if _, err := uuid.Parse(traceID); err != nil {
			t.Fatalf("expected parseable uuid, got %q: %v", traceID, err)
		}
	})

	t.Run("preserves existing trace id", func(t *testing.T) {
		tr := New()

		message := &mqtype.Message{}
		message.SetTraceID("existing-id")

		tr.ConsumeContext(context.Background(), message)

		if message.TraceID() != "existing-id" {
			t.Fatalf("expected trace id to stay %q, got %q", "existing-id", message.TraceID())
		}
	})

	t.Run("invokes registered TraceIDToContext hook", func(t *testing.T) {
		tr := New()
		tr.SetTraceIDToContext(func(ctx context.Context, traceID string) context.Context {
			return context.WithValue(ctx, traceCtxKey{}, traceID)
		})

		message := &mqtype.Message{}
		message.SetTraceID("ctx-id")

		ctx := tr.ConsumeContext(context.Background(), message)

		got, ok := ctx.Value(traceCtxKey{}).(string)
		if !ok || got != "ctx-id" {
			t.Fatalf("expected ctx value %q, got %q (ok=%v)", "ctx-id", got, ok)
		}
	})
}

func TestSetTraceIDFromContext(t *testing.T) {
	t.Run("ignores nil function", func(t *testing.T) {
		tr := New()

		tr.SetTraceIDFromContext(nil)

		if got := tr.TraceIDFromContext()(context.Background()); got != "" {
			t.Fatalf("expected default hook to return empty string, got %q", got)
		}
	})
}

func TestSetTraceIDToContext(t *testing.T) {
	t.Run("ignores nil function", func(t *testing.T) {
		tr := New()

		tr.SetTraceIDToContext(nil)

		ctx := context.Background()

		got := tr.TraceIDToContext()(ctx, "id")
		if got != ctx {
			t.Fatal("expected default hook to return the same context")
		}
	})
}
