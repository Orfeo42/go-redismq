package logattr

import (
	"errors"
	"testing"

	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

func TestMessageAttrs(t *testing.T) {
	t.Run("nil message returns nil", func(t *testing.T) {
		if MessageAttrs(nil) != nil {
			t.Fatal("expected nil attrs for nil message")
		}
	})

	t.Run("populated message yields expected attrs", func(t *testing.T) {
		m := &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "id1", ReconsumeTimes: 1, ReconsumeMax: 3}

		attrs := MessageAttrs(m)
		if len(attrs) != 5 {
			t.Fatalf("expected 5 attrs, got %d", len(attrs))
		}
	})

	t.Run("message_key present when Key set", func(t *testing.T) {
		m := &mqtype.Message{Topic: "t", Tag: "tag1", Key: "mykey"}

		attrs := MessageAttrs(m)
		last := attrs[len(attrs)-1]

		if last.Key != "message_key" {
			t.Fatalf("expected last attr key %q, got %q", "message_key", last.Key)
		}
	})

	t.Run("includes trace_id when present", func(t *testing.T) {
		m := &mqtype.Message{Topic: "t", Tag: "tag1"}
		m.SetTraceID("trace-1")

		attrs := MessageAttrs(m)

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
		m := &mqtype.Message{Topic: "t", Tag: "tag1"}

		attrs := MessageAttrs(m)

		for _, a := range attrs {
			if a.Key == "trace_id" {
				t.Fatal("expected no trace_id attr")
			}
		}
	})
}

func TestTopicAttrs(t *testing.T) {
	attrs := TopicAttrs("t1", "grp1")

	if len(attrs) != 3 {
		t.Fatalf("expected 3 attrs, got %d", len(attrs))
	}

	if attrs[1].Value.String() != "grp1" {
		t.Fatalf("expected consumer_group %q, got %q", "grp1", attrs[1].Value.String())
	}
}

func TestCauseAttr(t *testing.T) {
	attr := CauseAttr(errors.New("boom"))

	if attr.Key != "cause" {
		t.Fatalf("expected key %q, got %q", "cause", attr.Key)
	}
}

func TestStackAttr(t *testing.T) {
	attr := StackAttr(1)

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
}

func TestPanicError(t *testing.T) {
	t.Run("error exception is returned as-is", func(t *testing.T) {
		original := errors.New("boom")

		got := PanicError(original)
		if !errors.Is(got, original) {
			t.Fatalf("expected %v, got %v", original, got)
		}
	})

	t.Run("non-error exception is wrapped", func(t *testing.T) {
		got := PanicError("boom")
		if got == nil {
			t.Fatal("expected non-nil error")
		}
	})
}
