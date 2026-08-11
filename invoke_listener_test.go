package go_redismq

import (
	"context"
	"testing"
)

func TestRegisterInternalListeners(t *testing.T) {
	t.Run("registers the invoke listener under its message key", func(t *testing.T) {
		originalListeners := listeners
		originalTopics := Topics

		t.Cleanup(func() {
			listeners = originalListeners
			Topics = originalTopics
		})

		listeners = nil
		Topics = nil

		RegisterInternalListeners(context.Background())

		key := GetMessageKey(TopicInternal, TagInvoke)

		registered, ok := Listeners()[key]
		if !ok {
			t.Fatalf("expected listener registered under key %q", key)
		}

		if _, ok := registered.(*MessageInvokeListener); !ok {
			t.Fatalf("expected registered listener to be *MessageInvokeListener, got %T", registered)
		}
	})
}
