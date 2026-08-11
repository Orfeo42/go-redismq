package redismq

import (
	"context"
	"testing"

	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

func TestRegisterInternalListeners(t *testing.T) {
	t.Run("registers the invoke listener under its message key", func(t *testing.T) {
		listenerMu.Lock()
		originalListeners := listeners
		originalTopics := topics
		listeners = map[string]IMessageListener{}
		topics = nil
		listenerMu.Unlock()

		t.Cleanup(func() {
			listenerMu.Lock()
			listeners = originalListeners
			topics = originalTopics
			listenerMu.Unlock()
		})

		RegisterInternalListeners(context.Background())

		key := streamname.MessageKey(TopicInternal, TagInvoke)

		registered, ok := snapshotListeners()[key]
		if !ok {
			t.Fatalf("expected listener registered under key %q", key)
		}

		if _, ok := registered.(*messageInvokeListener); !ok {
			t.Fatalf("expected registered listener to be *messageInvokeListener, got %T", registered)
		}
	})
}
