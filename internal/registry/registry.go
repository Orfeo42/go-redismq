package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

const maxTopics = 60

type Registry struct {
	mu         sync.RWMutex
	listeners  map[string]mqtype.IMessageListener
	checkers   map[string]mqtype.IMessageChecker
	topics     []string
	attrLogger logging.AttrLogger
}

func New(attrLogger logging.AttrLogger) *Registry {
	return &Registry{
		listeners:  map[string]mqtype.IMessageListener{},
		checkers:   map[string]mqtype.IMessageChecker{},
		topics:     make([]string, 0, 100),
		attrLogger: attrLogger,
	}
}

func (r *Registry) SnapshotListeners() map[string]mqtype.IMessageListener {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]mqtype.IMessageListener, len(r.listeners))
	for k, v := range r.listeners {
		out[k] = v
	}

	return out
}

func (r *Registry) SnapshotCheckers() map[string]mqtype.IMessageChecker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]mqtype.IMessageChecker, len(r.checkers))
	for k, v := range r.checkers {
		out[k] = v
	}

	return out
}

func (r *Registry) GetListenerFor(topic string, tag string) mqtype.IMessageListener {
	messageKey := streamname.MessageKey(topic, tag)

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.listeners[messageKey]
}

func (r *Registry) GetCheckerFor(topic string, tag string) mqtype.IMessageChecker {
	messageKey := streamname.MessageKey(topic, tag)

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.checkers[messageKey]
}

func (r *Registry) GetTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(r.topics))
	copy(out, r.topics)

	return out
}

func (r *Registry) TopicCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.topics)
}

func IsValidTopic(topic string) bool {
	return len(topic) > 0 && topic != "*"
}

func (r *Registry) RegisterListener(ctx context.Context, i mqtype.IMessageListener) {
	if i == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.topics) > maxTopics {
		r.attrLogger.LogAttrs(ctx, slog.LevelWarn, "redismq: too many topics registered, merge listeners", slog.Int("topic_count", len(r.topics)))

		return
	}

	if !IsValidTopic(i.GetTopic()) {
		r.attrLogger.LogAttrs(ctx, slog.LevelWarn, "redismq: invalid topic, listener dropped", slog.String("topic", i.GetTopic()))

		return
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	if r.listeners[messageKey] != nil {
		r.attrLogger.LogAttrs(ctx, slog.LevelWarn, "redismq: duplicate listener for message key, listener dropped", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))

		return
	}

	r.listeners[messageKey] = i
	r.topics = append(r.topics, i.GetTopic())
	r.attrLogger.LogAttrs(ctx, slog.LevelInfo, "redismq: listener registered", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))
}

func (r *Registry) RegisterChecker(ctx context.Context, i mqtype.IMessageChecker) {
	if i == nil {
		return
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.checkers[messageKey] != nil {
		r.attrLogger.LogAttrs(ctx, slog.LevelWarn, "redismq: duplicate checker for message key, checker dropped", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))

		return
	}

	r.checkers[messageKey] = i
	r.attrLogger.LogAttrs(ctx, slog.LevelInfo, "redismq: checker registered", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))
}

// ResetListenersForTest swaps in empty listener/topic state and returns a
// closure that restores the original state. It exists so callers outside
// this package (root's own tests) can isolate registry state around a test
// without reaching into unexported fields.
func (r *Registry) ResetListenersForTest() (restore func()) {
	r.mu.Lock()
	originalListeners := r.listeners
	originalTopics := r.topics
	r.listeners = map[string]mqtype.IMessageListener{}
	r.topics = make([]string, 0, 100)
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		r.listeners = originalListeners
		r.topics = originalTopics
		r.mu.Unlock()
	}
}

// ResetCheckersForTest swaps in empty checker state and returns a closure
// that restores the original state, mirroring ResetListenersForTest.
func (r *Registry) ResetCheckersForTest() (restore func()) {
	r.mu.Lock()
	originalCheckers := r.checkers
	r.checkers = map[string]mqtype.IMessageChecker{}
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		r.checkers = originalCheckers
		r.mu.Unlock()
	}
}
