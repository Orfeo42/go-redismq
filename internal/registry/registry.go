package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

const maxTopics = 60

var (
	ErrNilListener       = errors.New("redismq: listener is nil")
	ErrTooManyTopics     = errors.New("redismq: too many topics registered")
	ErrInvalidTopic      = errors.New("redismq: invalid topic")
	ErrDuplicateListener = errors.New("redismq: duplicate listener for message key")
	ErrNilChecker        = errors.New("redismq: checker is nil")
	ErrDuplicateChecker  = errors.New("redismq: duplicate checker for message key")
)

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

func (r *Registry) RegisterListener(ctx context.Context, i mqtype.IMessageListener) error {
	if i == nil {
		return ErrNilListener
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.topics) > maxTopics {
		return ErrTooManyTopics
	}

	if !IsValidTopic(i.GetTopic()) {
		return ErrInvalidTopic
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	if r.listeners[messageKey] != nil {
		return ErrDuplicateListener
	}

	r.listeners[messageKey] = i
	r.topics = append(r.topics, i.GetTopic())
	r.attrLogger.LogAttrs(ctx, slog.LevelInfo, "redismq: listener registered", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))

	return nil
}

func (r *Registry) RegisterChecker(ctx context.Context, i mqtype.IMessageChecker) error {
	if i == nil {
		return ErrNilChecker
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.checkers[messageKey] != nil {
		return ErrDuplicateChecker
	}

	r.checkers[messageKey] = i
	r.attrLogger.LogAttrs(ctx, slog.LevelInfo, "redismq: checker registered", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))

	return nil
}
