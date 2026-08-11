package redismq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

type IMessageListener interface {
	GetTopic() string
	GetTag() string
	Consume(ctx context.Context, message *Message) Action
}

var (
	listenerMu sync.RWMutex
	listeners  = map[string]IMessageListener{}
	topics     = make([]string, 0, 100)
)

func snapshotListeners() map[string]IMessageListener {
	listenerMu.RLock()
	defer listenerMu.RUnlock()

	out := make(map[string]IMessageListener, len(listeners))
	for k, v := range listeners {
		out[k] = v
	}

	return out
}

func getListenerFor(topic string, tag string) IMessageListener {
	messageKey := streamname.MessageKey(topic, tag)

	listenerMu.RLock()
	defer listenerMu.RUnlock()

	return listeners[messageKey]
}

func getTopics() []string {
	listenerMu.RLock()
	defer listenerMu.RUnlock()

	out := make([]string, len(topics))
	copy(out, topics)

	return out
}

func topicCount() int {
	listenerMu.RLock()
	defer listenerMu.RUnlock()

	return len(topics)
}

func isValidTopic(topic string) bool {
	return len(topic) > 0 && topic != "*"
}

func RegisterListener(ctx context.Context, i IMessageListener) {
	if i == nil {
		return
	}

	listenerMu.Lock()
	defer listenerMu.Unlock()

	if len(topics) > 60 {
		logAttrs(ctx, slog.LevelWarn, "redismq: too many topics registered, merge listeners", slog.Int("topic_count", len(topics)))

		return
	}

	if !isValidTopic(i.GetTopic()) {
		logAttrs(ctx, slog.LevelWarn, "redismq: invalid topic, listener dropped", slog.String("topic", i.GetTopic()))

		return
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	if listeners[messageKey] != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: duplicate listener for message key, listener dropped", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))

		return
	}

	listeners[messageKey] = i
	topics = append(topics, i.GetTopic())
	logAttrs(ctx, slog.LevelInfo, "redismq: listener registered", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))
}
