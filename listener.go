package go_redismq

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type IMessageListener interface {
	GetTopic() string
	GetTag() string
	Consume(ctx context.Context, message *Message) Action
}

var listeners map[string]IMessageListener
var Topics []string

func Listeners() map[string]IMessageListener {
	if listeners == nil {
		listeners = make(map[string]IMessageListener)
	}

	return listeners
}

func isValidTopic(topic string) bool {
	return len(topic) > 0 && strings.Compare(topic, "*") != 0
}

func RegisterListener(ctx context.Context, i IMessageListener) {
	if i == nil {
		return
	}

	if Topics == nil {
		Topics = make([]string, 0, 100)
	}

	if len(Topics) > 60 {
		logAttrs(ctx, slog.LevelWarn, "redismq: too many topics registered, merge listeners", slog.Int("topic_count", len(Topics)))

		return
	}

	if !isValidTopic(i.GetTopic()) {
		logAttrs(ctx, slog.LevelWarn, "redismq: invalid topic, listener dropped", slog.String("topic", i.GetTopic()))

		return
	}

	if Listeners()[GetMessageKey(i.GetTopic(), i.GetTag())] != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: duplicate listener for message key, listener dropped", slog.String("message_key", GetMessageKey(i.GetTopic(), i.GetTag())), slog.String("listener_type", fmt.Sprintf("%T", i)))
	} else {
		messageKey := GetMessageKey(i.GetTopic(), i.GetTag())
		Listeners()[messageKey] = i

		Topics = append(Topics, i.GetTopic())
		logAttrs(ctx, slog.LevelInfo, "redismq: listener registered", slog.String("message_key", messageKey), slog.String("listener_type", fmt.Sprintf("%T", i)))
	}
}
