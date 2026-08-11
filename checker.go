package redismq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

type IMessageChecker interface {
	GetTopic() string
	GetTag() string
	Checker(message *Message) TransactionStatus
}

var (
	checkerMu sync.RWMutex
	checkers  = map[string]IMessageChecker{}
)

func snapshotCheckers() map[string]IMessageChecker {
	checkerMu.RLock()
	defer checkerMu.RUnlock()

	out := make(map[string]IMessageChecker, len(checkers))
	for k, v := range checkers {
		out[k] = v
	}

	return out
}

func getCheckerFor(topic string, tag string) IMessageChecker {
	messageKey := streamname.MessageKey(topic, tag)

	checkerMu.RLock()
	defer checkerMu.RUnlock()

	return checkers[messageKey]
}

func RegisterChecker(ctx context.Context, i IMessageChecker) {
	if i == nil {
		return
	}

	messageKey := streamname.MessageKey(i.GetTopic(), i.GetTag())

	checkerMu.Lock()
	defer checkerMu.Unlock()

	if checkers[messageKey] != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: duplicate checker for message key, checker dropped", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))

		return
	}

	checkers[messageKey] = i
	logAttrs(ctx, slog.LevelInfo, "redismq: checker registered", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))
}
