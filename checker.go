package go_redismq

import (
	"context"
	"fmt"
	"log/slog"
)

type IMessageChecker interface {
	GetTopic() string
	GetTag() string
	Checker(message *Message) TransactionStatus
}

var checkers map[string]IMessageChecker

func Checkers() map[string]IMessageChecker {
	if checkers == nil {
		checkers = make(map[string]IMessageChecker)
	}

	return checkers
}

func RegisterChecker(ctx context.Context, i IMessageChecker) {
	if i == nil {
		return
	}

	messageKey := GetMessageKey(i.GetTopic(), i.GetTag())

	if Checkers()[messageKey] != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: duplicate checker for message key, checker dropped", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))

		return
	}

	Checkers()[messageKey] = i
	logAttrs(ctx, slog.LevelInfo, "redismq: checker registered", slog.String("message_key", messageKey), slog.String("checker_type", fmt.Sprintf("%T", i)))
}
