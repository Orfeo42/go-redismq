package txchecker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

const (
	transactionDeathAfter    = 1000 * 60 * 60 * 8
	transactionRollbackAfter = 1000 * 60 * 60 * 24 * 7
)

//go:generate moq -out redis_mock.go . Redis
type Redis interface {
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

//go:generate moq -out checker_registry_mock.go . CheckerRegistry
type CheckerRegistry interface {
	GetCheckerFor(topic string, tag string) mqtype.IMessageChecker
}

//go:generate moq -out completer_mock.go . TransactionCompleter
type TransactionCompleter interface {
	Commit(ctx context.Context, message *mqtype.Message) (bool, error)
	Rollback(ctx context.Context, message *mqtype.Message) (bool, error)
}

//go:generate moq -out tracer_mock.go . Tracer
type Tracer interface {
	ConsumeContext(ctx context.Context, message *mqtype.Message) context.Context
}

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type Checker struct {
	resolveRedis func() (Redis, error)
	log          logging.AttrLogger
	checkers     CheckerRegistry
	completer    TransactionCompleter
	tracer       Tracer
	clock        Clock
}

func New(resolveRedis func() (Redis, error), log logging.AttrLogger, checkers CheckerRegistry, completer TransactionCompleter, tracer Tracer, clock Clock) *Checker {
	if clock == nil {
		clock = systemClock{}
	}

	return &Checker{resolveRedis: resolveRedis, log: log, checkers: checkers, completer: completer, tracer: tracer, clock: clock}
}

func (c *Checker) currentTimeMillis() int64 {
	return c.clock.Now().UnixNano() / int64(time.Millisecond)
}

func (c *Checker) Run(ctx context.Context, topic string) {
	for {
		if ctx.Err() != nil {
			c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: transaction checker loop stopped, context cancelled")

			return
		}

		c.runIteration(ctx, topic)
	}
}

func (c *Checker) runIteration(ctx context.Context, topic string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: transaction checker iteration panicked", slog.String("topic", topic), logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	messages := c.fetchTransactionPrepareMessagesForChecker(ctx, topic)
	for _, message := range messages {
		c.checkTransactionPrepareMessage(ctx, topic, message)

		time.Sleep(1 * time.Second)
	}

	select {
	case <-ctx.Done():
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: transaction checker iteration stopped, context cancelled")

		return
	case <-time.After(60 * time.Second):
	}
}

func (c *Checker) checkTransactionPrepareMessage(ctx context.Context, topic string, message *mqtype.Message) {
	msgCtx := c.tracer.ConsumeContext(ctx, message)

	ck := c.checkers.GetCheckerFor(message.Topic, message.Tag)
	if ck == nil {
		if (c.currentTimeMillis() - message.SendTime) > transactionRollbackAfter {
			_, _ = c.completer.Rollback(msgCtx, message)
		}

		return
	}

	switch ck.Checker(message) {
	case mqtype.CommitTransaction:
		_, _ = c.completer.Commit(msgCtx, message)
	case mqtype.RollbackTransaction:
		_, _ = c.completer.Rollback(msgCtx, message)
	default:
		//todo mark save send time, max retry times limit 50
		if (c.currentTimeMillis() - message.SendTime) > transactionDeathAfter {
			c.putMessageToTransactionDeathQueue(msgCtx, topic, message)
		}
	}
}

func (c *Checker) fetchTransactionPrepareMessagesForChecker(ctx context.Context, topic string) []*mqtype.Message {
	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return []*mqtype.Message{}
	}

	result, err := client.LRange(ctx, streamname.TransactionPrepareQueue(topic), 0, -1).Result()
	if err != nil {
		return []*mqtype.Message{}
	}

	messages := make([]*mqtype.Message, 0)

	for _, messageId := range result {
		if len(messageId) == 0 {
			continue
		}

		value, _ := client.Get(ctx, messageId).Result()
		if len(value) == 0 {
			c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message body missing",
				slog.String("message_id", messageId), slog.String("topic", topic))

			continue
		}

		message, ok := decodeTransactionPrepareMessage(value)
		if !ok {
			continue
		}

		messages = append(messages, message)
	}

	return messages
}

func decodeTransactionPrepareMessage(value string) (*mqtype.Message, bool) {
	var message *mqtype.Message

	err := json.Unmarshal([]byte(value), &message)
	if err != nil {
		return nil, false
	}

	return message, true
}

func (c *Checker) putMessageToTransactionDeathQueue(ctx context.Context, topic string, message *mqtype.Message) bool {
	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return false
	}

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(topic), 1, message.MessageId)
		pipe.RPush(ctx, streamname.TransactionDeathQueue(), message.MessageId)

		return nil
	})
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction message to death queue failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false
	}

	return true
}
