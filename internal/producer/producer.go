package producer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/idgen"
	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

//go:generate moq -out redis_mock.go . Redis
type Redis interface {
	XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd
	TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

//go:generate moq -out tracer_mock.go . Tracer
type Tracer interface {
	StampTraceID(ctx context.Context, message *mqtype.Message)
}

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

var (
	ErrMessageIDNotBlank              = errors.New("redismq: message id must be blank when sending")
	ErrBlankTag                       = errors.New("redismq: message tag must not be blank")
	ErrDelayNotSupportedInTransaction = errors.New("redismq: delayed message cannot be sent transactionally")
	ErrUnknownTransactionStatus       = errors.New("redismq: unknown transaction status")
)

type Producer struct {
	resolveRedis func() (Redis, error)
	log          logging.AttrLogger
	tracer       Tracer
	clock        Clock
}

func New(resolveRedis func() (Redis, error), log logging.AttrLogger, tracer Tracer, clock Clock) *Producer {
	if clock == nil {
		clock = systemClock{}
	}

	return &Producer{resolveRedis: resolveRedis, log: log, tracer: tracer, clock: clock}
}

func (p *Producer) currentTimeMillis() int64 {
	return p.clock.Now().UnixNano() / int64(time.Millisecond)
}

func (p *Producer) Publish(ctx context.Context, message *mqtype.Message) (bool, error) {
	return p.sendMessage(ctx, message, "ProducerWrapper")
}

func (p *Producer) SendTransaction(ctx context.Context, message *mqtype.Message, transactionExecuter func(messageToSend *mqtype.Message) (mqtype.TransactionStatus, error)) (bool, error) {
	if message.Tag == mqtype.TagBlank {
		return false, ErrBlankTag
	}

	if message.StartDeliverTime > 0 {
		return false, ErrDelayNotSupportedInTransaction
	}

	send, err := p.sendTransactionPrepareMessage(ctx, message)
	if err != nil || !send {
		return send, err
	}

	status, err := transactionExecuter(message)
	switch status {
	case mqtype.RollbackTransaction:
		_, rollBackErr := p.Rollback(ctx, message)
		if rollBackErr != nil {
			p.log.LogAttrs(ctx, slog.LevelError, "redismq: transaction rollback failed", logattr.CauseAttr(rollBackErr), slog.Any("transaction_cause", err))
		}

		return false, err
	case mqtype.CommitTransaction:
		return p.Commit(ctx, message)
	default:
		return false, ErrUnknownTransactionStatus
	}
}

func (p *Producer) sendMessage(ctx context.Context, message *mqtype.Message, source string) (bool, error) {
	if message.Tag == mqtype.TagBlank {
		return false, ErrBlankTag
	}

	if len(message.MessageId) != 0 {
		return false, ErrMessageIDNotBlank
	}

	message.SendTime = p.currentTimeMillis()

	client, err := p.resolveRedis()
	if err != nil {
		return false, err
	}

	p.tracer.StampTraceID(ctx, message)

	streamAddArgs, err := message.ToStreamAddArgsValues(streamname.Queue(message.Topic))
	if err != nil {
		return false, err
	}

	streamMessageId, err := client.XAdd(ctx, streamAddArgs).Result()
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "redismq: stream publish failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err), slog.String("stream", streamname.Queue(message.Topic)))...)

		return false, err
	}

	message.MessageId = streamMessageId
	p.log.LogAttrs(ctx, slog.LevelInfo, "redismq: message published",
		append(logattr.MessageAttrs(message), slog.String("stream", streamname.Queue(message.Topic)), slog.String("source", source))...)

	return true, nil
}

func (p *Producer) sendTransactionPrepareMessage(ctx context.Context, message *mqtype.Message) (bool, error) {
	if message.Tag == mqtype.TagBlank {
		return false, ErrBlankTag
	}

	message.MessageId = idgen.UniqueNo(message.Topic)
	message.SendTime = p.currentTimeMillis()

	client, err := p.resolveRedis()
	if err != nil {
		return false, err
	}

	p.tracer.StampTraceID(ctx, message)

	messageJson, err := json.Marshal(message)

	jsonString := string(messageJson)

	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message marshal failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false, err
	}

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, message.MessageId, jsonString, -1)
		pipe.LPush(ctx, streamname.TransactionPrepareQueue(message.Topic), message.MessageId)

		return nil
	})
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare pipeline failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false, err
	}

	return true, nil
}

func (p *Producer) Rollback(ctx context.Context, message *mqtype.Message) (bool, error) {
	return p.delTransactionPrepareMessage(ctx, message)
}

func (p *Producer) delTransactionPrepareMessage(ctx context.Context, message *mqtype.Message) (bool, error) {
	client, err := p.resolveRedis()
	if err != nil {
		return false, err
	}

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, message.MessageId)
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(message.Topic), 1, message.MessageId)

		return nil
	})
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message delete failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false, err
	}

	p.log.LogAttrs(ctx, slog.LevelInfo, "redismq: transaction prepare message deleted", logattr.MessageAttrs(message)...)

	return true, nil
}

func (p *Producer) Commit(ctx context.Context, message *mqtype.Message) (bool, error) {
	oldMessageId := message.MessageId
	message.MessageId = ""

	client, err := p.resolveRedis()
	if err != nil {
		return false, err
	}

	p.tracer.StampTraceID(ctx, message)

	streamMessageId := ""

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		streamAddArgs, argsErr := message.ToStreamAddArgsValues(streamname.Queue(message.Topic))
		if argsErr != nil {
			return argsErr
		}

		streamMessageId, _ = client.XAdd(ctx, streamAddArgs).Result()
		message.MessageId = streamMessageId

		pipe.Del(ctx, oldMessageId)
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(message.Topic), 1, oldMessageId)

		return nil
	})
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "redismq: transaction commit failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false, err
	}

	p.log.LogAttrs(ctx, slog.LevelInfo, "redismq: transaction committed",
		append(logattr.MessageAttrs(message), slog.String("prepare_message_id", oldMessageId))...)

	return true, nil
}
