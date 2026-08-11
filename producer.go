package redismq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/idgen"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

func Send(ctx context.Context, message *Message) (bool, error) {
	return sendMessage(ctx, message, "ProducerWrapper")
}

func SendTransaction(ctx context.Context, message *Message, transactionExecuter func(messageToSend *Message) (TransactionStatus, error)) (bool, error) {
	if message.Tag == TagBlank {
		return false, errors.New("blank tag message")
	}

	if message.StartDeliverTime > 0 {
		return false, errors.New("delay message not support transaction")
	}

	send, err := sendTransactionPrepareMessage(ctx, message)
	if err != nil || !send {
		return send, err
	}

	status, err := transactionExecuter(message)
	switch status {
	case RollbackTransaction:
		_, rollBackErr := rollbackTransactionPrepareMessage(ctx, message)
		if rollBackErr != nil {
			logAttrs(ctx, slog.LevelError, "redismq: transaction rollback failed", causeAttr(rollBackErr), slog.Any("transaction_cause", err))
		}

		return false, err
	case CommitTransaction:
		return commitTransactionPrepareMessage(ctx, message)
	default:
		return false, errors.New("unknown transaction status")
	}
}

var (
	ErrMessageIDNotBlank    = errors.New("redismq: message id must be blank when sending")
	ErrDeliverTimeInThePast = errors.New("redismq: start deliver time must be in the future")
)

func sendDelayMessage(ctx context.Context, message *Message) (bool, error) {
	if message.StartDeliverTime-time.Now().Unix() <= 0 {
		return false, ErrDeliverTimeInThePast
	}

	send, err := SendDelay(ctx, message, message.StartDeliverTime-time.Now().Unix())
	logAttrs(ctx, slog.LevelInfo, "redismq: delay message send result", append(messageAttrs(message), slog.Bool("sent", send))...)

	if err != nil {
		return false, err
	}

	return send, nil
}

func sendMessage(ctx context.Context, message *Message, source string) (bool, error) {
	if message.Tag == TagBlank {
		return false, errors.New("blank空消息")
	}

	if len(message.MessageId) != 0 {
		return false, ErrMessageIDNotBlank
	}

	message.SendTime = currentTimeMillis()

	client, err := newRedisClient()
	if err != nil {
		return false, err
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	stampTraceID(ctx, message)

	streamMessageId, err := client.XAdd(ctx, message.toStreamAddArgsValues(streamname.Queue(message.Topic))).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: stream publish failed", append(messageAttrs(message), causeAttr(err), slog.String("stream", streamname.Queue(message.Topic)))...)

		return false, err
	}

	message.MessageId = streamMessageId
	logAttrs(ctx, slog.LevelInfo, "redismq: message published", append(messageAttrs(message), slog.String("stream", streamname.Queue(message.Topic)), slog.String("source", source))...)

	return true, nil
}

func sendTransactionPrepareMessage(ctx context.Context, message *Message) (bool, error) {
	if message.Tag == TagBlank {
		return false, errors.New("Blank Message")
	}

	message.MessageId = idgen.UniqueNo(message.Topic)
	message.SendTime = currentTimeMillis()

	client, err := newRedisClient()
	if err != nil {
		return false, err
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	stampTraceID(ctx, message)

	messageJson, err := json.Marshal(message)

	jsonString := string(messageJson)

	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message marshal failed", append(messageAttrs(message), causeAttr(err))...)

		return false, err
	}

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, message.MessageId, jsonString, -1)
		pipe.LPush(ctx, streamname.TransactionPrepareQueue(message.Topic), message.MessageId)

		return nil
	})
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare pipeline failed", append(messageAttrs(message), causeAttr(err))...)

		return false, err
	}

	return true, nil
}

func rollbackTransactionPrepareMessage(ctx context.Context, message *Message) (bool, error) {
	return delTransactionPrepareMessage(ctx, message)
}

func delTransactionPrepareMessage(ctx context.Context, message *Message) (bool, error) {
	client, err := newRedisClient()
	if err != nil {
		return false, err
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, message.MessageId)
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(message.Topic), 1, message.MessageId)

		return nil
	})
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message delete failed", append(messageAttrs(message), causeAttr(err))...)

		return false, err
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: transaction prepare message deleted", messageAttrs(message)...)

	return true, nil
}

func commitTransactionPrepareMessage(ctx context.Context, message *Message) (bool, error) {
	oldMessageId := message.MessageId
	message.MessageId = ""

	client, err := newRedisClient()
	if err != nil {
		return false, err
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	stampTraceID(ctx, message)

	streamMessageId := ""

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		streamMessageId, _ = client.XAdd(ctx, message.toStreamAddArgsValues(streamname.Queue(message.Topic))).Result()
		message.MessageId = streamMessageId

		pipe.Del(ctx, oldMessageId)
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(message.Topic), 1, oldMessageId)

		return nil
	})
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction commit failed", append(messageAttrs(message), causeAttr(err))...)

		return false, err
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: transaction committed", append(messageAttrs(message), slog.String("prepare_message_id", oldMessageId))...)

	return true, nil
}
