package go_redismq

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MqDelayQueueName = "MQ_DELAY_QUEUE_SET"
)

func StartDelayBackgroundThread(ctx context.Context) {
	go func() {
		defer func() {
			if exception := recover(); exception != nil {
				err := panicError(exception)

				logAttrs(ctx, slog.LevelError, "redismq: delay background thread panic recovered", causeAttr(err), stackAttr(2))

				return
			}
		}()

		for {
			polling(ctx)

			select {
			case <-ctx.Done():
				logAttrs(ctx, slog.LevelInfo, "redismq: delay background thread stopped, context cancelled")

				return
			case <-time.After(10 * time.Second):
			}
		}
	}()
}

func polling(ctx context.Context) {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	result, err := client.Keys(ctx, MqDelayQueueName).Result()
	if err != nil {
		return
	}

	for _, key := range result {
		pollingCore(ctx, key)
	}
}

func pollingCore(ctx context.Context, key string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: delay queue polling panic recovered", causeAttr(err), stackAttr(2))

			return
		}
	}()

	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	result, err := client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "0",
		Max:    strconv.FormatInt(time.Now().Unix(), 10),
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		return
	}

	if len(result) == 0 {
		return
	}

	for _, messageJson := range result {
		republishDelayedMessage(ctx, client, key, messageJson)
	}
}

func republishDelayedMessage(ctx context.Context, client *redis.Client, key string, messageJson string) {
	message, err := decodeDelayedMessage(messageJson)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay queue message unmarshal failed", causeAttr(err))

		return
	}

	err = client.ZRem(ctx, key, messageJson).Err()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay queue message removal failed, message will be redelivered", append(messageAttrs(message), causeAttr(err))...)

		return
	}

	message.StartDeliverTime = 0
	message.MessageId = ""

	_, sendErr := sendMessage(ctx, message, "DelayQueue")
	if sendErr != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay message removed but resend failed", append(messageAttrs(message), causeAttr(sendErr))...)
	}
}

func decodeDelayedMessage(messageJson string) (*Message, error) {
	var message *Message

	err := json.Unmarshal([]byte(messageJson), &message)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func SendDelay(ctx context.Context, message *Message, delay int64) (bool, error) {
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
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay message marshal failed", append(messageAttrs(message), causeAttr(err), slog.String("delay_queue", MqDelayQueueName))...)

		return false, err
	}

	jsonString := string(messageJson)
	score := time.Now().Unix() + delay

	_, err = client.ZAdd(ctx, MqDelayQueueName, redis.Z{
		Score:  float64(score),
		Member: jsonString,
	}).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay message zadd failed", append(messageAttrs(message), causeAttr(err), slog.String("delay_queue", MqDelayQueueName))...)

		return false, err
	}

	return true, nil
}
