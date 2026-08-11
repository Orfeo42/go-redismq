package go_redismq

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/redis/go-redis/v9"
)

const (
	MqDelayQueueName = "MQ_DELAY_QUEUE_SET"
)

func StartDelayBackgroundThread(ctx context.Context) {
	go func() {
		defer func() {
			if exception := recover(); exception != nil {
				var err error

				if v, ok := exception.(error); ok && gerror.HasStack(v) {
					err = v
				} else {
					err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
				}

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
	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	client := redis.NewClient(options)

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
			var err error

			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: delay queue polling panic recovered", causeAttr(err), stackAttr(2))

			return
		}
	}()

	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	client := redis.NewClient(options)

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	result, err := client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "0",
		Max:    strconv.FormatInt(gtime.Now().Timestamp(), 10),
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
		var message *Message

		err = gjson.Unmarshal([]byte(messageJson), &message)
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: delay queue message unmarshal failed", causeAttr(err))

			continue
		}

		err = client.ZRem(ctx, key, messageJson).Err()
		if err == nil {
			message.StartDeliverTime = 0
			message.MessageId = ""

			_, sendErr := sendMessage(ctx, message, "DelayQueue")
			if sendErr != nil {
				logAttrs(ctx, slog.LevelWarn, "redismq: delay message removed but resend failed", append(messageAttrs(message), causeAttr(sendErr))...)
			}
		} else {
			logAttrs(ctx, slog.LevelWarn, "redismq: delay queue message removal failed, message will be redelivered", append(messageAttrs(message), causeAttr(err))...)
		}
	}
}

func SendDelay(ctx context.Context, message *Message, delay int64) (bool, error) {
	options, err := GetRedisConfig()
	if err != nil {
		return false, err
	}

	client := redis.NewClient(options)

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	stampTraceID(ctx, message)

	messageJson, err := gjson.Marshal(message)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay message marshal failed", append(messageAttrs(message), causeAttr(err), slog.String("delay_queue", MqDelayQueueName))...)

		return false, err
	}

	jsonString := string(messageJson)
	score := gtime.Now().Timestamp() + delay

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
