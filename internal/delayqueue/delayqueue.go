package delayqueue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

const mqDelayQueueName = "MQ_DELAY_QUEUE_SET"

//go:generate moq -out redis_mock.go . Redis
type Redis interface {
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
}

//go:generate moq -out publisher_mock.go . Publisher
type Publisher interface {
	Publish(ctx context.Context, message *mqtype.Message) (bool, error)
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

var ErrDeliverTimeInThePast = errors.New("redismq: start deliver time must be in the future")

type Queue struct {
	resolveRedis func() (Redis, error)
	log          logging.AttrLogger
	tracer       Tracer
	clock        Clock
	publisher    Publisher
	wg           *sync.WaitGroup
}

func New(resolveRedis func() (Redis, error), log logging.AttrLogger, tracer Tracer, clock Clock, publisher Publisher, wg *sync.WaitGroup) *Queue {
	if clock == nil {
		clock = systemClock{}
	}

	if wg == nil {
		wg = &sync.WaitGroup{}
	}

	return &Queue{resolveRedis: resolveRedis, log: log, tracer: tracer, clock: clock, publisher: publisher, wg: wg}
}

func (q *Queue) StartBackgroundThread(ctx context.Context) {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		defer func() {
			if exception := recover(); exception != nil {
				err := logattr.PanicError(exception)

				q.log.LogAttrs(ctx, slog.LevelError, "redismq: delay background thread panic recovered", logattr.CauseAttr(err), logattr.StackAttr(2))

				return
			}
		}()

		for {
			q.polling(ctx)

			select {
			case <-ctx.Done():
				q.log.LogAttrs(ctx, slog.LevelInfo, "redismq: delay background thread stopped, context cancelled")

				return
			case <-time.After(10 * time.Second):
			}
		}
	}()
}

func (q *Queue) polling(ctx context.Context) {
	client, err := q.resolveRedis()
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	result, err := client.Keys(ctx, mqDelayQueueName).Result()
	if err != nil {
		return
	}

	for _, key := range result {
		q.pollingCore(ctx, key)
	}
}

func (q *Queue) pollingCore(ctx context.Context, key string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			q.log.LogAttrs(ctx, slog.LevelError, "redismq: delay queue polling panic recovered", logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	client, err := q.resolveRedis()
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	result, err := client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "0",
		Max:    strconv.FormatInt(q.clock.Now().Unix(), 10),
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
		q.republishDelayedMessage(ctx, client, key, messageJson)
	}
}

func (q *Queue) republishDelayedMessage(ctx context.Context, client Redis, key string, messageJson string) {
	message, err := decodeDelayedMessage(messageJson)
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay queue message unmarshal failed", logattr.CauseAttr(err))

		return
	}

	err = client.ZRem(ctx, key, messageJson).Err()
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay queue message removal failed, message will be redelivered",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return
	}

	message.StartDeliverTime = 0
	message.MessageId = ""

	_, sendErr := q.publisher.Publish(ctx, message)
	if sendErr != nil {
		q.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay message removed but resend failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(sendErr))...)
	}
}

func decodeDelayedMessage(messageJson string) (*mqtype.Message, error) {
	var message *mqtype.Message

	err := json.Unmarshal([]byte(messageJson), &message)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func (q *Queue) SendDelay(ctx context.Context, message *mqtype.Message, delay int64) (bool, error) {
	client, err := q.resolveRedis()
	if err != nil {
		return false, err
	}

	q.tracer.StampTraceID(ctx, message)

	messageJson, err := json.Marshal(message)
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay message marshal failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err), slog.String("delay_queue", mqDelayQueueName))...)

		return false, err
	}

	jsonString := string(messageJson)
	score := q.clock.Now().Unix() + delay

	_, err = client.ZAdd(ctx, mqDelayQueueName, redis.Z{
		Score:  float64(score),
		Member: jsonString,
	}).Result()
	if err != nil {
		q.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay message zadd failed",
			append(logattr.MessageAttrs(message), logattr.CauseAttr(err), slog.String("delay_queue", mqDelayQueueName))...)

		return false, err
	}

	return true, nil
}

func (q *Queue) ScheduleDelay(ctx context.Context, message *mqtype.Message, delaySeconds int64) (bool, error) {
	if delaySeconds <= 0 {
		return false, ErrDeliverTimeInThePast
	}

	message.StartDeliverTime = q.clock.Now().Unix() + delaySeconds

	sent, err := q.SendDelay(ctx, message, delaySeconds)
	q.log.LogAttrs(ctx, slog.LevelInfo, "redismq: delay message send result", append(logattr.MessageAttrs(message), slog.Bool("sent", sent))...)

	if err != nil {
		return false, err
	}

	return sent, nil
}
