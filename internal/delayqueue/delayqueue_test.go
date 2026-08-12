package delayqueue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())

	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

func newTestAttrLogger() (logging.AttrLogger, *captureHandler) {
	handler := &captureHandler{}
	adapter := logging.NewSlogAdapter(slog.New(handler))
	bridge := logging.NewBridge(func() logging.Logger { return adapter })

	return bridge, handler
}

type stubTracer struct{}

func (stubTracer) StampTraceID(_ context.Context, _ *mqtype.Message) {}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func newTestQueue(redisClient Redis, publisher Publisher) (*Queue, *captureHandler) {
	log, handler := newTestAttrLogger()

	q := New(func() (Redis, error) { return redisClient, nil }, log, stubTracer{}, fixedClock{now: time.Unix(1000, 0)}, publisher, nil)

	return q, handler
}

func TestDelayQueueLogSourceResolvesToDelayQueuePackage(t *testing.T) {
	t.Run("pin: source resolves to delayqueue.go, not adapter.go or logger.go", func(t *testing.T) {
		mockRedis := &RedisMock{
			ZAddFunc: func(_ context.Context, _ string, _ ...redis.Z) *redis.IntCmd {
				return redis.NewIntCmd(context.Background())
			},
		}

		q, handler := newTestQueue(mockRedis, &PublisherMock{})

		_, err := q.ScheduleDelay(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1"}, 5)
		require.NoError(t, err)

		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.Equal(t, "delayqueue.go", file)
	})
}

func TestScheduleDelay(t *testing.T) {
	t.Run("non-positive delay returns ErrDeliverTimeInThePast", func(t *testing.T) {
		q, _ := newTestQueue(&RedisMock{}, &PublisherMock{})

		sent, err := q.ScheduleDelay(context.Background(), &mqtype.Message{}, 0)

		require.False(t, sent)
		require.ErrorIs(t, err, ErrDeliverTimeInThePast)
	})

	t.Run("negative delay returns ErrDeliverTimeInThePast", func(t *testing.T) {
		q, _ := newTestQueue(&RedisMock{}, &PublisherMock{})

		sent, err := q.ScheduleDelay(context.Background(), &mqtype.Message{}, -5)

		require.False(t, sent)
		require.ErrorIs(t, err, ErrDeliverTimeInThePast)
	})

	t.Run("positive delay sets StartDeliverTime and sends", func(t *testing.T) {
		var zaddScore float64

		mockRedis := &RedisMock{
			ZAddFunc: func(_ context.Context, _ string, members ...redis.Z) *redis.IntCmd {
				zaddScore = members[0].Score

				return redis.NewIntCmd(context.Background())
			},
		}

		q, _ := newTestQueue(mockRedis, &PublisherMock{})

		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		sent, err := q.ScheduleDelay(context.Background(), message, 60)

		require.NoError(t, err)
		require.True(t, sent)
		require.Equal(t, int64(1060), message.StartDeliverTime)
		require.InDelta(t, float64(1060), zaddScore, 0)
	})

	t.Run("SendDelay failure is propagated", func(t *testing.T) {
		wantErr := errors.New("zadd failed")

		mockRedis := &RedisMock{
			ZAddFunc: func(_ context.Context, _ string, _ ...redis.Z) *redis.IntCmd {
				cmd := redis.NewIntCmd(context.Background())
				cmd.SetErr(wantErr)

				return cmd
			},
		}

		q, _ := newTestQueue(mockRedis, &PublisherMock{})

		sent, err := q.ScheduleDelay(context.Background(), &mqtype.Message{}, 60)

		require.False(t, sent)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestSendDelay(t *testing.T) {
	t.Run("redis resolution failure is propagated", func(t *testing.T) {
		wantErr := errors.New("no redis")

		log, _ := newTestAttrLogger()
		q := New(func() (Redis, error) { return nil, wantErr }, log, stubTracer{}, nil, &PublisherMock{}, nil)

		sent, err := q.SendDelay(context.Background(), &mqtype.Message{}, 5)

		require.False(t, sent)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestRepublishDelayedMessage(t *testing.T) {
	t.Run("clears StartDeliverTime and MessageId and publishes", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "old-id", StartDeliverTime: 12345}

		raw, err := json.Marshal(message)
		require.NoError(t, err)

		mockRedis := &RedisMock{
			ZRemFunc: func(_ context.Context, _ string, _ ...any) *redis.IntCmd {
				return redis.NewIntCmd(context.Background())
			},
		}

		var published *mqtype.Message

		publisher := &PublisherMock{
			PublishFunc: func(_ context.Context, m *mqtype.Message) (bool, error) {
				published = m

				return true, nil
			},
		}

		q, _ := newTestQueue(mockRedis, publisher)

		q.republishDelayedMessage(context.Background(), mockRedis, "key", string(raw))

		require.NotNil(t, published)
		require.Equal(t, int64(0), published.StartDeliverTime)
		require.Empty(t, published.MessageId)
	})

	t.Run("ZRem failure prevents republish", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		raw, err := json.Marshal(message)
		require.NoError(t, err)

		mockRedis := &RedisMock{
			ZRemFunc: func(_ context.Context, _ string, _ ...any) *redis.IntCmd {
				cmd := redis.NewIntCmd(context.Background())
				cmd.SetErr(errors.New("zrem failed"))

				return cmd
			},
		}

		published := false

		publisher := &PublisherMock{
			PublishFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				published = true

				return true, nil
			},
		}

		q, _ := newTestQueue(mockRedis, publisher)

		q.republishDelayedMessage(context.Background(), mockRedis, "key", string(raw))

		require.False(t, published)
	})

	t.Run("malformed json is dropped without publishing", func(t *testing.T) {
		published := false

		publisher := &PublisherMock{
			PublishFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				published = true

				return true, nil
			},
		}

		q, _ := newTestQueue(&RedisMock{}, publisher)

		q.republishDelayedMessage(context.Background(), &RedisMock{}, "key", "not-json")

		require.False(t, published)
	})
}

func TestPollingCore(t *testing.T) {
	t.Run("republishes due messages returned by ZRangeByScore", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1"}
		raw, err := json.Marshal(message)
		require.NoError(t, err)

		mockRedis := &RedisMock{
			ZRangeByScoreFunc: func(_ context.Context, _ string, _ *redis.ZRangeBy) *redis.StringSliceCmd {
				cmd := redis.NewStringSliceCmd(context.Background())
				cmd.SetVal([]string{string(raw)})

				return cmd
			},
			ZRemFunc: func(_ context.Context, _ string, _ ...any) *redis.IntCmd {
				return redis.NewIntCmd(context.Background())
			},
		}

		published := false

		publisher := &PublisherMock{
			PublishFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				published = true

				return true, nil
			},
		}

		q, _ := newTestQueue(mockRedis, publisher)

		q.pollingCore(context.Background(), "key")

		require.True(t, published)
	})
}
