package producer

import (
	"context"
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

type stubTracer struct {
	stamped []*mqtype.Message
}

func (t *stubTracer) StampTraceID(_ context.Context, message *mqtype.Message) {
	t.stamped = append(t.stamped, message)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type fakePipeliner struct {
	redis.Pipeliner

	setCalls  []setCall
	delCalls  []delCall
	lpushCall *lpushCall
	lremCalls []lremCall
}

type setCall struct {
	key   string
	value any
}

type delCall struct {
	keys []string
}

type lpushCall struct {
	key    string
	values []any
}

type lremCall struct {
	key   string
	count int64
	value any
}

func (f *fakePipeliner) Set(ctx context.Context, key string, value any, _ time.Duration) *redis.StatusCmd {
	f.setCalls = append(f.setCalls, setCall{key: key, value: value})

	return redis.NewStatusCmd(ctx)
}

func (f *fakePipeliner) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.delCalls = append(f.delCalls, delCall{keys: keys})

	return redis.NewIntCmd(ctx)
}

func (f *fakePipeliner) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	f.lpushCall = &lpushCall{key: key, values: values}

	return redis.NewIntCmd(ctx)
}

func (f *fakePipeliner) LRem(ctx context.Context, key string, count int64, value any) *redis.IntCmd {
	f.lremCalls = append(f.lremCalls, lremCall{key: key, count: count, value: value})

	return redis.NewIntCmd(ctx)
}

func newTestProducer(t *testing.T, redisClient Redis) (*Producer, *captureHandler, *stubTracer) {
	t.Helper()

	log, handler := newTestAttrLogger()
	tracer := &stubTracer{}

	p := New(func() (Redis, error) { return redisClient, nil }, log, tracer, fixedClock{now: time.Unix(1000, 0)})

	return p, handler, tracer
}

func TestProducerLogSourceResolvesToProducerPackage(t *testing.T) {
	t.Run("pin: source resolves to producer.go, not adapter.go or logger.go", func(t *testing.T) {
		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("1-0")

				return cmd
			},
		}

		p, handler, _ := newTestProducer(t, mockRedis)

		_, err := p.Publish(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1"})
		require.NoError(t, err)
		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.Equal(t, "producer.go", file)
	})
}

func TestPublish(t *testing.T) {
	t.Run("blank tag returns ErrBlankTag", func(t *testing.T) {
		p, _, _ := newTestProducer(t, &RedisMock{})

		sent, err := p.Publish(context.Background(), &mqtype.Message{Topic: "t", Tag: mqtype.TagBlank})

		require.False(t, sent)
		require.ErrorIs(t, err, ErrBlankTag)
	})

	t.Run("non-blank message id returns ErrMessageIDNotBlank", func(t *testing.T) {
		p, _, _ := newTestProducer(t, &RedisMock{})

		sent, err := p.Publish(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "already-set"})

		require.False(t, sent)
		require.ErrorIs(t, err, ErrMessageIDNotBlank)
	})

	t.Run("successful publish stamps trace id, send time, and message id", func(t *testing.T) {
		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("42-0")

				return cmd
			},
		}

		p, _, tracer := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		sent, err := p.Publish(context.Background(), message)

		require.NoError(t, err)
		require.True(t, sent)
		require.Equal(t, "42-0", message.MessageId)
		require.Equal(t, int64(1000000), message.SendTime)
		require.Len(t, tracer.stamped, 1)
	})

	t.Run("XAdd failure is propagated", func(t *testing.T) {
		wantErr := errors.New("boom")

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetErr(wantErr)

				return cmd
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		sent, err := p.Publish(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1"})

		require.False(t, sent)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("redis resolution failure is propagated", func(t *testing.T) {
		wantErr := errors.New("no redis")

		log, _ := newTestAttrLogger()
		p := New(func() (Redis, error) { return nil, wantErr }, log, &stubTracer{}, nil)

		sent, err := p.Publish(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1"})

		require.False(t, sent)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestSendTransaction(t *testing.T) {
	t.Run("blank tag returns ErrBlankTag", func(t *testing.T) {
		p, _, _ := newTestProducer(t, &RedisMock{})

		sent, err := p.SendTransaction(context.Background(), &mqtype.Message{Topic: "t", Tag: mqtype.TagBlank}, nil)

		require.False(t, sent)
		require.ErrorIs(t, err, ErrBlankTag)
	})

	t.Run("StartDeliverTime set returns ErrDelayNotSupportedInTransaction", func(t *testing.T) {
		p, _, _ := newTestProducer(t, &RedisMock{})

		message := &mqtype.Message{Topic: "t", Tag: "tag1", StartDeliverTime: 100}

		sent, err := p.SendTransaction(context.Background(), message, nil)

		require.False(t, sent)
		require.ErrorIs(t, err, ErrDelayNotSupportedInTransaction)
	})

	t.Run("commit status commits the prepared message", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("7-0")

				return cmd
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		sent, err := p.SendTransaction(context.Background(), message, func(_ *mqtype.Message) (mqtype.TransactionStatus, error) {
			return mqtype.CommitTransaction, nil
		})

		require.NoError(t, err)
		require.True(t, sent)
		require.Equal(t, "7-0", message.MessageId)
	})

	t.Run("rollback status deletes the prepared message", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		wantErr := errors.New("business failure")

		sent, err := p.SendTransaction(context.Background(), message, func(_ *mqtype.Message) (mqtype.TransactionStatus, error) {
			return mqtype.RollbackTransaction, wantErr
		})

		require.False(t, sent)
		require.ErrorIs(t, err, wantErr)
		require.Len(t, pipe.delCalls, 1)
	})

	t.Run("unknown status returns ErrUnknownTransactionStatus", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1"}

		sent, err := p.SendTransaction(context.Background(), message, func(_ *mqtype.Message) (mqtype.TransactionStatus, error) {
			return mqtype.Unknown, nil
		})

		require.False(t, sent)
		require.ErrorIs(t, err, ErrUnknownTransactionStatus)
	})
}

func TestCommit(t *testing.T) {
	t.Run("commits by re-adding to the stream and removing the prepare entry", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("99-0")

				return cmd
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "prepare-1"}

		sent, err := p.Commit(context.Background(), message)

		require.NoError(t, err)
		require.True(t, sent)
		require.Equal(t, "99-0", message.MessageId)
		require.Len(t, pipe.delCalls, 1)
		require.Equal(t, []string{"prepare-1"}, pipe.delCalls[0].keys)
	})
}

func TestRollback(t *testing.T) {
	t.Run("deletes prepare message and list entry", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
		}

		p, _, _ := newTestProducer(t, mockRedis)

		message := &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "prepare-2"}

		sent, err := p.Rollback(context.Background(), message)

		require.NoError(t, err)
		require.True(t, sent)
		require.Len(t, pipe.lremCalls, 1)
		require.Equal(t, "prepare-2", pipe.lremCalls[0].value)
	})
}
