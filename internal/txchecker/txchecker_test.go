package txchecker

import (
	"context"
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

func (stubTracer) ConsumeContext(ctx context.Context, _ *mqtype.Message) context.Context { return ctx }

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type fakeChecker struct {
	fn func(*mqtype.Message) mqtype.TransactionStatus
}

func (c *fakeChecker) GetTopic() string { return "t" }

func (c *fakeChecker) GetTag() string { return "tag1" }

func (c *fakeChecker) Checker(m *mqtype.Message) mqtype.TransactionStatus { return c.fn(m) }

type fakePipeliner struct {
	redis.Pipeliner

	lremCalls  []string
	rpushCalls []string
}

func (f *fakePipeliner) LRem(ctx context.Context, _ string, _ int64, value any) *redis.IntCmd {
	f.lremCalls = append(f.lremCalls, value.(string))

	return redis.NewIntCmd(ctx)
}

func (f *fakePipeliner) RPush(ctx context.Context, _ string, values ...any) *redis.IntCmd {
	for _, v := range values {
		f.rpushCalls = append(f.rpushCalls, v.(string))
	}

	return redis.NewIntCmd(ctx)
}

func newTestChecker(redisClient Redis, registry CheckerRegistry, completer TransactionCompleter, now time.Time) (*Checker, *captureHandler) {
	log, handler := newTestAttrLogger()

	c := New(func() (Redis, error) { return redisClient, nil }, log, registry, completer, stubTracer{}, fixedClock{now: now})

	return c, handler
}

func TestTxCheckerLogSourceResolvesToTxCheckerPackage(t *testing.T) {
	t.Run("pin: source resolves to txchecker.go, not adapter.go or logger.go", func(t *testing.T) {
		mockRedis := &RedisMock{
			LRangeFunc: func(_ context.Context, _ string, _, _ int64) *redis.StringSliceCmd {
				cmd := redis.NewStringSliceCmd(context.Background())
				cmd.SetVal([]string{"id-1"})

				return cmd
			},
			GetFunc: func(_ context.Context, _ string) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("")

				return cmd
			},
		}
		registry := &CheckerRegistryMock{}
		completer := &TransactionCompleterMock{}

		c, handler := newTestChecker(mockRedis, registry, completer, time.Unix(0, 0))

		c.fetchTransactionPrepareMessagesForChecker(context.Background(), "t")

		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.Equal(t, "txchecker.go", file)
	})
}

func TestCheckTransactionPrepareMessage(t *testing.T) {
	t.Run("commit status commits", func(t *testing.T) {
		committed := false

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker {
				return &fakeChecker{fn: func(_ *mqtype.Message) mqtype.TransactionStatus { return mqtype.CommitTransaction }}
			},
		}
		completer := &TransactionCompleterMock{
			CommitFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				committed = true

				return true, nil
			},
		}

		c, _ := newTestChecker(&RedisMock{}, registry, completer, time.Unix(0, 0))

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1"})

		require.True(t, committed)
	})

	t.Run("rollback status rolls back", func(t *testing.T) {
		rolledBack := false

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker {
				return &fakeChecker{fn: func(_ *mqtype.Message) mqtype.TransactionStatus { return mqtype.RollbackTransaction }}
			},
		}
		completer := &TransactionCompleterMock{
			RollbackFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				rolledBack = true

				return true, nil
			},
		}

		c, _ := newTestChecker(&RedisMock{}, registry, completer, time.Unix(0, 0))

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1"})

		require.True(t, rolledBack)
	})

	t.Run("unknown status within death window does not death-queue", func(t *testing.T) {
		mockRedis := &RedisMock{
			TxPipelinedFunc: func(_ context.Context, _ func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				t.Fatal("unexpected death queue write")

				return nil, nil
			},
		}

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker {
				return &fakeChecker{fn: func(_ *mqtype.Message) mqtype.TransactionStatus { return mqtype.Unknown }}
			},
		}
		completer := &TransactionCompleterMock{}

		c, _ := newTestChecker(mockRedis, registry, completer, time.Unix(0, 0))

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1", SendTime: 0})
	})

	t.Run("unknown status past 8 hours routes to death queue", func(t *testing.T) {
		pipe := &fakePipeliner{}

		mockRedis := &RedisMock{
			TxPipelinedFunc: func(_ context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, fn(pipe)
			},
		}

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker {
				return &fakeChecker{fn: func(_ *mqtype.Message) mqtype.TransactionStatus { return mqtype.Unknown }}
			},
		}
		completer := &TransactionCompleterMock{}

		now := time.Unix(0, 0).Add(9 * time.Hour)

		c, _ := newTestChecker(mockRedis, registry, completer, now)

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "id-1", SendTime: 0})

		require.Len(t, pipe.lremCalls, 1)
		require.Len(t, pipe.rpushCalls, 1)
		require.Equal(t, "id-1", pipe.rpushCalls[0])
	})

	t.Run("no checker registered within rollback window is a no-op", func(t *testing.T) {
		rolledBack := false

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker { return nil },
		}
		completer := &TransactionCompleterMock{
			RollbackFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				rolledBack = true

				return true, nil
			},
		}

		c, _ := newTestChecker(&RedisMock{}, registry, completer, time.Unix(0, 0))

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1", SendTime: 0})

		require.False(t, rolledBack)
	})

	t.Run("no checker registered past 7 days rolls back", func(t *testing.T) {
		rolledBack := false

		registry := &CheckerRegistryMock{
			GetCheckerForFunc: func(_, _ string) mqtype.IMessageChecker { return nil },
		}
		completer := &TransactionCompleterMock{
			RollbackFunc: func(_ context.Context, _ *mqtype.Message) (bool, error) {
				rolledBack = true

				return true, nil
			},
		}

		now := time.Unix(0, 0).Add(8 * 24 * time.Hour)

		c, _ := newTestChecker(&RedisMock{}, registry, completer, now)

		c.checkTransactionPrepareMessage(context.Background(), "t", &mqtype.Message{Topic: "t", Tag: "tag1", SendTime: 0})

		require.True(t, rolledBack)
	})
}

func TestFetchTransactionPrepareMessagesForChecker(t *testing.T) {
	t.Run("decodes stored messages and skips missing bodies", func(t *testing.T) {
		mockRedis := &RedisMock{
			LRangeFunc: func(_ context.Context, _ string, _, _ int64) *redis.StringSliceCmd {
				cmd := redis.NewStringSliceCmd(context.Background())
				cmd.SetVal([]string{"id-1", "id-2"})

				return cmd
			},
			GetFunc: func(_ context.Context, key string) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())

				if key == "id-1" {
					cmd.SetVal(`{"topic":"t","tag":"tag1","messageId":"id-1"}`)
				} else {
					cmd.SetVal("")
				}

				return cmd
			},
		}

		registry := &CheckerRegistryMock{}
		completer := &TransactionCompleterMock{}

		c, _ := newTestChecker(mockRedis, registry, completer, time.Unix(0, 0))

		messages := c.fetchTransactionPrepareMessagesForChecker(context.Background(), "t")

		require.Len(t, messages, 1)
		require.Equal(t, "id-1", messages[0].MessageId)
	})

	t.Run("redis resolution failure returns empty slice", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		c := New(func() (Redis, error) { return nil, context.Canceled }, log, &CheckerRegistryMock{}, &TransactionCompleterMock{}, stubTracer{}, nil)

		messages := c.fetchTransactionPrepareMessagesForChecker(context.Background(), "t")

		require.Empty(t, messages)
	})
}
