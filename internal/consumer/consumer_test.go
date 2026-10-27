package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

func TestSetAndGetConsumerName(t *testing.T) {
	t.Run("round trips through the instance", func(t *testing.T) {
		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		require.Empty(t, c.getConsumerName())

		c.setConsumerName("10.0.0.1")

		require.Equal(t, "10.0.0.1", c.getConsumerName())
	})
}

func TestTryCreateGroup(t *testing.T) {
	t.Run("creates the group when XInfoGroups does not report it", func(t *testing.T) {
		var created bool

		var gotStream, gotGroup, gotStart string

		mockRedis := &RedisMock{
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal(nil)

				return cmd
			},
			XGroupCreateMkStreamFunc: func(_ context.Context, stream, group, start string) *redis.StatusCmd {
				created = true
				gotStream = stream
				gotGroup = group
				gotStart = start

				return redis.NewStatusCmd(context.Background())
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.tryCreateGroup(context.Background(), "stream1")

		require.True(t, created)
		require.Equal(t, "stream1", gotStream)
		require.Equal(t, "GID_Test", gotGroup)
		require.Equal(t, "0", gotStart)
	})

	t.Run("never writes to the stream", func(t *testing.T) {
		var xaddCalls atomic.Int32

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				xaddCalls.Add(1)

				return redis.NewStringCmd(context.Background())
			},
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal(nil)

				return cmd
			},
			XGroupCreateMkStreamFunc: func(_ context.Context, _, _, _ string) *redis.StatusCmd {
				return redis.NewStatusCmd(context.Background())
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.tryCreateGroup(context.Background(), "stream1")

		require.Equal(t, int32(0), xaddCalls.Load())
	})

	t.Run("skips creation when the group is already reported", func(t *testing.T) {
		var created bool

		mockRedis := &RedisMock{
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal([]redis.XInfoGroup{{Name: "GID_Test"}})

				return cmd
			},
			XGroupCreateMkStreamFunc: func(_ context.Context, _, _, _ string) *redis.StatusCmd {
				created = true

				return redis.NewStatusCmd(context.Background())
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.tryCreateGroup(context.Background(), "stream1")

		require.False(t, created)
	})

	t.Run("redis resolution failure does not panic", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		c := New(
			func() (Redis, error) { return nil, context.Canceled },
			nil,
			log,
			&ListenerRegistryMock{},
			&DelaySchedulerMock{},
			&TransactionCheckerMock{},
			stubTracer{},
			func() string { return "GID_Test" },
			nil,
			nil,
			int64(10000),
		)

		require.NotPanics(t, func() {
			c.tryCreateGroup(context.Background(), "stream1")
		})
	})
}

func TestTryCreateConsumer(t *testing.T) {
	t.Run("creates the consumer for the resolved name", func(t *testing.T) {
		var gotConsumer string

		mockRedis := &RedisMock{
			XGroupCreateConsumerFunc: func(_ context.Context, _, _, consumer string) *redis.IntCmd {
				gotConsumer = consumer

				return redis.NewIntCmd(context.Background())
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))
		c.setConsumerName("10.0.0.5")

		c.tryCreateConsumer(context.Background(), "stream1")

		require.Equal(t, "10.0.0.5", gotConsumer)
	})
}

func TestBlockConsumerTopicStartsTransactionChecker(t *testing.T) {
	t.Run("launches the transaction checker for the topic", func(t *testing.T) {
		mockRedis := &RedisMock{
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal([]redis.XInfoGroup{{Name: "GID_Test"}})

				return cmd
			},
			XGroupCreateConsumerFunc: func(_ context.Context, _, _, _ string) *redis.IntCmd {
				return redis.NewIntCmd(context.Background())
			},
			ClientUnblockFunc: func(ctx context.Context, _ int64) *redis.IntCmd {
				return redis.NewIntCmd(ctx)
			},
		}

		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetVal(1)

				return cmd
			},
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(context.Canceled)

				return cmd
			},
			CloseFunc: func() error { return nil },
		}

		ran := make(chan string, 1)

		txChecker := &TransactionCheckerMock{
			RunFunc: func(_ context.Context, topic string) {
				ran <- topic
			},
		}

		log, _ := newTestAttrLogger()
		c := New(
			func() (Redis, error) { return mockRedis, nil },
			func() StreamReader { return reader },
			log,
			&ListenerRegistryMock{},
			&DelaySchedulerMock{},
			txChecker,
			stubTracer{},
			func() string { return "GID_Test" },
			nil,
			nil,
			int64(10000),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c.blockConsumerTopic(ctx, "t")

		select {
		case topic := <-ran:
			require.Equal(t, "t", topic)
		case <-time.After(time.Second):
			t.Fatal("expected transaction checker to run")
		}
	})
}

func TestBlockReceiveConsumerMessage(t *testing.T) {
	t.Run("redis.Nil is treated as no message without logging a warning", func(t *testing.T) {
		reader := &StreamReaderMock{
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		message := c.blockReceiveConsumerMessage(context.Background(), reader, "t", time.Second)

		require.Nil(t, message)
	})

	t.Run("decodes a received message", func(t *testing.T) {
		reader := &StreamReaderMock{
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetVal([]redis.XStream{
					{
						Stream: "stream1",
						Messages: []redis.XMessage{
							{
								ID: "5-0",
								Values: map[string]any{
									"topic": "t",
									"tag":   "tag1",
									"body":  "hello",
								},
							},
						},
					},
				})

				return cmd
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		message := c.blockReceiveConsumerMessage(context.Background(), reader, "t", time.Second)

		require.NotNil(t, message)
		require.Equal(t, "5-0", message.MessageId)
		require.Equal(t, "hello", message.Body)
	})

	t.Run("read error after cancellation is not logged", func(t *testing.T) {
		reader := &StreamReaderMock{
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(errors.New("use of closed connection"))

				return cmd
			},
		}

		c, handler := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		message := c.blockReceiveConsumerMessage(ctx, reader, "t", time.Second)

		require.Nil(t, message)

		for _, record := range handler.records {
			require.NotEqual(t, "redismq: read consumer group failed", record.Message)
		}
	})
}

func TestLoopConsumer(t *testing.T) {
	t.Run("normal path reads with the long block", func(t *testing.T) {
		mockRedis := &RedisMock{
			ClientUnblockFunc: func(ctx context.Context, _ int64) *redis.IntCmd {
				return redis.NewIntCmd(ctx)
			},
		}

		var gotBlock atomic.Int64

		started := make(chan struct{})

		var startedOnce sync.Once

		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetVal(7)

				return cmd
			},
			XReadGroupFunc: func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				gotBlock.Store(int64(a.Block))
				startedOnce.Do(func() { close(started) })

				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
			CloseFunc: func() error { return nil },
		}

		registry := &ListenerRegistryMock{
			TopicCountFunc: func() int { return 1 },
		}

		c, _ := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))
		c.newStreamReader = func() StreamReader { return reader }

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		returned := make(chan struct{})

		go func() {
			defer close(returned)

			c.loopConsumer(ctx, "t")
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected the read to start")
		}

		cancel()

		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("expected loopConsumer to return after cancellation")
		}

		require.Equal(t, int64(60*time.Second), gotBlock.Load())
	})

	t.Run("unavailable unblock uses the short read block", func(t *testing.T) {
		var gotBlock atomic.Int64

		started := make(chan struct{})

		var startedOnce sync.Once

		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetErr(errors.New("NOPERM"))

				return cmd
			},
			XReadGroupFunc: func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				gotBlock.Store(int64(a.Block))
				startedOnce.Do(func() { close(started) })

				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
			CloseFunc: func() error { return nil },
		}

		registry := &ListenerRegistryMock{
			TopicCountFunc: func() int { return 1 },
		}

		c, _ := newTestConsumer(&RedisMock{}, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))
		c.newStreamReader = func() StreamReader { return reader }

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		returned := make(chan struct{})

		go func() {
			defer close(returned)

			c.loopConsumer(ctx, "t")
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected the read to start")
		}

		cancel()

		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("expected loopConsumer to return after cancellation")
		}

		require.Equal(t, int64(5*time.Second), gotBlock.Load())
	})

	t.Run("cancellation unblocks the blocked read and returns", func(t *testing.T) {
		started := make(chan struct{})
		unblocked := make(chan struct{})

		var startedOnce, unblockOnce sync.Once

		var unblockedID atomic.Int64

		var unblockCalls atomic.Int32

		var closeCalls atomic.Int32

		mockRedis := &RedisMock{
			ClientUnblockFunc: func(ctx context.Context, id int64) *redis.IntCmd {
				unblockedID.Store(id)

				if unblockCalls.Add(1) > 1 {
					unblockOnce.Do(func() { close(unblocked) })
				}

				return redis.NewIntCmd(ctx)
			},
		}

		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetVal(7)

				return cmd
			},
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				startedOnce.Do(func() { close(started) })
				<-unblocked

				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
			CloseFunc: func() error {
				closeCalls.Add(1)

				return nil
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))
		c.newStreamReader = func() StreamReader { return reader }

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		returned := make(chan struct{})

		go func() {
			defer close(returned)

			c.loopConsumer(ctx, "t")
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected the read to start")
		}

		cancel()

		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("expected loopConsumer to return after cancellation")
		}

		require.Equal(t, int64(7), unblockedID.Load())
		require.Equal(t, int32(1), closeCalls.Load())
	})

	t.Run("client id failure still closes the reader on cancellation", func(t *testing.T) {
		var closeCalls atomic.Int32

		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetErr(errors.New("NOPERM"))

				return cmd
			},
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				time.Sleep(10 * time.Millisecond)

				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
			CloseFunc: func() error {
				closeCalls.Add(1)

				return nil
			},
		}

		registry := &ListenerRegistryMock{
			TopicCountFunc: func() int { return 1 },
		}

		c, handler := newTestConsumer(&RedisMock{}, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))
		c.newStreamReader = func() StreamReader { return reader }

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		returned := make(chan struct{})

		go func() {
			defer close(returned)

			c.loopConsumer(ctx, "t")
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()

		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatal("expected loopConsumer to return after cancellation")
		}

		require.Equal(t, int32(1), closeCalls.Load())

		found := false

		for _, record := range handler.records {
			if record.Level == slog.LevelWarn && record.Message == "redismq: stream reader client id failed" {
				found = true
			}
		}

		require.True(t, found, "expected a client id failure warning")
	})
}

func TestGetConsumer(t *testing.T) {
	t.Run("blank tag returns nil without consulting the registry", func(t *testing.T) {
		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener {
				t.Fatal("unexpected registry lookup for a blank message")

				return nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		listener := c.getConsumer(&mqtype.Message{Tag: mqtype.TagBlank})

		require.Nil(t, listener)
	})
}

func TestStartUnblocker(t *testing.T) {
	t.Run("returns nil when client id fails", func(t *testing.T) {
		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetErr(errors.New("NOPERM client id"))

				return cmd
			},
		}

		c, handler := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		stop := c.startUnblocker(context.Background(), &RedisMock{}, reader, "t")

		require.Nil(t, stop)
		require.Len(t, handler.records, 1)
		require.Equal(t, "redismq: stream reader client id failed", handler.records[0].Message)
	})

	t.Run("returns nil and warns when unblock permission is denied", func(t *testing.T) {
		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetVal(7)

				return cmd
			},
		}

		mockRedis := &RedisMock{
			ClientUnblockFunc: func(ctx context.Context, _ int64) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetErr(errors.New("NOPERM this user has no permissions to run the 'client|unblock' command"))

				return cmd
			},
		}

		c, handler := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		stop := c.startUnblocker(context.Background(), mockRedis, reader, "t")

		require.Nil(t, stop)
		require.Len(t, handler.records, 1)
		require.Equal(t, slog.LevelWarn, handler.records[0].Level)
		require.Equal(t, "redismq: stream reader unblock unavailable, using short read block", handler.records[0].Message)
	})

	t.Run("returns a stop func when unblock is permitted", func(t *testing.T) {
		reader := &StreamReaderMock{
			ClientIDFunc: func(ctx context.Context) *redis.IntCmd {
				cmd := redis.NewIntCmd(ctx)
				cmd.SetVal(7)

				return cmd
			},
		}

		mockRedis := &RedisMock{
			ClientUnblockFunc: func(ctx context.Context, _ int64) *redis.IntCmd {
				return redis.NewIntCmd(ctx)
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		stop := c.startUnblocker(context.Background(), mockRedis, reader, "t")

		require.NotNil(t, stop)
		stop()
	})
}
