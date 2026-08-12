package consumer

import (
	"context"
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

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("1-0")

				return cmd
			},
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal(nil)

				return cmd
			},
			XGroupCreateMkStreamFunc: func(_ context.Context, _, _, _ string) *redis.StatusCmd {
				created = true

				return redis.NewStatusCmd(context.Background())
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.tryCreateGroup(context.Background(), "stream1", "t")

		require.True(t, created)
	})

	t.Run("skips creation when the group is already reported", func(t *testing.T) {
		var created bool

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("1-0")

				return cmd
			},
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

		c.tryCreateGroup(context.Background(), "stream1", "t")

		require.False(t, created)
	})

	t.Run("redis resolution failure does not panic", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		c := New(
			func() (Redis, error) { return nil, context.Canceled },
			log,
			&ListenerRegistryMock{},
			&DelaySchedulerMock{},
			&TransactionCheckerMock{},
			stubTracer{},
			func() string { return "GID_Test" },
			nil,
			nil,
		)

		require.NotPanics(t, func() {
			c.tryCreateGroup(context.Background(), "stream1", "t")
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
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("1-0")

				return cmd
			},
			XInfoGroupsFunc: func(_ context.Context, _ string) *redis.XInfoGroupsCmd {
				cmd := redis.NewXInfoGroupsCmd(context.Background(), "")
				cmd.SetVal([]redis.XInfoGroup{{Name: "GID_Test"}})

				return cmd
			},
			XGroupCreateConsumerFunc: func(_ context.Context, _, _, _ string) *redis.IntCmd {
				return redis.NewIntCmd(context.Background())
			},
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(context.Canceled)

				return cmd
			},
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
			log,
			&ListenerRegistryMock{},
			&DelaySchedulerMock{},
			txChecker,
			stubTracer{},
			func() string { return "GID_Test" },
			nil,
			nil,
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
		mockRedis := &RedisMock{
			XReadGroupFunc: func(ctx context.Context, _ *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
				cmd := redis.NewXStreamSliceCmd(ctx)
				cmd.SetErr(redis.Nil)

				return cmd
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		message := c.blockReceiveConsumerMessage(context.Background(), mockRedis, "t")

		require.Nil(t, message)
	})

	t.Run("decodes a received message", func(t *testing.T) {
		mockRedis := &RedisMock{
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

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		message := c.blockReceiveConsumerMessage(context.Background(), mockRedis, "t")

		require.NotNil(t, message)
		require.Equal(t, "5-0", message.MessageId)
		require.Equal(t, "hello", message.Body)
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
