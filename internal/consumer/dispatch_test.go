package consumer

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

type stubTracer struct{}

func (stubTracer) ConsumeContext(ctx context.Context, _ *mqtype.Message) context.Context { return ctx }

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type stubListener struct {
	action mqtype.Action
}

func (l *stubListener) GetTopic() string { return "t" }

func (l *stubListener) GetTag() string { return "tag1" }

func (l *stubListener) Consume(_ context.Context, _ *mqtype.Message) mqtype.Action { return l.action }

type streamIDCall struct {
	stream string
	group  string
	ids    []string
}

type fakePipeliner struct {
	redis.Pipeliner

	xackCalls []streamIDCall
	xdelCalls []streamIDCall
	signal    chan struct{}
}

func (f *fakePipeliner) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	f.xackCalls = append(f.xackCalls, streamIDCall{stream: stream, group: group, ids: ids})

	return redis.NewIntCmd(ctx)
}

func (f *fakePipeliner) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	f.xdelCalls = append(f.xdelCalls, streamIDCall{stream: stream, ids: ids})

	if f.signal != nil {
		f.signal <- struct{}{}
	}

	return redis.NewIntCmd(ctx)
}

func newPipelinedRedis(pipe *fakePipeliner) *RedisMock {
	return &RedisMock{
		PipelinedFunc: func(_ context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
			return nil, fn(pipe)
		},
	}
}

func recordHasAttr(record slog.Record, key string) bool {
	found := false

	record.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
		}

		return true
	})

	return found
}

func newTestConsumer(
	redisClient Redis,
	registry ListenerRegistry,
	delayScheduler DelayScheduler,
	group string,
	now time.Time,
) (*Consumer, *captureHandler) {
	log, handler := newTestAttrLogger()

	c := New(
		func() (Redis, error) { return redisClient, nil },
		log,
		registry,
		delayScheduler,
		&TransactionCheckerMock{},
		stubTracer{},
		func() string { return group },
		fixedClock{now: now},
		nil,
		int64(10000),
	)

	return c, handler
}

func TestConsumerLogSourceResolvesToConsumerPackage(t *testing.T) {
	t.Run("pin: source resolves to consumer package files, not adapter.go or logger.go", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		registry := &ListenerRegistryMock{}

		c, handler := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.messageAck(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "1-0"})

		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.True(t, file == "consumer.go" || file == "dispatch.go", "expected consumer.go or dispatch.go, got %s", file)
	})
}

func TestDispatchConsumerMessage(t *testing.T) {
	t.Run("no registered listener drops and acks the message", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener { return nil },
		}

		c, _ := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.dispatchConsumerMessage(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "1-0"})

		require.Len(t, pipe.xackCalls, 1)
		require.Len(t, pipe.xdelCalls, 1)
	})

	t.Run("registered listener returning CommitMessage acks the message", func(t *testing.T) {
		pipe := &fakePipeliner{signal: make(chan struct{}, 1)}
		mockRedis := newPipelinedRedis(pipe)

		listener := &stubListener{action: mqtype.CommitMessage}

		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener { return listener },
		}

		c, _ := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.dispatchConsumerMessage(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "1-0", ConsumerDelayMilliSeconds: 1})

		select {
		case <-pipe.signal:
		case <-time.After(2 * time.Second):
			t.Fatal("expected message to be acked")
		}
	})

	t.Run("registered listener returning ReconsumeLater schedules a delay", func(t *testing.T) {
		scheduled := make(chan struct{}, 1)

		listener := &stubListener{action: mqtype.ReconsumeLater}

		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener { return listener },
		}

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled <- struct{}{}

				return true, nil
			},
		}

		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		c, _ := newTestConsumer(mockRedis, registry, delayScheduler, "GID_Test", time.Unix(0, 0))

		c.dispatchConsumerMessage(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "1-0", ConsumerDelayMilliSeconds: 1})

		select {
		case <-scheduled:
		case <-time.After(2 * time.Second):
			t.Fatal("expected delay to be scheduled")
		}
	})

	t.Run("ReconsumeLater at cap with drop flag acks and deletes without death queue", func(t *testing.T) {
		pipe := &fakePipeliner{signal: make(chan struct{}, 1)}
		mockRedis := newPipelinedRedis(pipe)

		xaddCalled := false
		mockRedis.XAddFunc = func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
			xaddCalled = true

			return redis.NewStringCmd(context.Background())
		}

		listener := &stubListener{action: mqtype.ReconsumeLater}

		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener { return listener },
		}

		c, _ := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.dispatchConsumerMessage(context.Background(), &mqtype.Message{
			Topic: "t", Tag: "tag1", MessageId: "7-0", ConsumerDelayMilliSeconds: 1,
			ReconsumeMax: 3, ReconsumeTimes: 3, DropOnReconsumeExhausted: true,
		})

		select {
		case <-pipe.signal:
		case <-time.After(2 * time.Second):
			t.Fatal("expected message to be acked and deleted")
		}

		c.wg.Wait()

		require.False(t, xaddCalled)
		require.Len(t, pipe.xdelCalls, 1)
		require.Equal(t, []string{"7-0"}, pipe.xdelCalls[0].ids)
	})
}

func TestRunConsumeMessage(t *testing.T) {
	t.Run("expired message is acked and deleted", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.UnixMilli(1000*60*60*24*4))

		c.runConsumeMessage(context.Background(), &stubListener{action: mqtype.CommitMessage}, &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "5-0", SendTime: 1})

		require.Len(t, pipe.xdelCalls, 1)
		require.Equal(t, []string{"5-0"}, pipe.xdelCalls[0].ids)
	})

	t.Run("broadcast message is acked and deleted", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "6-0", CustomData: map[string]any{"messageModel": "BROADCASTING"}}

		c.runConsumeMessage(context.Background(), &stubListener{action: mqtype.CommitMessage}, message)

		require.Len(t, pipe.xdelCalls, 1)
		require.Equal(t, []string{"6-0"}, pipe.xdelCalls[0].ids)
	})
}

type blockingListener struct {
	started chan struct{}
	release chan struct{}
}

func (l *blockingListener) GetTopic() string { return "t" }

func (l *blockingListener) GetTag() string { return "tag1" }

func (l *blockingListener) Consume(_ context.Context, _ *mqtype.Message) mqtype.Action {
	close(l.started)
	<-l.release

	return mqtype.CommitMessage
}

func TestConsumeMessageGoroutineTracksWaitGroup(t *testing.T) {
	t.Run("wg.Wait blocks until the in-flight handler completes", func(t *testing.T) {
		listener := &blockingListener{started: make(chan struct{}), release: make(chan struct{})}

		registry := &ListenerRegistryMock{
			GetListenerForFunc: func(_, _ string) mqtype.IMessageListener { return listener },
		}

		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		c, _ := newTestConsumer(mockRedis, registry, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.dispatchConsumerMessage(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "1-0", ConsumerDelayMilliSeconds: 1})

		select {
		case <-listener.started:
		case <-time.After(2 * time.Second):
			t.Fatal("expected handler to start")
		}

		waitDone := make(chan struct{})

		go func() {
			c.wg.Wait()
			close(waitDone)
		}()

		select {
		case <-waitDone:
			t.Fatal("expected wg.Wait to block while the handler is in flight")
		case <-time.After(100 * time.Millisecond):
		}

		close(listener.release)

		select {
		case <-waitDone:
		case <-time.After(2 * time.Second):
			t.Fatal("expected wg.Wait to return once the handler completes")
		}
	})
}

func TestMessageAck(t *testing.T) {
	t.Run("acks and deletes the entry in one pipeline", func(t *testing.T) {
		pipe := &fakePipeliner{}
		mockRedis := newPipelinedRedis(pipe)

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		c.messageAck(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "42-0"})

		require.Len(t, pipe.xackCalls, 1)
		require.Len(t, pipe.xdelCalls, 1)
		require.Contains(t, pipe.xackCalls[0].stream, "t")
		require.Equal(t, "GID_Test", pipe.xackCalls[0].group)
		require.Equal(t, []string{"42-0"}, pipe.xackCalls[0].ids)
		require.Equal(t, pipe.xackCalls[0].stream, pipe.xdelCalls[0].stream)
		require.Equal(t, []string{"42-0"}, pipe.xdelCalls[0].ids)
	})

	t.Run("pipeline error is logged with cause and does not panic", func(t *testing.T) {
		mockRedis := &RedisMock{
			PipelinedFunc: func(_ context.Context, _ func(redis.Pipeliner) error) ([]redis.Cmder, error) {
				return nil, errors.New("pipeline failed")
			},
		}

		c, handler := newTestConsumer(mockRedis, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		require.NotPanics(t, func() {
			c.messageAck(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "42-0"})
		})

		require.Len(t, handler.records, 1)
		require.Equal(t, slog.LevelWarn, handler.records[0].Level)
		require.Equal(t, "redismq: message ack failed", handler.records[0].Message)
		require.True(t, recordHasAttr(handler.records[0], "cause"))
	})

	t.Run("panic inside ack is recovered and logged with cause", func(t *testing.T) {
		log, handler := newTestAttrLogger()

		c := New(
			func() (Redis, error) { panic("boom") },
			log,
			&ListenerRegistryMock{},
			&DelaySchedulerMock{},
			&TransactionCheckerMock{},
			stubTracer{},
			func() string { return "GID_Test" },
			fixedClock{now: time.Unix(0, 0)},
			nil,
			int64(10000),
		)

		require.NotPanics(t, func() {
			c.messageAck(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", MessageId: "42-0"})
		})

		found := false

		for _, record := range handler.records {
			record.Attrs(func(a slog.Attr) bool {
				if a.Key == "cause" {
					found = true
				}

				return true
			})
		}

		require.True(t, found, "expected a log record with a cause attr")
	})
}

func TestPushTaskToResumeLater(t *testing.T) {
	t.Run("increments ReconsumeTimes and schedules the 60*ReconsumeTimes backoff", func(t *testing.T) {
		var gotDelay int64

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, delaySeconds int64) (bool, error) {
				gotDelay = delaySeconds

				return true, nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 2}

		sent := c.pushTaskToResumeLater(context.Background(), message)

		require.True(t, sent)
		require.Equal(t, 3, message.ReconsumeTimes)
		require.Equal(t, int64(180), gotDelay)
	})

	t.Run("first retry uses the 60 second floor", func(t *testing.T) {
		var gotDelay int64

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, delaySeconds int64) (bool, error) {
				gotDelay = delaySeconds

				return true, nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 0}

		c.pushTaskToResumeLater(context.Background(), message)

		require.Equal(t, int64(60), gotDelay)
	})

	t.Run("exceeding the max routes to the death queue", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		var deathQueueStream string

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, args *redis.XAddArgs) *redis.StringCmd {
				deathQueueStream = args.Stream
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("99-0")

				return cmd
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 40}

		sent := c.pushTaskToResumeLater(context.Background(), message)

		require.True(t, sent)
		require.False(t, scheduled)
		require.Contains(t, deathQueueStream, "DEATH")
	})

	t.Run("custom ReconsumeMax raises the retry ceiling", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 45, ReconsumeMax: 50}

		c.pushTaskToResumeLater(context.Background(), message)

		require.True(t, scheduled)
	})

	t.Run("ReconsumeMax below 40 lowers the retry ceiling", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		xaddCalled := false

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				xaddCalled = true
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("99-0")

				return cmd
			},
		}

		c, _ := newTestConsumer(mockRedis, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		sent := c.pushTaskToResumeLater(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 3, ReconsumeMax: 3})

		require.True(t, sent)
		require.True(t, xaddCalled)
		require.False(t, scheduled)
	})

	t.Run("ReconsumeMax below 40 still retries under the cap", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		c.pushTaskToResumeLater(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 2, ReconsumeMax: 3})

		require.True(t, scheduled)
	})

	t.Run("zero ReconsumeMax defaults to 40 retries", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		c.pushTaskToResumeLater(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 39})

		require.True(t, scheduled)
	})

	t.Run("drop on exhaustion discards instead of death queue", func(t *testing.T) {
		scheduled := false

		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				scheduled = true

				return true, nil
			},
		}

		xaddCalled := false

		mockRedis := &RedisMock{
			XAddFunc: func(_ context.Context, _ *redis.XAddArgs) *redis.StringCmd {
				xaddCalled = true

				return redis.NewStringCmd(context.Background())
			},
		}

		c, handler := newTestConsumer(mockRedis, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		message := &mqtype.Message{Topic: "t", Tag: "tag1", ReconsumeTimes: 3, ReconsumeMax: 3, DropOnReconsumeExhausted: true}

		sent := c.pushTaskToResumeLater(context.Background(), message)

		require.True(t, sent)
		require.False(t, xaddCalled)
		require.False(t, scheduled)

		found := false

		for _, record := range handler.records {
			if record.Level == slog.LevelWarn && record.Message == "redismq: message dropped, reconsume limit reached" {
				found = true
			}
		}

		require.True(t, found)
	})

	t.Run("delay scheduling failure is propagated", func(t *testing.T) {
		delayScheduler := &DelaySchedulerMock{
			ScheduleDelayFunc: func(_ context.Context, _ *mqtype.Message, _ int64) (bool, error) {
				return false, errors.New("schedule failed")
			},
		}

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, delayScheduler, "GID_Test", time.Unix(0, 0))

		sent := c.pushTaskToResumeLater(context.Background(), &mqtype.Message{Topic: "t", Tag: "tag1"})

		require.False(t, sent)
	})
}

func TestConsumerDelay(t *testing.T) {
	t.Run("internal invoke message with default delay uses the 20ms special case", func(t *testing.T) {
		message := &mqtype.Message{Topic: mqtype.TopicInternal, Tag: mqtype.TagInvoke, ConsumerDelayMilliSeconds: mqtype.DefaultConsumerDelayMilliSeconds}

		delay := consumerDelay(message)

		require.Equal(t, 20*time.Millisecond, delay)
	})

	t.Run("delay between 0 and 10000 exclusive is used as-is", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1", ConsumerDelayMilliSeconds: 500}

		delay := consumerDelay(message)

		require.Equal(t, 500*time.Millisecond, delay)
	})

	t.Run("zero delay defaults to 1000ms", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1", ConsumerDelayMilliSeconds: 0}

		delay := consumerDelay(message)

		require.Equal(t, 1000*time.Millisecond, delay)
	})

	t.Run("delay at or above 10000 falls out of range and yields zero", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1", ConsumerDelayMilliSeconds: 10000}

		delay := consumerDelay(message)

		require.Equal(t, time.Duration(0), delay)
	})

	t.Run("negative delay falls out of range and yields zero", func(t *testing.T) {
		message := &mqtype.Message{Topic: "t", Tag: "tag1", ConsumerDelayMilliSeconds: -5}

		delay := consumerDelay(message)

		require.Equal(t, time.Duration(0), delay)
	})
}

func TestMessageCost(t *testing.T) {
	t.Run("non-positive send time is never expired", func(t *testing.T) {
		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", time.Unix(0, 0))

		cost, expired := c.messageCost(&mqtype.Message{SendTime: 0})

		require.Equal(t, int64(0), cost)
		require.False(t, expired)
	})

	t.Run("message older than the expiry window is expired", func(t *testing.T) {
		now := time.UnixMilli(1000 * 60 * 60 * 24 * 4)

		c, _ := newTestConsumer(&RedisMock{}, &ListenerRegistryMock{}, &DelaySchedulerMock{}, "GID_Test", now)

		_, expired := c.messageCost(&mqtype.Message{SendTime: 1})

		require.True(t, expired)
	})
}

func TestStartScheduleTrimStreamIteration(t *testing.T) {
	t.Run("trims each topic stream, backup stream and death queue to streamMaxLen", func(t *testing.T) {
		var gotLens []int64

		mockRedis := &RedisMock{
			XTrimMaxLenFunc: func(ctx context.Context, _ string, maxLen int64) *redis.IntCmd {
				gotLens = append(gotLens, maxLen)

				return redis.NewIntCmd(ctx)
			},
		}

		registry := &ListenerRegistryMock{
			GetTopicsFunc: func() []string { return []string{"t"} },
		}

		log, _ := newTestAttrLogger()

		c := New(
			func() (Redis, error) { return mockRedis, nil },
			log,
			registry,
			&DelaySchedulerMock{},
			&TransactionCheckerMock{},
			stubTracer{},
			func() string { return "GID_Test" },
			fixedClock{now: time.Unix(0, 0)},
			nil,
			int64(500),
		)

		c.startScheduleTrimStreamIteration(context.Background(), mockRedis)

		require.Equal(t, []int64{500, 500, 500}, gotLens)
	})
}
