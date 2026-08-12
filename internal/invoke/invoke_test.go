package invoke

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

	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
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

func newTestInvoker(redisClient Redis, publisher Publisher, registry ListenerRegistry, group string) (*Invoker, *captureHandler) {
	log, handler := newTestAttrLogger()

	inv := New(func() (Redis, error) { return redisClient, nil }, log, publisher, registry, func() string { return group })

	return inv, handler
}

func TestInvokeLogSourceResolvesToInvokePackage(t *testing.T) {
	t.Run("pin: source resolves to invoke package files, not adapter.go or logger.go", func(t *testing.T) {
		inv, handler := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		err := inv.RegisterInvoke(context.Background(), "method1", func(_ context.Context, _ any) (any, error) { return nil, nil })
		require.NoError(t, err)

		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.True(t, file == "invoke.go" || file == "listener.go", "expected invoke.go or listener.go, got %s", file)
	})
}

func TestRegisterInvoke(t *testing.T) {
	t.Run("registers a new method", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		called := false

		err := inv.RegisterInvoke(context.Background(), "method1", func(_ context.Context, _ any) (any, error) {
			called = true

			return nil, nil
		})
		require.NoError(t, err)

		op, ok := inv.GetMethod("method1")
		require.True(t, ok)

		_, _ = op(context.Background(), nil)

		require.True(t, called)
	})

	t.Run("rejects blank method name", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		err := inv.RegisterInvoke(context.Background(), "", func(_ context.Context, _ any) (any, error) { return nil, nil })
		require.ErrorIs(t, err, ErrMethodNameBlank)

		_, ok := inv.GetMethod("")
		require.False(t, ok)
	})

	t.Run("rejects nil handler", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		err := inv.RegisterInvoke(context.Background(), "method2", nil)
		require.ErrorIs(t, err, ErrHandlerNil)

		_, ok := inv.GetMethod("method2")
		require.False(t, ok)
	})

	t.Run("does not overwrite an already registered method", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		err := inv.RegisterInvoke(context.Background(), "method3", func(_ context.Context, _ any) (any, error) { return "first", nil })
		require.NoError(t, err)

		err = inv.RegisterInvoke(context.Background(), "method3", func(_ context.Context, _ any) (any, error) { return "second", nil })
		require.ErrorIs(t, err, ErrMethodAlreadyRegistered)

		op, ok := inv.GetMethod("method3")
		require.True(t, ok)

		res, _ := op(context.Background(), nil)
		require.Equal(t, "first", res)
	})
}

func TestResetMethodsForTest(t *testing.T) {
	t.Run("clears and restores the method map", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		err := inv.RegisterInvoke(context.Background(), "m1", func(_ context.Context, _ any) (any, error) { return nil, nil })
		require.NoError(t, err)

		restore := inv.ResetMethodsForTest()

		_, ok := inv.GetMethod("m1")
		require.False(t, ok)

		restore()

		_, ok = inv.GetMethod("m1")
		require.True(t, ok)
	})
}

func TestInvoke(t *testing.T) {
	t.Run("redis resolution failure returns a failed response", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		inv := New(func() (Redis, error) { return nil, errors.New("no redis") }, log, &PublisherMock{}, &ListenerRegistryMock{}, func() string { return "GID_Test" })

		res := inv.Invoke(context.Background(), &Request{Group: "GID_Test", Method: "m"}, 1)

		require.False(t, res.Status)
	})

	t.Run("group not found returns a failed response", func(t *testing.T) {
		mockRedis := &RedisMock{
			GetFunc: func(_ context.Context, _ string) *redis.StringCmd {
				cmd := redis.NewStringCmd(context.Background())
				cmd.SetVal("")

				return cmd
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		res := inv.Invoke(context.Background(), &Request{Group: "GID_Test", Method: "m"}, 1)

		require.False(t, res.Status)
	})
}

func TestRegisterInternalListeners(t *testing.T) {
	t.Run("registers the invoke listener under the internal topic and tag", func(t *testing.T) {
		var registered mqtype.IMessageListener

		registry := &ListenerRegistryMock{
			RegisterListenerFunc: func(_ context.Context, l mqtype.IMessageListener) error {
				registered = l

				return nil
			},
		}

		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, registry, "GID_Test")

		err := inv.RegisterInternalListeners(context.Background())
		require.NoError(t, err)

		require.NotNil(t, registered)
		require.Equal(t, mqtype.TopicInternal, registered.GetTopic())
		require.Equal(t, mqtype.TagInvoke, registered.GetTag())
	})
}

func TestMessageInvokeListenerConsume(t *testing.T) {
	newListener := func(inv *Invoker) *messageInvokeListener {
		return &messageInvokeListener{invoker: inv}
	}

	t.Run("malformed body is committed without dispatch", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		listener := newListener(inv)

		action := listener.Consume(context.Background(), &mqtype.Message{Body: "not-json"})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
	})

	t.Run("request for another group is committed without dispatch", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "m", Group: "OtherGroup"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
	})

	t.Run("malformed request missing method or message id is committed", func(t *testing.T) {
		inv, _ := newTestInvoker(&RedisMock{}, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{Group: "GID_Test"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
	})

	t.Run("unknown method publishes a not-found response", func(t *testing.T) {
		var publishedPayload string

		mockRedis := &RedisMock{
			PublishFunc: func(_ context.Context, _ string, msg any) *redis.IntCmd {
				publishedPayload, _ = msg.(string)

				return redis.NewIntCmd(context.Background())
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "missing", Group: "GID_Test"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
		require.Contains(t, publishedPayload, "method not found")
	})

	t.Run("registered method success publishes response", func(t *testing.T) {
		var publishedPayload string

		mockRedis := &RedisMock{
			PublishFunc: func(_ context.Context, _ string, msg any) *redis.IntCmd {
				publishedPayload, _ = msg.(string)

				return redis.NewIntCmd(context.Background())
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")
		err := inv.RegisterInvoke(context.Background(), "echo", func(_ context.Context, request any) (any, error) {
			return request, nil
		})
		require.NoError(t, err)

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "echo", Group: "GID_Test", Request: "payload"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
		require.Contains(t, publishedPayload, "payload")
	})

	t.Run("method error publishes a failed response", func(t *testing.T) {
		var publishedPayload string

		mockRedis := &RedisMock{
			PublishFunc: func(_ context.Context, _ string, msg any) *redis.IntCmd {
				publishedPayload, _ = msg.(string)

				return redis.NewIntCmd(context.Background())
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")
		err := inv.RegisterInvoke(context.Background(), "boom", func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("business failure")
		})
		require.NoError(t, err)

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "boom", Group: "GID_Test"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
		require.Contains(t, publishedPayload, "business failure")
	})

	t.Run("method panic is recovered and published as a failure", func(t *testing.T) {
		var publishedPayload string

		mockRedis := &RedisMock{
			PublishFunc: func(_ context.Context, _ string, msg any) *redis.IntCmd {
				publishedPayload, _ = msg.(string)

				return redis.NewIntCmd(context.Background())
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")
		err := inv.RegisterInvoke(context.Background(), "panics", func(_ context.Context, _ any) (any, error) {
			panic("boom")
		})
		require.NoError(t, err)

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "panics", Group: "GID_Test"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
		require.Contains(t, publishedPayload, "boom")
	})

	t.Run("redis resolution failure is committed without dispatch", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		inv := New(func() (Redis, error) { return nil, errors.New("no redis") }, log, &PublisherMock{}, &ListenerRegistryMock{}, func() string { return "GID_Test" })

		listener := newListener(inv)

		body := jsonutil.MarshalString(&Request{MessageId: "id", Method: "m", Group: "GID_Test"})

		action := listener.Consume(context.Background(), &mqtype.Message{Body: body})

		require.Equal(t, mqtype.Action(mqtype.CommitMessage), action)
	})
}

func TestKeepAlive(t *testing.T) {
	t.Run("sets the invoke group key and stops on context cancellation", func(t *testing.T) {
		setCalled := make(chan struct{}, 1)

		mockRedis := &RedisMock{
			SetFunc: func(_ context.Context, key string, _ any, _ time.Duration) *redis.StatusCmd {
				require.Equal(t, "MessageInvokeGroup:GID_Test", key)

				setCalled <- struct{}{}

				return redis.NewStatusCmd(context.Background())
			},
		}

		inv, _ := newTestInvoker(mockRedis, &PublisherMock{}, &ListenerRegistryMock{}, "GID_Test")

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})

		go func() {
			inv.KeepAlive(ctx)
			close(done)
		}()

		select {
		case <-setCalled:
		case <-time.After(time.Second):
			t.Fatal("expected Set to be called")
		}

		cancel()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("expected KeepAlive to return after context cancellation")
		}
	})

	t.Run("redis resolution failure returns immediately", func(t *testing.T) {
		log, _ := newTestAttrLogger()
		inv := New(func() (Redis, error) { return nil, errors.New("no redis") }, log, &PublisherMock{}, &ListenerRegistryMock{}, func() string { return "GID_Test" })

		inv.KeepAlive(context.Background())
	})
}
