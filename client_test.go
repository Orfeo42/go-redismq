package redismq

import (
	"context"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testAddr = "127.0.0.1:6379"

func newTestClient(t *testing.T, opts ...Option) *Client {
	t.Helper()

	client, err := New(RedisMqConfig{Group: "GID_Test", Addr: testAddr}, opts...)
	require.NoError(t, err)

	return client
}

func TestNewValidatesConfig(t *testing.T) {
	t.Run("blank addr returns ErrConfigAddrBlank", func(t *testing.T) {
		_, err := New(RedisMqConfig{Group: "GID_Test"})
		require.ErrorIs(t, err, ErrConfigAddrBlank)
	})

	t.Run("blank group returns ErrConfigGroupBlank", func(t *testing.T) {
		_, err := New(RedisMqConfig{Addr: testAddr})
		require.ErrorIs(t, err, ErrConfigGroupBlank)
	})

	t.Run("valid config constructs a client", func(t *testing.T) {
		client, err := New(RedisMqConfig{Group: "GID_Test", Addr: testAddr})
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

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

func TestWithSlogLoggerOption(t *testing.T) {
	t.Run("client logging reaches the injected slog logger", func(t *testing.T) {
		handler := &captureHandler{}
		client := newTestClient(t, WithSlogLogger(slog.New(handler)))

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := client.Close(closeCtx)
		require.NoError(t, err)

		require.NotEmpty(t, handler.records)
	})

	t.Run("pin: Client log source resolves to client.go, not adapter.go or logger.go", func(t *testing.T) {
		handler := &captureHandler{}
		client := newTestClient(t, WithSlogLogger(slog.New(handler)))

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := client.Close(closeCtx)
		require.NoError(t, err)

		require.NotEmpty(t, handler.records)

		frames := runtime.CallersFrames([]uintptr{handler.records[len(handler.records)-1].PC})
		frame, _ := frames.Next()
		file := filepath.Base(frame.File)

		require.Equal(t, "client.go", file)
	})
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func TestWithClockOption(t *testing.T) {
	t.Run("producer stamps SendTime from the injected clock", func(t *testing.T) {
		client := newTestClient(t, WithClock(fixedClock{now: time.Unix(1234, 0)}))

		message := &Message{Topic: "t", Tag: "tag1"}
		_, _ = client.Send(context.Background(), message)

		require.Equal(t, int64(1234000), message.SendTime)
	})
}

func TestWithTraceIDFromContextOption(t *testing.T) {
	t.Run("producer stamps the trace id resolved from the option hook", func(t *testing.T) {
		client := newTestClient(t, WithTraceIDFromContext(func(_ context.Context) string { return "trace-abc" }))

		message := &Message{Topic: "t", Tag: "tag1"}
		_, _ = client.Send(context.Background(), message)

		require.Equal(t, "trace-abc", message.TraceID())
	})
}

func TestCloseWithoutStart(t *testing.T) {
	t.Run("Close is safe to call when Start was never invoked", func(t *testing.T) {
		client := newTestClient(t)

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := client.Close(closeCtx)
		require.NoError(t, err)
	})
}

func TestCloseReturnsCtxErrorOnTimeout(t *testing.T) {
	t.Run("Close returns the context error when in-flight work outlives the deadline", func(t *testing.T) {
		client := newTestClient(t)

		client.wg.Add(1)
		defer client.wg.Done()

		closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := client.Close(closeCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRegisterListenerErrors(t *testing.T) {
	t.Run("nil listener returns ErrNilListener", func(t *testing.T) {
		client := newTestClient(t)

		err := client.RegisterListener(context.Background(), nil)
		require.ErrorIs(t, err, ErrNilListener)
	})

	t.Run("duplicate listener returns ErrDuplicateListener", func(t *testing.T) {
		client := newTestClient(t)

		listener := &raceTestListener{topic: "t1", tag: "tag1"}

		err := client.RegisterListener(context.Background(), listener)
		require.NoError(t, err)

		err = client.RegisterListener(context.Background(), listener)
		require.ErrorIs(t, err, ErrDuplicateListener)
	})
}

func TestRegisterCheckerErrors(t *testing.T) {
	t.Run("nil checker returns ErrNilChecker", func(t *testing.T) {
		client := newTestClient(t)

		err := client.RegisterChecker(context.Background(), nil)
		require.ErrorIs(t, err, ErrNilChecker)
	})

	t.Run("duplicate checker returns ErrDuplicateChecker", func(t *testing.T) {
		client := newTestClient(t)

		checker := &raceTestChecker{topic: "t1", tag: "tag1"}

		err := client.RegisterChecker(context.Background(), checker)
		require.NoError(t, err)

		err = client.RegisterChecker(context.Background(), checker)
		require.ErrorIs(t, err, ErrDuplicateChecker)
	})
}

func TestRegisterInvokeErrors(t *testing.T) {
	t.Run("blank method name returns ErrMethodNameBlank", func(t *testing.T) {
		client := newTestClient(t)

		err := client.RegisterInvoke(context.Background(), "", func(_ context.Context, request any) (any, error) { return request, nil })
		require.ErrorIs(t, err, ErrMethodNameBlank)
	})

	t.Run("nil handler returns ErrHandlerNil", func(t *testing.T) {
		client := newTestClient(t)

		err := client.RegisterInvoke(context.Background(), "method1", nil)
		require.ErrorIs(t, err, ErrHandlerNil)
	})

	t.Run("duplicate method returns ErrMethodAlreadyRegistered", func(t *testing.T) {
		client := newTestClient(t)

		op := func(_ context.Context, request any) (any, error) { return request, nil }

		err := client.RegisterInvoke(context.Background(), "method1", op)
		require.NoError(t, err)

		err = client.RegisterInvoke(context.Background(), "method1", op)
		require.ErrorIs(t, err, ErrMethodAlreadyRegistered)
	})
}

func TestErrorsAreStableSentinels(t *testing.T) {
	t.Run("re-exported errors keep their identity across errors.Is", func(t *testing.T) {
		var wrapped error = &wrappedErr{cause: ErrDuplicateListener}

		require.ErrorIs(t, wrapped, ErrDuplicateListener)
	})
}

type wrappedErr struct {
	cause error
}

func (e *wrappedErr) Error() string { return "wrapped: " + e.cause.Error() }

func (e *wrappedErr) Unwrap() error { return e.cause }

func TestErrConsumerNameUnresolvedIsExported(t *testing.T) {
	t.Run("re-exported for Start callers to check via errors.Is", func(t *testing.T) {
		require.Error(t, ErrConsumerNameUnresolved)
	})
}
