package redismq

import (
	"bytes"
	"context"
	"log"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeLogger struct {
	calls []string
}

func (f *fakeLogger) Debugf(_ string, _ ...any) { f.calls = append(f.calls, "debug") }

func (f *fakeLogger) Infof(_ string, _ ...any) { f.calls = append(f.calls, "info") }

func (f *fakeLogger) Warnf(_ string, _ ...any) { f.calls = append(f.calls, "warn") }

func (f *fakeLogger) Errorf(_ string, _ ...any) { f.calls = append(f.calls, "error") }

func TestWithLoggerOption(t *testing.T) {
	t.Run("client logging reaches the injected Logger", func(t *testing.T) {
		fl := &fakeLogger{}
		client := newTestClient(t, WithLogger(fl))

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := client.Close(closeCtx)
		require.NoError(t, err)

		require.NotEmpty(t, fl.calls)
	})

	t.Run("nil logger is ignored", func(t *testing.T) {
		client := newTestClient(t, WithLogger(nil))
		require.NotNil(t, client)
	})
}

func TestWithStdLoggerOption(t *testing.T) {
	t.Run("client logging reaches the injected *log.Logger", func(t *testing.T) {
		var buf bytes.Buffer

		client := newTestClient(t, WithStdLogger(log.New(&buf, "", 0)))

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := client.Close(closeCtx)
		require.NoError(t, err)

		require.NotEmpty(t, buf.String())
	})

	t.Run("nil logger is ignored", func(t *testing.T) {
		client := newTestClient(t, WithStdLogger(nil))
		require.NotNil(t, client)
	})
}

func TestWithTraceIDToContextOption(t *testing.T) {
	t.Run("consumer context carries the trace id through the hook", func(t *testing.T) {
		type ctxKey struct{}

		client := newTestClient(t, WithTraceIDToContext(func(ctx context.Context, traceID string) context.Context {
			return context.WithValue(ctx, ctxKey{}, traceID)
		}))

		require.NotNil(t, client)
	})

	t.Run("nil hook is ignored", func(t *testing.T) {
		client := newTestClient(t, WithTraceIDToContext(nil))
		require.NotNil(t, client)
	})
}

func TestWithClockOptionIgnoresNil(t *testing.T) {
	t.Run("nil clock is ignored", func(t *testing.T) {
		client := newTestClient(t, WithClock(nil))
		require.NotNil(t, client)
	})
}

func TestWithTraceIDFromContextOptionIgnoresNil(t *testing.T) {
	t.Run("nil hook is ignored", func(t *testing.T) {
		client := newTestClient(t, WithTraceIDFromContext(nil))
		require.NotNil(t, client)
	})
}

func TestWithSlogLoggerOptionIgnoresNil(t *testing.T) {
	t.Run("nil logger is ignored", func(t *testing.T) {
		client := newTestClient(t, WithSlogLogger(nil))
		require.NotNil(t, client)
	})
}

func TestGetLogLevelFromEnv(t *testing.T) {
	t.Run("DEBUG maps to slog.LevelDebug", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "DEBUG")
		require.Equal(t, slog.LevelDebug, getLogLevelFromEnv())
	})

	t.Run("INFO maps to slog.LevelInfo", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "INFO")
		require.Equal(t, slog.LevelInfo, getLogLevelFromEnv())
	})

	t.Run("WARN maps to slog.LevelWarn", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "WARN")
		require.Equal(t, slog.LevelWarn, getLogLevelFromEnv())
	})

	t.Run("ERROR maps to slog.LevelError", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "ERROR")
		require.Equal(t, slog.LevelError, getLogLevelFromEnv())
	})

	t.Run("unset defaults to slog.LevelInfo", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "")
		require.Equal(t, slog.LevelInfo, getLogLevelFromEnv())
	})
}

func TestNewDefaultLogger(t *testing.T) {
	t.Run("returns a usable Logger", func(t *testing.T) {
		logger := NewDefaultLogger()
		require.NotNil(t, logger)
	})
}
