package redismq

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
)

type Clock interface {
	Now() time.Time
}

type settings struct {
	logger             Logger
	traceIDFromContext func(ctx context.Context) string
	traceIDToContext   func(ctx context.Context, traceID string) context.Context
	clock              Clock
	streamMaxLen       int64
}

const defaultStreamMaxLen = 10000

func newSettings() *settings {
	return &settings{streamMaxLen: defaultStreamMaxLen}
}

type Option func(*settings)

func WithLogger(l Logger) Option {
	return func(s *settings) {
		if l == nil {
			return
		}

		s.logger = l
	}
}

func WithSlogLogger(l *slog.Logger) Option {
	return func(s *settings) {
		if l == nil {
			return
		}

		s.logger = logging.NewSlogAdapter(l)
	}
}

func WithStdLogger(l *log.Logger) Option {
	return func(s *settings) {
		if l == nil {
			return
		}

		s.logger = &stdLogger{logger: l}
	}
}

func WithTraceIDFromContext(fn func(ctx context.Context) string) Option {
	return func(s *settings) {
		if fn == nil {
			return
		}

		s.traceIDFromContext = fn
	}
}

func WithTraceIDToContext(fn func(ctx context.Context, traceID string) context.Context) Option {
	return func(s *settings) {
		if fn == nil {
			return
		}

		s.traceIDToContext = fn
	}
}

func WithClock(c Clock) Option {
	return func(s *settings) {
		if c == nil {
			return
		}

		s.clock = c
	}
}

func WithStreamMaxLen(n int64) Option {
	return func(s *settings) {
		if n <= 0 {
			return
		}

		s.streamMaxLen = n
	}
}
