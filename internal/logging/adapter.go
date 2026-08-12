package logging

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"time"
)

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type AttrLogger interface {
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

const DirectCallerSkip = 2

type Adapter struct {
	hostLogger Logger
	attrLogger AttrLogger
	slogLogger *slog.Logger
}

func NewAdapter(l Logger) *Adapter {
	if a, ok := l.(*Adapter); ok {
		return a
	}

	a := &Adapter{hostLogger: l}

	if al, ok := l.(AttrLogger); ok {
		a.attrLogger = al
	}

	return a
}

func NewSlogAdapter(l *slog.Logger) *Adapter {
	return &Adapter{slogLogger: l}
}

func (a *Adapter) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	var pcs [1]uintptr

	runtime.Callers(DirectCallerSkip, pcs[:])

	a.LogRecord(ctx, level, msg, pcs[0], attrs)
}

func (a *Adapter) LogRecord(ctx context.Context, level slog.Level, msg string, pc uintptr, attrs []slog.Attr) {
	if a.slogLogger != nil {
		if !a.slogLogger.Enabled(ctx, level) {
			return
		}

		record := slog.NewRecord(time.Now(), level, msg, pc)
		record.AddAttrs(attrs...)

		_ = a.slogLogger.Handler().Handle(ctx, record)

		return
	}

	if a.attrLogger != nil {
		a.attrLogger.LogAttrs(ctx, level, msg, attrs...)

		return
	}

	if a.hostLogger != nil {
		fallbackPrintf(a.hostLogger, level, msg, attrs)
	}
}

func fallbackPrintf(l Logger, level slog.Level, msg string, attrs []slog.Attr) {
	var b strings.Builder

	b.WriteString(msg)

	for _, attr := range attrs {
		b.WriteString(" ")
		b.WriteString(attr.Key)
		b.WriteString("=")
		b.WriteString(attr.Value.String())
	}

	line := b.String()

	switch level {
	case slog.LevelDebug:
		l.Debugf("%s", line)
	case slog.LevelInfo:
		l.Infof("%s", line)
	case slog.LevelWarn:
		l.Warnf("%s", line)
	default:
		l.Errorf("%s", line)
	}
}
