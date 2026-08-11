package redismq

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
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

func getLogLevelFromEnv() slog.Level {
	levelStr := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	switch levelStr {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type slogLogger struct {
	logger *slog.Logger
}

func (l *slogLogger) Debugf(format string, args ...any) {
	l.logger.Debug(fmt.Sprintf(format, args...))
}

func (l *slogLogger) Infof(format string, args ...any) {
	l.logger.Info(fmt.Sprintf(format, args...))
}

func (l *slogLogger) Warnf(format string, args ...any) {
	l.logger.Warn(fmt.Sprintf(format, args...))
}

func (l *slogLogger) Errorf(format string, args ...any) {
	l.logger.Error(fmt.Sprintf(format, args...))
}

const callerSkip = 3

func (l *slogLogger) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if !l.logger.Enabled(ctx, level) {
		return
	}

	var pcs [1]uintptr
	runtime.Callers(callerSkip, pcs[:])

	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.AddAttrs(attrs...)

	_ = l.logger.Handler().Handle(ctx, record)
}

type stdLogger struct {
	logger *log.Logger
}

func (l *stdLogger) Debugf(format string, args ...any) {
	l.logger.Printf("[DEBUG] "+format, args...)
}

func (l *stdLogger) Infof(format string, args ...any) {
	l.logger.Printf("[INFO] "+format, args...)
}

func (l *stdLogger) Warnf(format string, args ...any) {
	l.logger.Printf("[WARN] "+format, args...)
}

func (l *stdLogger) Errorf(format string, args ...any) {
	l.logger.Printf("[ERROR] "+format, args...)
}

var (
	loggerMu sync.RWMutex
	logger   Logger
)

func NewDefaultLogger() Logger {
	return &slogLogger{logger: slog.New(logging.NewHandler(os.Stdout, &slog.HandlerOptions{
		Level:     getLogLevelFromEnv(),
		AddSource: true,
	}))}
}

func logAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	loggerMu.RLock()

	l := logger

	loggerMu.RUnlock()

	if l == nil {
		return
	}

	if al, ok := l.(AttrLogger); ok {
		al.LogAttrs(ctx, level, msg, attrs...)

		return
	}

	fallbackPrintf(l, level, msg, attrs)
}

func fallbackPrintf(l Logger, level slog.Level, msg string, attrs []slog.Attr) {
	var b strings.Builder

	b.WriteString(msg)

	for _, a := range attrs {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
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

func SetLogger(l Logger) {
	if l == nil {
		return
	}

	loggerMu.Lock()
	logger = l
	loggerMu.Unlock()
}

func SetStdLogger(l *log.Logger) {
	if l == nil {
		return
	}

	loggerMu.Lock()
	logger = &stdLogger{logger: l}
	loggerMu.Unlock()
}

func SetSlogLogger(l *slog.Logger) {
	if l == nil {
		return
	}

	loggerMu.Lock()
	logger = &slogLogger{logger: l}
	loggerMu.Unlock()
}

func GetLogger() Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()

	return logger
}
