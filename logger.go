package redismq

import (
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/Orfeo42/go-redismq/v3/internal/logging"
)

type Logger = logging.Logger

type AttrLogger = logging.AttrLogger

func getLogLevelFromEnv() slog.Level {
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
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

func NewDefaultLogger() Logger {
	return logging.NewSlogAdapter(slog.New(logging.NewHandler(os.Stdout, &slog.HandlerOptions{
		Level:     getLogLevelFromEnv(),
		AddSource: true,
	})))
}
