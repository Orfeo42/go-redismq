package logging

import (
	"context"
	"log/slog"
	"runtime"
)

type Bridge struct {
	resolve func() Logger
}

func NewBridge(resolve func() Logger) *Bridge {
	return &Bridge{resolve: resolve}
}

func (b *Bridge) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	var pcs [1]uintptr

	runtime.Callers(DirectCallerSkip, pcs[:])

	l := b.resolve()
	if l == nil {
		return
	}

	NewAdapter(l).LogRecord(ctx, level, msg, pcs[0], attrs)
}
