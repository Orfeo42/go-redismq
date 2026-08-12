package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorGray   = "\033[90m"
)

type Handler struct {
	handler    slog.Handler
	w          io.Writer
	opts       *slog.HandlerOptions
	goidCache  map[uint64]string
	cacheMutex sync.RWMutex
}

func NewHandler(w io.Writer, opts *slog.HandlerOptions) *Handler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}

	return &Handler{
		handler:   slog.NewTextHandler(w, opts),
		w:         w,
		opts:      opts,
		goidCache: make(map[uint64]string),
	}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{
		handler:   h.handler.WithAttrs(attrs),
		w:         h.w,
		opts:      h.opts,
		goidCache: h.goidCache,
	}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		handler:   h.handler.WithGroup(name),
		w:         h.w,
		opts:      h.opts,
		goidCache: h.goidCache,
	}
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	level := r.Level.String()
	color := getColorForLevel(r.Level)
	goid := getGoroutineID()

	var caller string

	showCaller := r.Level == slog.LevelDebug || r.Level == slog.LevelError

	if showCaller && h.opts.AddSource && r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		funcName := filepath.Base(f.Function)
		fileName := filepath.Base(f.File)
		caller = fmt.Sprintf("%s %s:%d", funcName, fileName, f.Line)
	}

	if caller != "" {
		fmt.Fprintf(h.w, "%s[%s]%s %s [thread-%d] [%s] %s",
			color,
			level,
			colorReset,
			r.Time.Format("2006-01-02 15:04:05"),
			goid,
			caller,
			r.Message,
		)
	} else {
		fmt.Fprintf(h.w, "%s[%s]%s %s [thread-%d] %s",
			color,
			level,
			colorReset,
			r.Time.Format("2006-01-02 15:04:05"),
			goid,
			r.Message,
		)
	}

	r.Attrs(func(a slog.Attr) bool {
		if a.Key != slog.SourceKey {
			fmt.Fprintf(h.w, " %s=%v", a.Key, a.Value)
		}

		return true
	})

	fmt.Fprintln(h.w)

	return nil
}

func getColorForLevel(level slog.Level) string {
	switch level {
	case slog.LevelDebug:
		return colorGray
	case slog.LevelInfo:
		return colorBlue
	case slog.LevelWarn:
		return colorYellow
	case slog.LevelError:
		return colorRed
	default:
		return colorReset
	}
}

func getGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]

	var goid uint64
	fmt.Sscanf(string(b), "goroutine %d ", &goid)

	return goid
}
