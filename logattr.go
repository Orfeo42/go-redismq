package redismq

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"

	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

func messageAttrs(m *Message) []slog.Attr {
	if m == nil {
		return nil
	}

	attrs := []slog.Attr{
		slog.String("topic", m.Topic),
		slog.String("tag", m.Tag),
		slog.String("message_id", m.MessageId),
		slog.Int("reconsume_times", m.ReconsumeTimes),
		slog.Int("reconsume_max", m.ReconsumeMax),
	}

	if m.Key != "" {
		attrs = append(attrs, slog.String("message_key", m.Key))
	}

	if traceID := m.traceID(); traceID != "" {
		attrs = append(attrs, slog.String("trace_id", traceID))
	}

	return attrs
}

func topicAttrs(topic string) []slog.Attr {
	return []slog.Attr{
		slog.String("topic", topic),
		slog.String("consumer_group", getGroup()),
		slog.String("stream", streamname.Queue(topic)),
	}
}

func causeAttr(err error) slog.Attr {
	return slog.Any("cause", err)
}

func stackAttr(skip int) slog.Attr {
	var pcs [32]uintptr

	n := runtime.Callers(skip, pcs[:])

	frames := runtime.CallersFrames(pcs[:n])

	var stack []string

	for {
		frame, more := frames.Next()
		stack = append(stack, fmt.Sprintf("%s %s:%d", frame.Function, filepath.Base(frame.File), frame.Line))

		if !more {
			break
		}
	}

	return slog.Any("stack", stack)
}
