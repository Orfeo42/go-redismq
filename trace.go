package redismq

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

const traceIDKey = "traceId"

var (
	traceMu            sync.RWMutex
	traceIDFromContext = func(ctx context.Context) string { return "" }
	traceIDToContext   = func(ctx context.Context, traceID string) context.Context { return ctx }
)

func SetTraceIDFromContext(fn func(ctx context.Context) string) {
	if fn == nil {
		return
	}

	traceMu.Lock()
	traceIDFromContext = fn
	traceMu.Unlock()
}

func SetTraceIDToContext(fn func(ctx context.Context, traceID string) context.Context) {
	if fn == nil {
		return
	}

	traceMu.Lock()
	traceIDToContext = fn
	traceMu.Unlock()
}

func getTraceIDFromContext() func(ctx context.Context) string {
	traceMu.RLock()
	defer traceMu.RUnlock()

	return traceIDFromContext
}

func getTraceIDToContext() func(ctx context.Context, traceID string) context.Context {
	traceMu.RLock()
	defer traceMu.RUnlock()

	return traceIDToContext
}

func (message *Message) traceID() string {
	if value, ok := message.CustomData[traceIDKey].(string); ok {
		return value
	}

	return ""
}

func (message *Message) setTraceID(traceID string) {
	if message.CustomData == nil {
		message.CustomData = make(map[string]any)
	}

	message.CustomData[traceIDKey] = traceID
}

func stampTraceID(ctx context.Context, message *Message) {
	if message.traceID() != "" {
		return
	}

	traceID := getTraceIDFromContext()(ctx)
	if traceID == "" {
		return
	}

	message.setTraceID(traceID)
}

func consumeContext(ctx context.Context, message *Message) context.Context {
	traceID := message.traceID()
	if traceID == "" {
		traceID = uuid.New().String()
		message.setTraceID(traceID)
	}

	return getTraceIDToContext()(ctx, traceID)
}
