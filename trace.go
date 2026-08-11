package go_redismq

import (
	"context"

	"github.com/google/uuid"
)

const traceIDKey = "traceId"

var TraceIDFromContext = func(ctx context.Context) string { return "" }

var TraceIDToContext = func(ctx context.Context, traceID string) context.Context { return ctx }

func (message *Message) traceID() string {
	if value, ok := message.CustomData[traceIDKey].(string); ok {
		return value
	}

	return ""
}

func (message *Message) setTraceID(traceID string) {
	if message.CustomData == nil {
		message.CustomData = make(map[string]interface{})
	}

	message.CustomData[traceIDKey] = traceID
}

func stampTraceID(ctx context.Context, message *Message) {
	if message.traceID() != "" {
		return
	}

	traceID := TraceIDFromContext(ctx)
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

	return TraceIDToContext(ctx, traceID)
}
