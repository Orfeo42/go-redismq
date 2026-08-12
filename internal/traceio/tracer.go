package traceio

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

type Tracer struct {
	mu                 sync.RWMutex
	traceIDFromContext func(ctx context.Context) string
	traceIDToContext   func(ctx context.Context, traceID string) context.Context
}

func New() *Tracer {
	return &Tracer{
		traceIDFromContext: func(_ context.Context) string { return "" },
		traceIDToContext:   func(ctx context.Context, _ string) context.Context { return ctx },
	}
}

func (t *Tracer) SetTraceIDFromContext(fn func(ctx context.Context) string) {
	if fn == nil {
		return
	}

	t.mu.Lock()
	t.traceIDFromContext = fn
	t.mu.Unlock()
}

func (t *Tracer) SetTraceIDToContext(fn func(ctx context.Context, traceID string) context.Context) {
	if fn == nil {
		return
	}

	t.mu.Lock()
	t.traceIDToContext = fn
	t.mu.Unlock()
}

func (t *Tracer) TraceIDFromContext() func(ctx context.Context) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.traceIDFromContext
}

func (t *Tracer) TraceIDToContext() func(ctx context.Context, traceID string) context.Context {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.traceIDToContext
}

func (t *Tracer) StampTraceID(ctx context.Context, message *mqtype.Message) {
	if message.TraceID() != "" {
		return
	}

	traceID := t.TraceIDFromContext()(ctx)
	if traceID == "" {
		return
	}

	message.SetTraceID(traceID)
}

func (t *Tracer) ConsumeContext(ctx context.Context, message *mqtype.Message) context.Context {
	traceID := message.TraceID()
	if traceID == "" {
		traceID = uuid.New().String()
		message.SetTraceID(traceID)
	}

	return t.TraceIDToContext()(ctx, traceID)
}
