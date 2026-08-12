package redismq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSendDelay(t *testing.T) {
	t.Run("stamps the trace id and attempts the delayed send", func(t *testing.T) {
		client := newTestClient(t, WithTraceIDFromContext(func(_ context.Context) string { return "trace-delay" }))

		message := &Message{Topic: "t", Tag: "tag1"}

		_, _ = client.SendDelay(context.Background(), message, 5)

		require.Equal(t, "trace-delay", message.TraceID())
	})
}
