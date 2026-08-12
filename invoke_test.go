package redismq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvokeUnreachableRedis(t *testing.T) {
	t.Run("returns a failed response instead of blocking", func(t *testing.T) {
		client := newTestClient(t)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		res := client.Invoke(ctx, &InvokeRequest{Group: "GID_Test", Method: "m", Request: 1}, 1)

		require.NotNil(t, res)
		require.False(t, res.Status)
	})
}
