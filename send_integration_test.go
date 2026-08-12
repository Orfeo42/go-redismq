package redismq

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testGroup = "GID_RedisMQ_Test1"

const testRedisAddr = "127.0.0.1:6379"

func requireRedis(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn, err := net.DialTimeout("tcp", testRedisAddr, 500*time.Millisecond)
	if err != nil {
		t.Skip("redis not reachable at 127.0.0.1:6379")
	}

	_ = conn.Close()
}

type stubListener struct {
	receiveCount atomic.Int64
}

func (l *stubListener) GetTopic() string {
	return "test"
}

func (l *stubListener) GetTag() string {
	return "test"
}

func (l *stubListener) Consume(_ context.Context, _ *Message) Action {
	l.receiveCount.Add(1)

	return CommitMessage
}

func closeClient(t *testing.T, client *Client) {
	t.Helper()

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = client.Close(closeCtx)
}

func TestProducerAndConsumer(t *testing.T) {
	requireRedis(t)

	client, err := New(RedisMqConfig{Group: testGroup, Addr: testRedisAddr})
	require.NoError(t, err)

	listener := &stubListener{}

	err = client.RegisterListener(context.Background(), listener)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = client.Start(ctx)
	require.NoError(t, err)

	defer closeClient(t, client)

	t.Run("Test Start RedisMQ", func(t *testing.T) {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				result, sendErr := client.Send(ctx, &Message{
					Topic: "test",
					Tag:   "test",
					Body:  "Test",
				})
				assert.NoError(t, sendErr, "error")
				assert.True(t, result)
				time.Sleep(1 * time.Second)
			}
		}()

		time.Sleep(5 * time.Second)
		require.Positive(t, listener.receiveCount.Load())
	})
}
