package redismq

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
)

const testGroup = "GID_RedisMQ_Test1"

func requireRedis(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn, err := net.DialTimeout("tcp", "127.0.0.1:6379", 500*time.Millisecond)
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

func (l *stubListener) Consume(ctx context.Context, message *Message) Action {
	count := l.receiveCount.Add(1)
	logAttrs(ctx, slog.LevelInfo, "redismq: message received", slog.Int64("receive_count", count), slog.String("message", jsonutil.MarshalString(message)))

	return CommitMessage
}

func TestProducerAndConsumer(t *testing.T) {
	requireRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	savedGroup, savedAddr, savedPassword, savedDatabase := snapshotConfig()
	restoreConfig(t, savedGroup, savedAddr, savedPassword, savedDatabase)

	listenerMu.Lock()
	originalListeners := listeners
	originalTopics := topics
	listenerMu.Unlock()

	t.Cleanup(func() {
		listenerMu.Lock()
		listeners = originalListeners
		topics = originalTopics
		listenerMu.Unlock()
	})

	err := RegisterRedisMqConfig(context.Background(), &RedisMqConfig{
		Group:    testGroup,
		Addr:     "127.0.0.1:6379",
		Password: "",
		Database: 0,
	})
	require.NoError(t, err)

	listener := &stubListener{}
	RegisterListener(ctx, listener)
	StartRedisMqConsumer(ctx)

	t.Run("Test Start RedisMQ", func(t *testing.T) {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				result, err := Send(ctx, &Message{
					Topic: "test",
					Tag:   "test",
					Body:  "Test",
				})
				assert.NoError(t, err, "error")
				assert.True(t, result)
				time.Sleep(1 * time.Second)
			}
		}()

		time.Sleep(5 * time.Second)
		require.Positive(t, listener.receiveCount.Load())
	})
}
