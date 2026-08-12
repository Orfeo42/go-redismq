package redismq

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type raceTestListener struct {
	topic string
	tag   string
}

func (l *raceTestListener) GetTopic() string { return l.topic }

func (l *raceTestListener) GetTag() string { return l.tag }

func (l *raceTestListener) Consume(_ context.Context, _ *Message) Action { return CommitMessage }

type raceTestChecker struct {
	topic string
	tag   string
}

func (c *raceTestChecker) GetTopic() string { return c.topic }

func (c *raceTestChecker) GetTag() string { return c.tag }

func (c *raceTestChecker) Checker(_ *Message) TransactionStatus { return CommitTransaction }

func newRaceTestClient(t *testing.T, group string) *Client {
	t.Helper()

	client, err := New(RedisMqConfig{Group: group, Addr: "127.0.0.1:6379"})
	require.NoError(t, err)

	return client
}

func TestConcurrentClientRegistration(t *testing.T) {
	t.Run("RegisterListener, RegisterChecker and RegisterInvoke run concurrently without racing", func(t *testing.T) {
		client := newRaceTestClient(t, "GID_Race_Test")

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 3)

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				_ = client.RegisterListener(context.Background(), &raceTestListener{topic: fmt.Sprintf("topic-%d", i), tag: "tag"})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				_ = client.RegisterChecker(context.Background(), &raceTestChecker{topic: fmt.Sprintf("topic-%d", i), tag: "tag"})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				method := fmt.Sprintf("method-%d", i)
				_ = client.RegisterInvoke(context.Background(), method, func(_ context.Context, request any) (any, error) {
					return request, nil
				})
			}(i)
		}

		wg.Wait()
	})
}

func TestClientsDoNotShareState(t *testing.T) {
	t.Run("two independent Clients register the same topic and tag without conflict", func(t *testing.T) {
		clientA := newRaceTestClient(t, "GID_A")
		clientB := newRaceTestClient(t, "GID_B")

		err := clientA.RegisterListener(context.Background(), &raceTestListener{topic: "shared-topic", tag: "tag"})
		require.NoError(t, err)

		err = clientB.RegisterListener(context.Background(), &raceTestListener{topic: "shared-topic", tag: "tag"})
		require.NoError(t, err, "expected client B to register independently of client A")

		err = clientA.RegisterListener(context.Background(), &raceTestListener{topic: "shared-topic", tag: "tag"})
		require.ErrorIs(t, err, ErrDuplicateListener, "expected client A's own duplicate registration to be rejected")
	})
}
