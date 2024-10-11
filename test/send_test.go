package test

import (
	"context"
	"github.com/gogf/gf/v2/frame/g"
	goredismq "github.com/jackyang-hk/go-redismq"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var receiveCount = 0

type TestListener struct {
}

func (t TestListener) GetTopic() string {
	return "test"
}

func (t TestListener) GetTag() string {
	return "test"
}

func (t TestListener) Consume(ctx context.Context, message *goredismq.Message) goredismq.Action {
	receiveCount = receiveCount + 1
	g.Log().Infof(ctx, "Receive Message %d:%s", receiveCount, goredismq.MarshalToJsonString(message))
	return goredismq.CommitMessage
}

func TestProducerAndConsumer(t *testing.T) {
	goredismq.RegisterRedisMqConfig(&goredismq.RedisMqConfig{
		Group:    TestGroup,
		Addr:     "127.0.0.1:6379",
		Password: "",
		Database: 0,
	})
	goredismq.RegisterListener(&TestListener{})
	goredismq.StartRedisMqConsumer()
	t.Run("Test Start RedisMQ", func(t *testing.T) {
		go func() {
			for {
				result, err := goredismq.Send(&goredismq.Message{
					Topic: "test",
					Tag:   "test",
					Body:  "Test",
				})
				require.Nil(t, err, "error")
				require.Equal(t, result, true)
				time.Sleep(1 * time.Second)
			}
		}()

		time.Sleep(5 * time.Second)
		require.Equal(t, receiveCount > 0, true)
	})
}
