package redismq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRedisMQMessage(t *testing.T) {
	t.Run("builds a message from the topic wrapper and body", func(t *testing.T) {
		topic := MQTopicEnum{Topic: "t1", Tag: "tag1", Description: "desc"}

		message := NewRedisMQMessage(topic, "body")

		require.Equal(t, "t1", message.Topic)
		require.Equal(t, "tag1", message.Tag)
		require.Equal(t, "body", message.Body)
	})
}
