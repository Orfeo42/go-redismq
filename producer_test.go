package redismq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSendValidation(t *testing.T) {
	t.Run("blank tag returns ErrBlankTag", func(t *testing.T) {
		client := newTestClient(t)

		_, err := client.Send(context.Background(), &Message{Topic: "t", Tag: TagBlank})
		require.ErrorIs(t, err, ErrBlankTag)
	})

	t.Run("non-blank message id returns ErrMessageIDNotBlank", func(t *testing.T) {
		client := newTestClient(t)

		_, err := client.Send(context.Background(), &Message{Topic: "t", Tag: "tag1", MessageId: "already-set"})
		require.ErrorIs(t, err, ErrMessageIDNotBlank)
	})
}

func TestSendTransactionValidation(t *testing.T) {
	t.Run("blank tag returns ErrBlankTag", func(t *testing.T) {
		client := newTestClient(t)

		_, err := client.SendTransaction(context.Background(), &Message{Topic: "t", Tag: TagBlank}, func(_ *Message) (TransactionStatus, error) {
			return CommitTransaction, nil
		})
		require.ErrorIs(t, err, ErrBlankTag)
	})

	t.Run("delayed message returns ErrDelayNotSupportedInTransaction", func(t *testing.T) {
		client := newTestClient(t)

		_, err := client.SendTransaction(context.Background(), &Message{Topic: "t", Tag: "tag1", StartDeliverTime: 1}, func(_ *Message) (TransactionStatus, error) {
			return CommitTransaction, nil
		})
		require.ErrorIs(t, err, ErrDelayNotSupportedInTransaction)
	})
}
