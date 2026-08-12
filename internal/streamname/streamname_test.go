package streamname

import "testing"

func TestQueue(t *testing.T) {
	t.Run("formats queue name for topic", func(t *testing.T) {
		got := Queue("orders")

		want := "MQ_QUEUE_LIST_STREAM_orders_V3"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestBackupQueue(t *testing.T) {
	t.Run("formats backup queue name for topic", func(t *testing.T) {
		got := BackupQueue("orders")

		want := "MQ_BACKUP_QUEUE_LIST_STREAM_orders_V3"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestTransactionPrepareQueue(t *testing.T) {
	t.Run("formats transaction prepare queue name for topic", func(t *testing.T) {
		got := TransactionPrepareQueue("orders")

		want := "MQ_TRANSACTION_PRE_QUEUE_LIST_orders_V3"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestDeathQueue(t *testing.T) {
	t.Run("formats death queue name", func(t *testing.T) {
		got := DeathQueue()

		want := "MQ_DEATH_QUEUE_LISTSTREAM_death_message_V3"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestMessageKey(t *testing.T) {
	t.Run("formats topic and tag key", func(t *testing.T) {
		got := MessageKey("orders", "created")

		want := "orders_created"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestTransactionDeathQueue(t *testing.T) {
	t.Run("formats transaction death queue name", func(t *testing.T) {
		got := TransactionDeathQueue()

		want := "MQ_TRANSACTION_DEATH_QUEUE_LIST__V3"
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}
