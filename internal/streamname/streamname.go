package streamname

import "fmt"

const (
	queueKeyPrefix           = "MQ_QUEUE_LIST_"
	backupQueueKeyPrefix     = "MQ_BACKUP_QUEUE_LIST_"
	streamPrefix             = "STREAM_"
	version                  = "_V3"
	transactionPreQueueKey   = "MQ_TRANSACTION_PRE_QUEUE_LIST_"
	deathQueueKey            = "MQ_DEATH_QUEUE_LIST"
	transactionDeathQueueKey = "MQ_TRANSACTION_DEATH_QUEUE_LIST_"
)

func Queue(topic string) string {
	return fmt.Sprintf("%s%s%s%s", queueKeyPrefix, streamPrefix, topic, version)
}

func BackupQueue(topic string) string {
	return fmt.Sprintf("%s%s%s%s", backupQueueKeyPrefix, streamPrefix, topic, version)
}

func TransactionPrepareQueue(topic string) string {
	return fmt.Sprintf("%s%s%s", transactionPreQueueKey, topic, version)
}

func DeathQueue() string {
	return fmt.Sprintf("%s%sdeath_message%s", deathQueueKey, streamPrefix, version)
}

func MessageKey(topic string, tag string) string {
	return fmt.Sprintf("%s_%s", topic, tag)
}

func TransactionDeathQueue() string {
	return fmt.Sprintf("%s%s", transactionDeathQueueKey, version)
}
