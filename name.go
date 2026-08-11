package go_redismq

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	redisMQQueueKeyPrefix           = "MQ_QUEUE_LIST_"
	redisMQBackupQueueKeyPrefix     = "MQ_BACKUP_QUEUE_LIST_"
	streamName                      = "STREAM_"
	nameVersion                     = "_V3"
	redisMQTransactionPreQueueKey   = "MQ_TRANSACTION_PRE_QUEUE_LIST_"
	redisMQDeathQueueKey            = "MQ_DEATH_QUEUE_LIST"
	redisMQTransactionDeathQueueKey = "MQ_TRANSACTION_DEATH_QUEUE_LIST_"
)

func GetQueueName(topic string) string {
	return fmt.Sprintf("%s%s%s%s", redisMQQueueKeyPrefix, streamName, topic, nameVersion)
}

func getBackupQueueName(topic string) string {
	return fmt.Sprintf("%s%s%s%s", redisMQBackupQueueKeyPrefix, streamName, topic, nameVersion)
}

func GenerateUniqueNo(districtCode string) string {
	uuid := uuid.New()

	return fmt.Sprintf("MQCT_%s_%s", districtCode, strings.Replace(uuid.String(), "-", "", -1))
}

func GetTransactionPrepareQueueName(topic string) string {
	return fmt.Sprintf("%s%s%s", redisMQTransactionPreQueueKey, topic, nameVersion)
}

func GetDeathQueueName() string {
	return fmt.Sprintf("%s%sdeath_message%s", redisMQDeathQueueKey, streamName, nameVersion)
}

func GetMessageKey(topic string, tag string) string {
	return fmt.Sprintf("%s_%s", topic, tag)
}

func getTransactionDeathQueueName() string {
	return fmt.Sprintf("%s%s", redisMQTransactionDeathQueueKey, nameVersion)
}
