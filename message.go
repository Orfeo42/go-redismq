package redismq

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const DefaultConsumerDelayMilliSeconds = 1500

type Message struct {
	MessageId                 string         `dc:"MessageId"                    json:"messageId"`
	Topic                     string         `dc:"Topic"                        json:"topic"`
	Tag                       string         `dc:"Tag"                          json:"tag"`
	Body                      string         `dc:"Body"                         json:"body"`
	Key                       string         `dc:"Key"                          json:"key"`
	StartDeliverTime          int64          `dc:"Send Time,0-No Delay, Second" json:"startDeliverTime"`
	ReconsumeTimes            int            `dc:"Reconsume Count"              json:"reconsumeTimes"`
	ReconsumeMax              int            `dc:"Reconsume Max Count"          json:"reconsumeMax"`
	CustomData                map[string]any `dc:"CustomData"                   json:"customData"`
	SendTime                  int64          `dc:"Sent Time"                    json:"sendTime"`
	ConsumerDelayMilliSeconds int            `dc:"Consumer Delay Milliseconds"  json:"consumerDelayMilliSeconds"`
}

type messageMetaData struct {
	StartDeliverTime          int64          `dc:"Send Time,0-No Delay, Second" json:"startDeliverTime"`
	ReconsumeTimes            int            `dc:"Reconsume Count"              json:"reconsumeTimes"`
	ReconsumeMax              int            `dc:"Reconsume Max Count"          json:"reconsumeMax"`
	CustomData                map[string]any `dc:"CustomData"                   json:"customData"`
	Key                       string         `dc:"Key"                          json:"key"`
	SendTime                  int64          `dc:"SendTime"                     json:"sendTime"`
	ConsumerDelayMilliSeconds int            `dc:"Consumer Delay Milliseconds"  json:"consumerDelayMilliSeconds"`
}

func NewRedisMQMessage(topicWrapper MQTopicEnum, body string) *Message {
	return &Message{
		Topic:    topicWrapper.Topic,
		Tag:      topicWrapper.Tag,
		Body:     body,
		SendTime: currentTimeMillis(),
	}
}

func (message *Message) getUniqueKey() string {
	if message.CustomData == nil {
		message.CustomData = make(map[string]any)
	}

	uniqueKey := ""
	if value, ok := message.CustomData["uniqueKey"].(string); ok && len(value) > 0 {
		uniqueKey = value
	}

	if len(uniqueKey) > 0 || len(message.MessageId) == 0 {
		return uniqueKey
	}

	message.CustomData["uniqueKey"] = message.MessageId

	return message.MessageId
}

func (message *Message) isBroadcastingMessage() bool {
	value, ok := message.CustomData["messageModel"].(string)
	if !ok {
		return false
	}

	return value == "BROADCASTING"
}

func (message *Message) toStreamAddArgsValues(stream string) *redis.XAddArgs {
	if message.ConsumerDelayMilliSeconds == 0 {
		message.ConsumerDelayMilliSeconds = DefaultConsumerDelayMilliSeconds
	}

	metadata := messageMetaData{
		StartDeliverTime:          message.StartDeliverTime,
		ReconsumeTimes:            message.ReconsumeTimes,
		CustomData:                message.CustomData,
		Key:                       message.Key,
		ConsumerDelayMilliSeconds: message.ConsumerDelayMilliSeconds,
		SendTime:                  currentTimeMillis(),
	}
	metaJson, _ := json.Marshal(metadata)

	var values = map[string]any{
		"topic":    message.Topic,
		"tag":      message.Tag,
		"body":     message.Body,
		"metadata": string(metaJson),
	}

	return &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}
}

func (message *Message) decodeStreamMetadata(ctx context.Context, metadata string) {
	var parsed streamMetadata

	if err := json.Unmarshal([]byte(metadata), &parsed); err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: stream metadata unmarshal failed", causeAttr(err), slog.String("message_id", message.MessageId))

		return
	}

	message.ReconsumeTimes = parsed.ReconsumeTimes
	message.ReconsumeMax = parsed.ReconsumeMax
	message.StartDeliverTime = parsed.StartDeliverTime
	message.SendTime = parsed.SendTime
	message.CustomData = parsed.CustomData
	message.Key = parsed.Key

	if parsed.ConsumerDelayMilliSeconds != nil {
		message.ConsumerDelayMilliSeconds = *parsed.ConsumerDelayMilliSeconds
	}

	message.getUniqueKey()
}

func (message *Message) passStreamMessage(ctx context.Context, value map[string]any) {
	if target, ok := value["topic"].(string); ok {
		message.Topic = target
	}

	if target, ok := value["tag"].(string); ok {
		message.Tag = target
	}

	if target, ok := value["body"].(string); ok {
		message.Body = target
	}

	metadata, ok := value["metadata"].(string)
	if !ok || len(metadata) == 0 {
		return
	}

	defer func() {
		exception := recover()
		if exception == nil {
			return
		}

		err := panicError(exception)
		logAttrs(ctx, slog.LevelError, "redismq: passStreamMessage panic recovered", causeAttr(err), stackAttr(2), slog.String("message_id", message.MessageId))
	}()

	message.decodeStreamMetadata(ctx, metadata)
}

type streamMetadata struct {
	StartDeliverTime          int64          `json:"startDeliverTime"`
	ReconsumeTimes            int            `json:"reconsumeTimes"`
	ReconsumeMax              int            `json:"reconsumeMax"`
	CustomData                map[string]any `json:"customData"`
	Key                       string         `json:"key"`
	SendTime                  int64          `json:"sendTime"`
	ConsumerDelayMilliSeconds *int           `json:"consumerDelayMilliSeconds"`
}
