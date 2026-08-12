package mqtype

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultConsumerDelayMilliSeconds = 1500

const traceIDKey = "traceId"

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

type streamMetadata struct {
	StartDeliverTime          int64          `json:"startDeliverTime"`
	ReconsumeTimes            int            `json:"reconsumeTimes"`
	ReconsumeMax              int            `json:"reconsumeMax"`
	CustomData                map[string]any `json:"customData"`
	Key                       string         `json:"key"`
	SendTime                  int64          `json:"sendTime"`
	ConsumerDelayMilliSeconds *int           `json:"consumerDelayMilliSeconds"`
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func NewRedisMQMessage(topicWrapper MQTopicEnum, body string) *Message {
	return &Message{
		Topic:    topicWrapper.Topic,
		Tag:      topicWrapper.Tag,
		Body:     body,
		SendTime: currentTimeMillis(),
	}
}

func (message *Message) GetUniqueKey() string {
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

func (message *Message) IsBroadcastingMessage() bool {
	value, ok := message.CustomData["messageModel"].(string)
	if !ok {
		return false
	}

	return value == "BROADCASTING"
}

func (message *Message) ToStreamAddArgsValues(stream string) (*redis.XAddArgs, error) {
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

	metaJson, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	var values = map[string]any{
		"topic":    message.Topic,
		"tag":      message.Tag,
		"body":     message.Body,
		"metadata": string(metaJson),
	}

	return &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}, nil
}

func (message *Message) decodeStreamMetadata(metadata string) error {
	var parsed streamMetadata

	if err := json.Unmarshal([]byte(metadata), &parsed); err != nil {
		return err
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

	message.GetUniqueKey()

	return nil
}

func (message *Message) PassStreamMessage(value map[string]any) (panicStack []string, err error) {
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
		return nil, nil
	}

	defer func() {
		exception := recover()
		if exception == nil {
			return
		}

		err = panicError(exception)
		panicStack = captureStack(2)
	}()

	return nil, message.decodeStreamMetadata(metadata)
}

func panicError(exception any) error {
	err, ok := exception.(error)
	if !ok {
		err = fmt.Errorf("redismq: panic: %v", exception)
	}

	return err
}

func captureStack(skip int) []string {
	var pcs [32]uintptr

	n := runtime.Callers(skip, pcs[:])

	frames := runtime.CallersFrames(pcs[:n])

	var stack []string

	for {
		frame, more := frames.Next()
		stack = append(stack, fmt.Sprintf("%s %s:%d", frame.Function, filepath.Base(frame.File), frame.Line))

		if !more {
			break
		}
	}

	return stack
}

func (message *Message) TraceID() string {
	if value, ok := message.CustomData[traceIDKey].(string); ok {
		return value
	}

	return ""
}

func (message *Message) SetTraceID(traceID string) {
	if message.CustomData == nil {
		message.CustomData = make(map[string]any)
	}

	message.CustomData[traceIDKey] = traceID
}
