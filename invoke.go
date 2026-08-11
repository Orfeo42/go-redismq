package go_redismq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type InvoiceRequest struct {
	MessageId string      `json:"messageId"`
	Group     string      `json:"group"`
	Method    string      `json:"method"`
	Request   interface{} `json:"request"`
}

type InvoiceResponse struct {
	Status   bool        `json:"status"`
	Response interface{} `json:"response"`
}

func listenForResponse(ctx context.Context, req *InvoiceRequest, responseChan chan *InvoiceResponse) {
	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	client := redis.NewClient(options)

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	replyChannel := getReplyChannel(req)

	pubSub := client.Subscribe(ctx, replyChannel)
	defer func(pubSub *redis.PubSub) {
		err := pubSub.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: pubsub close failed", causeAttr(err))
		}
	}(pubSub)

	ch := pubSub.Channel()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				logAttrs(ctx, slog.LevelInfo, "redismq: invoke response subscription channel closed", slog.String("reply_channel", replyChannel))

				return
			}

			var res *InvoiceResponse

			err := json.Unmarshal([]byte(msg.Payload), &res)
			if err != nil {
				logAttrs(ctx, slog.LevelWarn, "redismq: invoke response deserialization failed", causeAttr(err), slog.String("reply_channel", replyChannel))

				return
			}

			responseChan <- res

			return
		case <-ctx.Done():
			logAttrs(ctx, slog.LevelInfo, "redismq: invoke wait cancelled or timed out", slog.String("reply_channel", replyChannel))

			return
		}
	}
}

func Invoke(ctx context.Context, req *InvoiceRequest, timeoutSeconds int) *InvoiceResponse {
	startTime := time.Now()

	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}

	invokeId := fmt.Sprintf("%s%d", GenerateRandomAlphanumeric(6), CurrentTimeMillis())
	req.MessageId = invokeId

	options, err := GetRedisConfig()
	if err != nil {
		return &InvoiceResponse{Status: false, Response: err.Error()}
	}

	client := redis.NewClient(options)

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	data, err := client.Get(ctx, "MessageInvokeGroup:"+req.Group).Result()
	if err != nil {
		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke get group:" + err.Error(),
		}
	}

	if len(data) == 0 {
		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke Group Not Found:" + req.Group,
		}
	}

	responseChan := make(chan *InvoiceResponse)
	go listenForResponse(ctx, req, responseChan)

	send, err := Send(ctx, &Message{
		Topic: TopicInternal,
		Tag:   TagInvoke,
		Body:  MarshalToJsonString(req),
	})
	if err != nil {
		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke error:" + err.Error(),
		}
	} else if !send {
		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke send failed",
		}
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: invoke request published", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

	go func() {
		time.Sleep(time.Duration(timeoutSeconds) * time.Second)

		select {
		case <-ctx.Done():
			return
		case responseChan <- &InvoiceResponse{
			Status:   false,
			Response: "Timeout",
		}:
		}
	}()

	select {
	case <-ctx.Done():
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke cancelled or timed out", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke context timeout",
		}
	case response := <-responseChan:
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke response received", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return response
	}
}
