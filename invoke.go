package redismq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/idgen"
	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
)

type InvokeRequest struct {
	MessageId string `json:"messageId"`
	Group     string `json:"group"`
	Method    string `json:"method"`
	Request   any    `json:"request"`
}

type InvokeResponse struct {
	Status   bool `json:"status"`
	Response any  `json:"response"`
}

func failedInvokeResponse(response string) *InvokeResponse {
	return &InvokeResponse{Status: false, Response: response}
}

func decodeInvokeResponse(ctx context.Context, payload string, replyChannel string) (*InvokeResponse, bool) {
	var res *InvokeResponse

	err := json.Unmarshal([]byte(payload), &res)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: invoke response deserialization failed", causeAttr(err), slog.String("reply_channel", replyChannel))

		return nil, false
	}

	return res, true
}

func listenForResponse(ctx context.Context, req *InvokeRequest, responseChan chan *InvokeResponse) {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

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

	select {
	case msg, ok := <-ch:
		if !ok {
			logAttrs(ctx, slog.LevelInfo, "redismq: invoke response subscription channel closed", slog.String("reply_channel", replyChannel))

			return
		}

		res, ok := decodeInvokeResponse(ctx, msg.Payload, replyChannel)
		if !ok {
			return
		}

		responseChan <- res
	case <-ctx.Done():
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke wait cancelled or timed out", slog.String("reply_channel", replyChannel))
	}
}

func Invoke(ctx context.Context, req *InvokeRequest, timeoutSeconds int) *InvokeResponse {
	startTime := time.Now()

	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}

	invokeId := fmt.Sprintf("%s%d", idgen.RandomAlphanumeric(6), currentTimeMillis())
	req.MessageId = invokeId

	client, err := newRedisClient()
	if err != nil {
		return failedInvokeResponse(err.Error())
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	data, err := client.Get(ctx, "MessageInvokeGroup:"+req.Group).Result()
	if err != nil {
		return failedInvokeResponse("Invoke get group:" + err.Error())
	}

	if len(data) == 0 {
		return failedInvokeResponse("Invoke Group Not Found:" + req.Group)
	}

	responseChan := make(chan *InvokeResponse)
	go listenForResponse(ctx, req, responseChan)

	send, err := Send(ctx, &Message{
		Topic: TopicInternal,
		Tag:   TagInvoke,
		Body:  jsonutil.MarshalString(req),
	})
	if err != nil {
		return failedInvokeResponse("Invoke error:" + err.Error())
	}

	if !send {
		return failedInvokeResponse("Invoke send failed")
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: invoke request published", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

	go func() {
		time.Sleep(time.Duration(timeoutSeconds) * time.Second)

		select {
		case <-ctx.Done():
			return
		case responseChan <- failedInvokeResponse("Timeout"):
		}
	}()

	select {
	case <-ctx.Done():
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke cancelled or timed out", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return failedInvokeResponse("Invoke context timeout")
	case response := <-responseChan:
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke response received", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return response
	}
}
