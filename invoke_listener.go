package go_redismq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type MessageInvokeListener struct {
}

func (t MessageInvokeListener) GetTopic() string {
	return TopicInternal
}

func (t MessageInvokeListener) GetTag() string {
	return TagInvoke
}

func publishInvokeResponse(ctx context.Context, client *redis.Client, replyChannel string, res *InvoiceResponse, status bool, response any) {
	res.Status = status
	res.Response = response
	client.Publish(ctx, replyChannel, MarshalToJsonString(res))
}

func (t MessageInvokeListener) Consume(ctx context.Context, message *Message) Action {
	var req *InvoiceRequest

	err := UnmarshalFromJsonString(message.Body, &req)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: invoke request body unmarshal failed", append([]slog.Attr{causeAttr(err)}, messageAttrs(message)...)...)

		return CommitMessage
	}

	if req == nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: invoke request is nil", messageAttrs(message)...)

		return CommitMessage
	}

	if req.Group != Group {
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke request addressed to another group", slog.String("invoke_group", Group), slog.String("request_group", req.Group))

		return CommitMessage
	}

	if len(req.MessageId) == 0 || len(req.Method) == 0 {
		logAttrs(ctx, slog.LevelWarn, "redismq: invoke request malformed", slog.String("invoke_group", Group), slog.String("invoke_method", req.Method), slog.String("message_id", req.MessageId))

		return CommitMessage
	}

	res := &InvoiceResponse{}

	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return CommitMessage
	}

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	replyChannel := getReplyChannel(req)

	defer func() {
		exception := recover()
		if exception == nil {
			return
		}

		err := panicError(exception)
		logAttrs(ctx, slog.LevelError, "redismq: invoke method panicked", causeAttr(err), stackAttr(2), slog.String("invoke_method", req.Method), slog.String("reply_channel", replyChannel))

		publishInvokeResponse(ctx, client, replyChannel, res, false, err.Error())
	}()

	op, ok := invokeMap[req.Method]
	if !ok {
		publishInvokeResponse(ctx, client, replyChannel, res, false, "error: method not found")

		return CommitMessage
	}

	response, err := op(ctx, req.Request)
	if err != nil {
		publishInvokeResponse(ctx, client, replyChannel, res, false, err.Error())

		return CommitMessage
	}

	publishInvokeResponse(ctx, client, replyChannel, res, true, response)

	return CommitMessage
}

func getReplyChannel(req *InvoiceRequest) string {
	replyChannel := fmt.Sprintf("RedisMQ:%s_%s:%s", req.Group, req.Method, req.MessageId)

	return replyChannel
}

func RegisterInternalListeners(ctx context.Context) {
	RegisterListener(ctx, &MessageInvokeListener{})

	logAttrs(ctx, slog.LevelInfo, "redismq: invoke listener registered")
}

var invokeMap = make(map[string]func(ctx context.Context, request any) (response any, err error))

func RegisterInvoke(ctx context.Context, methodName string, op func(ctx context.Context, request any) (response any, err error)) {
	if len(methodName) <= 0 || op == nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: register invoke called with invalid method or nil handler", slog.String("invoke_method", methodName))
	} else if _, ok := invokeMap[methodName]; ok {
		logAttrs(ctx, slog.LevelWarn, "redismq: register invoke called for already registered method", slog.String("invoke_method", methodName))
	} else {
		invokeMap[methodName] = op
		logAttrs(ctx, slog.LevelInfo, "redismq: invoke method registered", slog.String("invoke_method", methodName))
	}
}

func keepAliveMessageInvokeListener(ctx context.Context) {
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

	client.Set(ctx, "MessageInvokeGroup:"+Group, true, 300*time.Second)

	for {
		select {
		case <-ctx.Done():
			logAttrs(ctx, slog.LevelInfo, "redismq: invoke keepalive stopped, context cancelled")

			return
		case <-time.After(60 * time.Second):
		}

		client.Expire(ctx, "MessageInvokeGroup:"+Group, 300*time.Second)
	}
}
