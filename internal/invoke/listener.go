package invoke

import (
	"context"
	"log/slog"

	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

type messageInvokeListener struct {
	invoker *Invoker
}

func (t *messageInvokeListener) GetTopic() string {
	return mqtype.TopicInternal
}

func (t *messageInvokeListener) GetTag() string {
	return mqtype.TagInvoke
}

func publishInvokeResponse(ctx context.Context, client Redis, replyChannel string, res *Response, status bool, response any) {
	res.Status = status
	res.Response = response
	client.Publish(ctx, replyChannel, jsonutil.MarshalString(res))
}

func (t *messageInvokeListener) Consume(ctx context.Context, message *mqtype.Message) mqtype.Action {
	var req *Request

	err := jsonutil.UnmarshalString(message.Body, &req)
	if err != nil {
		t.invoker.log.LogAttrs(ctx, slog.LevelWarn, "redismq: invoke request body unmarshal failed", append([]slog.Attr{logattr.CauseAttr(err)}, logattr.MessageAttrs(message)...)...)

		return mqtype.CommitMessage
	}

	if req == nil {
		t.invoker.log.LogAttrs(ctx, slog.LevelWarn, "redismq: invoke request is nil", logattr.MessageAttrs(message)...)

		return mqtype.CommitMessage
	}

	grp := t.invoker.group()

	if req.Group != grp {
		t.invoker.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke request addressed to another group", slog.String("invoke_group", grp), slog.String("request_group", req.Group))

		return mqtype.CommitMessage
	}

	if len(req.MessageId) == 0 || len(req.Method) == 0 {
		t.invoker.log.LogAttrs(ctx, slog.LevelWarn, "redismq: invoke request malformed", slog.String("invoke_group", grp), slog.String("invoke_method", req.Method), slog.String("message_id", req.MessageId))

		return mqtype.CommitMessage
	}

	res := &Response{}

	client, err := t.invoker.resolveRedis()
	if err != nil {
		t.invoker.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return mqtype.CommitMessage
	}

	replyChannel := getReplyChannel(req)

	defer func() {
		exception := recover()
		if exception == nil {
			return
		}

		err := logattr.PanicError(exception)
		t.invoker.log.LogAttrs(ctx, slog.LevelError, "redismq: invoke method panicked", logattr.CauseAttr(err), logattr.StackAttr(2), slog.String("invoke_method", req.Method), slog.String("reply_channel", replyChannel))

		publishInvokeResponse(ctx, client, replyChannel, res, false, err.Error())
	}()

	op, ok := t.invoker.GetMethod(req.Method)
	if !ok {
		publishInvokeResponse(ctx, client, replyChannel, res, false, "error: method not found")

		return mqtype.CommitMessage
	}

	response, err := op(ctx, req.Request)
	if err != nil {
		publishInvokeResponse(ctx, client, replyChannel, res, false, err.Error())

		return mqtype.CommitMessage
	}

	publishInvokeResponse(ctx, client, replyChannel, res, true, response)

	return mqtype.CommitMessage
}
