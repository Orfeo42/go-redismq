package invoke

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/idgen"
	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

var (
	ErrMethodNameBlank         = errors.New("redismq: invoke method name must not be blank")
	ErrHandlerNil              = errors.New("redismq: invoke handler must not be nil")
	ErrMethodAlreadyRegistered = errors.New("redismq: invoke method already registered")
)

//go:generate moq -out redis_mock.go . Redis
type Redis interface {
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Publish(ctx context.Context, channel string, message any) *redis.IntCmd
}

//go:generate moq -out publisher_mock.go . Publisher
type Publisher interface {
	Publish(ctx context.Context, message *mqtype.Message) (bool, error)
}

//go:generate moq -out listener_registry_mock.go . ListenerRegistry
type ListenerRegistry interface {
	RegisterListener(ctx context.Context, i mqtype.IMessageListener) error
}

type Request struct {
	MessageId string `json:"messageId"`
	Group     string `json:"group"`
	Method    string `json:"method"`
	Request   any    `json:"request"`
}

type Response struct {
	Status   bool `json:"status"`
	Response any  `json:"response"`
}

func failedInvokeResponse(response string) *Response {
	return &Response{Status: false, Response: response}
}

type Invoker struct {
	resolveRedis func() (Redis, error)
	log          logging.AttrLogger
	publisher    Publisher
	registry     ListenerRegistry
	group        func() string

	mu      sync.RWMutex
	methods map[string]func(ctx context.Context, request any) (response any, err error)
}

func New(resolveRedis func() (Redis, error), log logging.AttrLogger, publisher Publisher, registry ListenerRegistry, group func() string) *Invoker {
	return &Invoker{
		resolveRedis: resolveRedis,
		log:          log,
		publisher:    publisher,
		registry:     registry,
		group:        group,
		methods:      make(map[string]func(ctx context.Context, request any) (response any, err error)),
	}
}

func (i *Invoker) decodeInvokeResponse(ctx context.Context, payload string, replyChannel string) (*Response, bool) {
	var res *Response

	err := json.Unmarshal([]byte(payload), &res)
	if err != nil {
		i.log.LogAttrs(ctx, slog.LevelWarn, "redismq: invoke response deserialization failed", logattr.CauseAttr(err), slog.String("reply_channel", replyChannel))

		return nil, false
	}

	return res, true
}

func (i *Invoker) listenForResponse(ctx context.Context, req *Request, responseChan chan *Response) {
	client, err := i.resolveRedis()
	if err != nil {
		i.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	replyChannel := getReplyChannel(req)

	pubSub := client.Subscribe(ctx, replyChannel)
	defer func(pubSub *redis.PubSub) {
		err := pubSub.Close()
		if err != nil {
			i.log.LogAttrs(ctx, slog.LevelWarn, "redismq: pubsub close failed", logattr.CauseAttr(err))
		}
	}(pubSub)

	ch := pubSub.Channel()

	select {
	case msg, ok := <-ch:
		if !ok {
			i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke response subscription channel closed", slog.String("reply_channel", replyChannel))

			return
		}

		res, ok := i.decodeInvokeResponse(ctx, msg.Payload, replyChannel)
		if !ok {
			return
		}

		responseChan <- res
	case <-ctx.Done():
		i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke wait cancelled or timed out", slog.String("reply_channel", replyChannel))
	}
}

func (i *Invoker) Invoke(ctx context.Context, req *Request, timeoutSeconds int) *Response {
	startTime := time.Now()

	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}

	invokeId := fmt.Sprintf("%s%d", idgen.RandomAlphanumeric(6), time.Now().UnixNano()/int64(time.Millisecond))
	req.MessageId = invokeId

	client, err := i.resolveRedis()
	if err != nil {
		return failedInvokeResponse(err.Error())
	}

	data, err := client.Get(ctx, "MessageInvokeGroup:"+req.Group).Result()
	if err != nil {
		return failedInvokeResponse("Invoke get group:" + err.Error())
	}

	if len(data) == 0 {
		return failedInvokeResponse("Invoke Group Not Found:" + req.Group)
	}

	responseChan := make(chan *Response)
	go i.listenForResponse(ctx, req, responseChan)

	send, err := i.publisher.Publish(ctx, &mqtype.Message{
		Topic: mqtype.TopicInternal,
		Tag:   mqtype.TagInvoke,
		Body:  jsonutil.MarshalString(req),
	})
	if err != nil {
		return failedInvokeResponse("Invoke error:" + err.Error())
	}

	if !send {
		return failedInvokeResponse("Invoke send failed")
	}

	i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke request published", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

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
		i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke cancelled or timed out", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return failedInvokeResponse("Invoke context timeout")
	case response := <-responseChan:
		i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke response received", slog.Int64("cost_ms", time.Since(startTime).Milliseconds()), slog.String("invoke_method", req.Method), slog.String("invoke_group", req.Group), slog.String("message_id", req.MessageId))

		return response
	}
}

func getReplyChannel(req *Request) string {
	return fmt.Sprintf("RedisMQ:%s_%s:%s", req.Group, req.Method, req.MessageId)
}

func (i *Invoker) RegisterInternalListeners(ctx context.Context) error {
	if err := i.registry.RegisterListener(ctx, &messageInvokeListener{invoker: i}); err != nil {
		return err
	}

	i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke listener registered")

	return nil
}

func (i *Invoker) GetMethod(method string) (func(ctx context.Context, request any) (response any, err error), bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	op, ok := i.methods[method]

	return op, ok
}

func (i *Invoker) RegisterInvoke(ctx context.Context, methodName string, op func(ctx context.Context, request any) (response any, err error)) error {
	if len(methodName) == 0 {
		return ErrMethodNameBlank
	}

	if op == nil {
		return ErrHandlerNil
	}

	i.mu.Lock()

	_, exists := i.methods[methodName]
	if !exists {
		i.methods[methodName] = op
	}
	i.mu.Unlock()

	if exists {
		return ErrMethodAlreadyRegistered
	}

	i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke method registered", slog.String("invoke_method", methodName))

	return nil
}

func (i *Invoker) ResetMethodsForTest() (restore func()) {
	i.mu.Lock()
	original := i.methods
	i.methods = make(map[string]func(ctx context.Context, request any) (response any, err error))
	i.mu.Unlock()

	return func() {
		i.mu.Lock()
		i.methods = original
		i.mu.Unlock()
	}
}

func (i *Invoker) KeepAlive(ctx context.Context) {
	client, err := i.resolveRedis()
	if err != nil {
		i.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	client.Set(ctx, "MessageInvokeGroup:"+i.group(), true, 300*time.Second)

	for {
		select {
		case <-ctx.Done():
			i.log.LogAttrs(ctx, slog.LevelInfo, "redismq: invoke keepalive stopped, context cancelled")

			return
		case <-time.After(60 * time.Second):
		}

		client.Expire(ctx, "MessageInvokeGroup:"+i.group(), 300*time.Second)
	}
}
