package redismq

import (
	"context"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/consumer"
	"github.com/Orfeo42/go-redismq/v3/internal/delayqueue"
	"github.com/Orfeo42/go-redismq/v3/internal/invoke"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/producer"
	"github.com/Orfeo42/go-redismq/v3/internal/registry"
	"github.com/Orfeo42/go-redismq/v3/internal/traceio"
	"github.com/Orfeo42/go-redismq/v3/internal/txchecker"
)

type Client struct {
	redisClient *redis.Client
	log         logging.AttrLogger

	producer   *producer.Producer
	delayQueue *delayqueue.Queue
	invoker    *invoke.Invoker
	registry   *registry.Registry
	consumer   *consumer.Consumer

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func New(cfg RedisMqConfig, opts ...Option) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	s := newSettings()
	for _, opt := range opts {
		opt(s)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.Database,
	})

	logAttr := logging.NewAdapter(s.logger)

	tracer := traceio.New()
	tracer.SetTraceIDFromContext(s.traceIDFromContext)
	tracer.SetTraceIDToContext(s.traceIDToContext)

	group := func() string { return cfg.Group }

	reg := registry.New(logAttr)

	prod := producer.New(
		func() (producer.Redis, error) { return redisClient, nil },
		logAttr, tracer, s.clock,
	)

	c := &Client{
		redisClient: redisClient,
		log:         logAttr,
		producer:    prod,
		registry:    reg,
	}

	c.delayQueue = delayqueue.New(
		func() (delayqueue.Redis, error) { return redisClient, nil },
		logAttr, tracer, s.clock, prod, &c.wg,
	)

	c.invoker = invoke.New(
		func() (invoke.Redis, error) { return redisClient, nil },
		logAttr, prod, reg, group,
	)

	txCheck := txchecker.New(
		func() (txchecker.Redis, error) { return redisClient, nil },
		logAttr, reg, prod, tracer, s.clock,
	)

	c.consumer = consumer.New(
		func() (consumer.Redis, error) { return redisClient, nil },
		logAttr, reg, c.delayQueue, txCheck, tracer, group, s.clock, &c.wg,
	)

	return c, nil
}

func (c *Client) Start(ctx context.Context) error {
	if err := c.invoker.RegisterInternalListeners(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)

	if err := c.consumer.Start(runCtx); err != nil {
		cancel()

		return err
	}

	c.cancel = cancel

	c.delayQueue.StartBackgroundThread(runCtx)

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.invoker.KeepAlive(runCtx)
	}()

	return nil
}

func (c *Client) Close(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}

	done := make(chan struct{})

	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: client closed, all handlers drained")
	case <-ctx.Done():
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: client close timed out waiting for in-flight handlers")

		return ctx.Err()
	}

	return c.redisClient.Close()
}

func (c *Client) Send(ctx context.Context, message *Message) (bool, error) {
	return c.producer.Publish(ctx, message)
}

func (c *Client) SendTransaction(ctx context.Context, message *Message, transactionExecuter func(messageToSend *Message) (TransactionStatus, error)) (bool, error) {
	return c.producer.SendTransaction(ctx, message, transactionExecuter)
}

func (c *Client) SendDelay(ctx context.Context, message *Message, delay int64) (bool, error) {
	return c.delayQueue.SendDelay(ctx, message, delay)
}

func (c *Client) Invoke(ctx context.Context, req *InvokeRequest, timeoutSeconds int) *InvokeResponse {
	return c.invoker.Invoke(ctx, req, timeoutSeconds)
}

func (c *Client) RegisterInvoke(ctx context.Context, method string, op func(ctx context.Context, request any) (response any, err error)) error {
	return c.invoker.RegisterInvoke(ctx, method, op)
}

func (c *Client) RegisterListener(ctx context.Context, l IMessageListener) error {
	return c.registry.RegisterListener(ctx, l)
}

func (c *Client) RegisterChecker(ctx context.Context, ch IMessageChecker) error {
	return c.registry.RegisterChecker(ctx, ch)
}
