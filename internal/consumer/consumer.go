package consumer

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/logging"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

const messageExpireAfter = 1000 * 60 * 60 * 24 * 3

//go:generate moq -out redis_mock.go . Redis
type Redis interface {
	XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XGroupCreateConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	XTrimMaxLen(ctx context.Context, key string, maxLen int64) *redis.IntCmd
}

//go:generate moq -out listener_registry_mock.go . ListenerRegistry
type ListenerRegistry interface {
	GetListenerFor(topic string, tag string) mqtype.IMessageListener
	GetTopics() []string
	TopicCount() int
}

//go:generate moq -out delay_scheduler_mock.go . DelayScheduler
type DelayScheduler interface {
	ScheduleDelay(ctx context.Context, message *mqtype.Message, delaySeconds int64) (bool, error)
}

//go:generate moq -out transaction_checker_mock.go . TransactionChecker
type TransactionChecker interface {
	Run(ctx context.Context, topic string)
}

//go:generate moq -out tracer_mock.go . Tracer
type Tracer interface {
	ConsumeContext(ctx context.Context, message *mqtype.Message) context.Context
}

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

var ErrConsumerNameUnresolved = errors.New("redismq: consumer name could not be resolved")

type Consumer struct {
	resolveRedis func() (Redis, error)
	log          logging.AttrLogger
	registry     ListenerRegistry
	delayQueue   DelayScheduler
	txChecker    TransactionChecker
	tracer       Tracer
	group        func() string
	clock        Clock
	wg           *sync.WaitGroup

	nameMu sync.RWMutex
	name   string
}

func New(
	resolveRedis func() (Redis, error),
	log logging.AttrLogger,
	registry ListenerRegistry,
	delayQueue DelayScheduler,
	txChecker TransactionChecker,
	tracer Tracer,
	group func() string,
	clock Clock,
	wg *sync.WaitGroup,
) *Consumer {
	if clock == nil {
		clock = systemClock{}
	}

	if wg == nil {
		wg = &sync.WaitGroup{}
	}

	return &Consumer{
		resolveRedis: resolveRedis,
		log:          log,
		registry:     registry,
		delayQueue:   delayQueue,
		txChecker:    txChecker,
		tracer:       tracer,
		group:        group,
		clock:        clock,
		wg:           wg,
	}
}

func (c *Consumer) currentTimeMillis() int64 {
	return c.clock.Now().UnixNano() / int64(time.Millisecond)
}

func (c *Consumer) setConsumerName(name string) {
	c.nameMu.Lock()
	c.name = name
	c.nameMu.Unlock()
}

func (c *Consumer) getConsumerName() string {
	c.nameMu.RLock()
	defer c.nameMu.RUnlock()

	return c.name
}

func (c *Consumer) Start(ctx context.Context) error {
	c.innerSettingConsumerName(ctx)

	if len(c.getConsumerName()) == 0 {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: consumer name invalid, startup aborted")

		return ErrConsumerNameUnresolved
	}

	deathQueueName := streamname.DeathQueue()
	c.createStreamGroup(ctx, deathQueueName, "death_message")
	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: death queue initialized", slog.String("stream", deathQueueName))

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.innerLoadConsumer(ctx)
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: default topic subscriptions started")
		c.startScheduleTrimStream(ctx)
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: stream trim scheduler started")
	}()

	return nil
}

func (c *Consumer) innerSettingConsumerName(ctx context.Context) {
	interfaces, err := net.Interfaces()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: listing network interfaces failed", logattr.CauseAttr(err))

		return
	}

	for _, face := range interfaces {
		if face.Flags&net.FlagLoopback != 0 {
			continue
		}

		c.resolveConsumerNameFromInterface(ctx, face)
	}
}

func (c *Consumer) resolveConsumerNameFromInterface(ctx context.Context, face net.Interface) {
	addrList, err := face.Addrs()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: listing interface addresses failed", logattr.CauseAttr(err))

		return
	}

	for _, one := range addrList {
		ip, _, err := net.ParseCIDR(one.String())
		if err != nil {
			c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: parsing interface address failed", logattr.CauseAttr(err))

			continue
		}

		if ip.To4() == nil {
			continue
		}

		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: consumer name resolved from ipv4 address", slog.String("consumer_name", ip.String()))
		c.setConsumerName(ip.String())
	}
}

func (c *Consumer) createStreamGroup(ctx context.Context, queueName string, topic string) {
	c.tryCreateGroup(ctx, queueName, topic)
	c.tryCreateConsumer(ctx, queueName)
}

func (c *Consumer) tryCreateGroup(ctx context.Context, queueName string, topic string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: try create group panicked", logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	message := &mqtype.Message{
		Topic: topic,
		Tag:   mqtype.TagBlank,
		Body:  "test",
	}

	grp := c.group()

	streamAddArgs, err := message.ToStreamAddArgsValues(queueName)
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: group setup probe message marshal failed",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), logattr.CauseAttr(err))

		return
	}

	_, err = client.XAdd(ctx, streamAddArgs).Result()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: group setup probe message failed, group may already exist",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), logattr.CauseAttr(err))
	}

	found := false

	groups, _ := client.XInfoGroups(ctx, queueName).Result()
	for _, g := range groups {
		if g.Name == grp {
			found = true
		}
	}

	if found {
		return
	}

	if err := client.XGroupCreateMkStream(ctx, queueName, grp, "$").Err(); err != nil {
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: create consumer group failed, group likely exists",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), logattr.CauseAttr(err))

		return
	}

	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: consumer group initialized",
		slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic))
}

func (c *Consumer) tryCreateConsumer(ctx context.Context, queueName string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: try create consumer panicked", logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	grp := c.group()
	name := c.getConsumerName()

	if _, err = client.XGroupCreateConsumer(ctx, queueName, grp, name).Result(); err != nil {
		c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: create consumer failed, consumer likely exists",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("consumer_name", name), logattr.CauseAttr(err))

		return
	}

	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: consumer initialized",
		slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("consumer_name", name))
}

func (c *Consumer) innerLoadConsumer(ctx context.Context) {
	for _, topic := range c.registry.GetTopics() {
		c.blockConsumerTopic(ctx, topic)
	}
}

func (c *Consumer) blockConsumerTopic(ctx context.Context, topic string) {
	c.createStreamGroup(ctx, streamname.Queue(topic), topic)

	c.createStreamGroup(ctx, streamname.BackupQueue(topic), topic)

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.loopConsumer(ctx, topic)
	}()

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.txChecker.Run(ctx, topic)
	}()
}

func (c *Consumer) loopConsumer(ctx context.Context, topic string) {
	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	for {
		if ctx.Err() != nil {
			c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: consumer loop stopped, context cancelled")

			return
		}

		c.customerIteration(ctx, client, topic)
	}
}

func (c *Consumer) customerIteration(ctx context.Context, client Redis, topic string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: consumer iteration panicked", slog.String("topic", topic), logattr.CauseAttr(err), logattr.StackAttr(2))
		}
	}()

	count := 0

	message := c.blockReceiveConsumerMessage(ctx, client, topic)
	if message != nil {
		c.dispatchConsumerMessage(ctx, message)

		count++
	}

	if count == c.registry.TopicCount() {
		time.Sleep(1 * time.Second)
	}
}

func (c *Consumer) blockReceiveConsumerMessage(ctx context.Context, client Redis, topic string) *mqtype.Message {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: block receive consumer message panicked", slog.String("topic", topic), logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	streamName := streamname.Queue(topic)

	result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group(),
		Consumer: c.getConsumerName(),
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    60 * time.Second,
		NoAck:    true,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
			return nil
		}

		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: read consumer group failed", append(logattr.TopicAttrs(topic, c.group()), logattr.CauseAttr(err))...)

		return nil
	}

	if len(result) != 1 || len(result[0].Messages) != 1 {
		return nil
	}

	messageId := result[0].Messages[0].ID
	value := result[0].Messages[0].Values
	message := mqtype.Message{}
	message.MessageId = messageId
	message.GetUniqueKey()

	if stack, passErr := message.PassStreamMessage(value); passErr != nil {
		if stack != nil {
			c.log.LogAttrs(ctx, slog.LevelError, "redismq: passStreamMessage panic recovered", logattr.CauseAttr(passErr), slog.Any("stack", stack), slog.String("message_id", message.MessageId))
		} else {
			c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: stream metadata unmarshal failed", logattr.CauseAttr(passErr), slog.String("message_id", message.MessageId))
		}
	}

	return &message
}
