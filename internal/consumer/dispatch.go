package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/Orfeo42/go-redismq/v3/internal/logattr"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

func (c *Consumer) dispatchConsumerMessage(ctx context.Context, message *mqtype.Message) {
	msgCtx := c.tracer.ConsumeContext(ctx, message)

	listener := c.getConsumer(message)
	if listener == nil {
		c.dropMessage(msgCtx, message)

		return
	}

	c.runConsumeMessage(msgCtx, listener, message)
	//todo mark use group get message , should drop message which has no consumer
}

func (c *Consumer) getConsumer(message *mqtype.Message) mqtype.IMessageListener {
	if isBlankMessage(message) {
		return nil
	}

	return c.registry.GetListenerFor(message.Topic, message.Tag)
}

func (c *Consumer) dropMessage(ctx context.Context, message *mqtype.Message) {
	if isBlankMessage(message) {
		c.messageAck(ctx, message)

		return
	}

	c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: message dropped, no registered consumer", logattr.MessageAttrs(message)...)
	c.messageAck(ctx, message)
}

func isBlankMessage(message *mqtype.Message) bool {
	return message.Tag == mqtype.TagBlank
}

func (c *Consumer) runConsumeMessage(ctx context.Context, listener mqtype.IMessageListener, message *mqtype.Message) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: run consume message panicked", append(logattr.MessageAttrs(message), logattr.CauseAttr(err), logattr.StackAttr(2))...)

			return
		}
	}()

	if message.IsBroadcastingMessage() {
		// todo mark it's a bug
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: broadcast message received by group consumer, dropped", logattr.MessageAttrs(message)...)

		return
	}

	cost, expired := c.messageCost(message)
	if expired {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: message expired, dropped",
			append(logattr.MessageAttrs(message), slog.Int64("age_ms", cost))...)

		return
	}

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.consumeMessage(ctx, listener, message, cost)
	}()
}

func (c *Consumer) messageCost(message *mqtype.Message) (cost int64, expired bool) {
	if message.SendTime <= 0 {
		return 0, false
	}

	elapsed := c.currentTimeMillis() - message.SendTime

	return elapsed, elapsed > messageExpireAfter
}

func (c *Consumer) consumeMessage(ctx context.Context, listener mqtype.IMessageListener, message *mqtype.Message, cost int64) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: consume message panicked", append(logattr.MessageAttrs(message), logattr.CauseAttr(err), logattr.StackAttr(2))...)

			c.resumeOrLoseMessage(ctx, message)

			return
		}
	}()

	time.Sleep(consumerDelay(message))

	action := listener.Consume(ctx, message)
	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: message consumed",
		append(logattr.MessageAttrs(message), slog.Int64("cost_ms", cost), slog.Int("consume_action", int(action)))...)

	if action != mqtype.ReconsumeLater {
		c.messageAck(ctx, message)

		return
	}

	c.resumeOrLoseMessage(ctx, message)
}

func (c *Consumer) resumeOrLoseMessage(ctx context.Context, message *mqtype.Message) {
	if c.pushTaskToResumeLater(ctx, message) {
		c.messageAck(ctx, message)

		return
	}
	// todo mark enter Resume failure, avoid message loss
}

func consumerDelay(message *mqtype.Message) time.Duration {
	if message.Topic == mqtype.TopicInternal && message.Tag == mqtype.TagInvoke && message.ConsumerDelayMilliSeconds == mqtype.DefaultConsumerDelayMilliSeconds {
		message.ConsumerDelayMilliSeconds = 20
	}

	if message.ConsumerDelayMilliSeconds > 0 && message.ConsumerDelayMilliSeconds < 10000 {
		return time.Duration(message.ConsumerDelayMilliSeconds) * time.Millisecond
	}

	if message.ConsumerDelayMilliSeconds == 0 {
		return time.Duration(1000) * time.Millisecond
	}

	return 0
}

func (c *Consumer) messageAck(ctx context.Context, message *mqtype.Message) {
	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: message ack panicked", append(logattr.MessageAttrs(message), logattr.CauseAttr(err), logattr.StackAttr(2))...)

			return
		}
	}()

	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return
	}

	streamName := streamname.Queue(message.Topic)

	ackResult, err := client.XAck(ctx, streamName, c.group(), message.MessageId).Result()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: message ack failed", append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return
	}

	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: message acked",
		slog.String("message_id", message.MessageId), slog.String("stream", streamName), slog.Int64("ack_result", ackResult))
}

func (c *Consumer) pushTaskToResumeLater(ctx context.Context, message *mqtype.Message) bool {
	resumeTimesMax := max(40, message.ReconsumeMax)
	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: message pushed to resume later",
		append(logattr.MessageAttrs(message), slog.Int("reconsume_limit", resumeTimesMax))...)

	if message.ReconsumeTimes >= resumeTimesMax {
		return c.putMessageToDeathQueue(ctx, message)
	}

	message.ReconsumeTimes++

	appendTime := max(int64(60), int64(60*message.ReconsumeTimes))

	send, err := c.delayQueue.ScheduleDelay(ctx, message, appendTime)
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "redismq: delay message send failed", append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false
	}

	return send
}

func (c *Consumer) putMessageToDeathQueue(ctx context.Context, message *mqtype.Message) bool {
	client, err := c.resolveRedis()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

		return false
	}

	streamAddArgs, err := message.ToStreamAddArgsValues(streamname.DeathQueue())
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: death queue message marshal failed", append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false
	}

	streamMessageId, err := client.XAdd(ctx, streamAddArgs).Result()
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelError, "redismq: push message to death queue failed", append(logattr.MessageAttrs(message), logattr.CauseAttr(err))...)

		return false
	}

	c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: message pushed to death queue",
		append(logattr.MessageAttrs(message), slog.String("death_message_id", streamMessageId), slog.String("stream", streamname.DeathQueue()))...)

	return true
}

func (c *Consumer) startScheduleTrimStream(ctx context.Context) {
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		client, err := c.resolveRedis()
		if err != nil {
			c.log.LogAttrs(ctx, slog.LevelError, "redismq: redis config not registered", logattr.CauseAttr(err))

			return
		}

		for {
			c.startScheduleTrimStreamIteration(ctx, client)

			select {
			case <-ctx.Done():
				c.log.LogAttrs(ctx, slog.LevelInfo, "redismq: trim stream scheduler stopped, context cancelled")

				return
			case <-time.After(1000 * 60 * 10 * time.Second):
			}
		}
	}()
}

func (c *Consumer) startScheduleTrimStreamIteration(ctx context.Context, client Redis) {
	const maxLen = 10000

	defer func() {
		if exception := recover(); exception != nil {
			err := logattr.PanicError(exception)

			c.log.LogAttrs(ctx, slog.LevelError, "redismq: trim stream iteration panicked", logattr.CauseAttr(err), logattr.StackAttr(2))

			return
		}
	}()

	for _, topic := range c.registry.GetTopics() {
		queueName := streamname.Queue(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
		queueName = streamname.BackupQueue(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
	}

	queueName := streamname.DeathQueue()
	client.XTrimMaxLen(ctx, queueName, int64(maxLen))
}
