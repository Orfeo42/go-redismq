package go_redismq

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/redis/go-redis/v9"
)

var consumerName = ""

func StartRedisMqConsumer(ctx context.Context) {
	go func() {
		innerSettingConsumerName(ctx)

		if len(consumerName) == 0 {
			logAttrs(ctx, slog.LevelError, "redismq: consumer name invalid, startup aborted")

			return
		}

		StartDelayBackgroundThread(ctx)
		logAttrs(ctx, slog.LevelInfo, "redismq: delay background thread started")

		deathQueueName := GetDeathQueueName()
		createStreamGroup(ctx, deathQueueName, "death_message")
		logAttrs(ctx, slog.LevelInfo, "redismq: death queue initialized", slog.String("stream", deathQueueName))
		innerLoadConsumer(ctx)
		logAttrs(ctx, slog.LevelInfo, "redismq: default topic subscriptions started")
		startScheduleTrimStream(ctx)
		logAttrs(ctx, slog.LevelInfo, "redismq: stream trim scheduler started")
	}()
	go keepAliveMessageInvokeListener(ctx)
}

func innerSettingConsumerName(ctx context.Context) {
	interfaces, err := net.Interfaces()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: listing network interfaces failed", causeAttr(err))

		return
	}

	for _, face := range interfaces {
		if face.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrList, err := face.Addrs()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: listing interface addresses failed", causeAttr(err))

			continue
		}

		for _, one := range addrList {
			ip, _, err := net.ParseCIDR(one.String())
			if err != nil {
				logAttrs(ctx, slog.LevelWarn, "redismq: parsing interface address failed", causeAttr(err))

				continue
			}

			if ip.To4() != nil {
				logAttrs(ctx, slog.LevelInfo, "redismq: consumer name resolved from ipv4 address", slog.String("consumer_name", ip.String()))
				consumerName = ip.String()
			}
		}
	}
}

func createStreamGroup(ctx context.Context, queueName string, topic string) {
	tryCreateGroup(ctx, queueName, topic)
	tryCreateConsumer(ctx, queueName)
}

func tryCreateGroup(ctx context.Context, queueName string, topic string) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: try create group panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

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

	message := &Message{
		Topic: topic,
		Tag:   TagBlank,
		Body:  "test",
	}

	_, err = client.XAdd(ctx, message.toStreamAddArgsValues(queueName)).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelInfo, "redismq: group setup probe message failed, group may already exist",
			slog.String("stream", queueName), slog.String("consumer_group", Group), slog.String("topic", topic), causeAttr(err))
	}

	found := false

	groups, _ := client.XInfoGroups(ctx, queueName).Result()
	for _, group := range groups {
		if group.Name == Group {
			found = true
		}
	}

	if !found {
		if err := client.XGroupCreateMkStream(ctx, queueName, Group, "$").Err(); err != nil {
			logAttrs(ctx, slog.LevelInfo, "redismq: create consumer group failed, group likely exists",
				slog.String("stream", queueName), slog.String("consumer_group", Group), slog.String("topic", topic), causeAttr(err))

			return
		} else {
			logAttrs(ctx, slog.LevelInfo, "redismq: consumer group initialized",
				slog.String("stream", queueName), slog.String("consumer_group", Group), slog.String("topic", topic))
		}
	}
}

func tryCreateConsumer(ctx context.Context, queueName string) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: try create consumer panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

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

	if _, err = client.XGroupCreateConsumer(ctx, queueName, Group, consumerName).Result(); err != nil {
		logAttrs(ctx, slog.LevelInfo, "redismq: create consumer failed, consumer likely exists",
			slog.String("stream", queueName), slog.String("consumer_group", Group), slog.String("consumer_name", consumerName), causeAttr(err))
	} else {
		logAttrs(ctx, slog.LevelInfo, "redismq: consumer initialized",
			slog.String("stream", queueName), slog.String("consumer_group", Group), slog.String("consumer_name", consumerName))
	}
}

func innerLoadConsumer(ctx context.Context) {
	for _, topic := range Topics {
		blockConsumerTopic(ctx, topic)
	}
}

func blockConsumerTopic(ctx context.Context, topic string) {
	createStreamGroup(ctx, GetQueueName(topic), topic)

	createStreamGroup(ctx, getBackupQueueName(topic), topic)
	go loopConsumer(ctx, topic)
	go loopTransactionChecker(ctx, topic)
}

func loopConsumer(ctx context.Context, topic string) {
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

	for {
		if ctx.Err() != nil {
			logAttrs(ctx, slog.LevelInfo, "redismq: consumer loop stopped, context cancelled")

			return
		}

		customerIteration(ctx, client, topic)
	}
}

func customerIteration(ctx context.Context, client *redis.Client, topic string) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: consumer iteration panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))
		}
	}()

	count := 0

	message := blockReceiveConsumerMessage(ctx, client, topic)
	if message != nil {
		msgCtx := consumeContext(ctx, message)

		if consumer := getConsumer(message); consumer != nil {
			runConsumeMessage(msgCtx, consumer, message)
			//todo mark use group get message , should drop message which has no consumer
		} else {
			dropMessage(msgCtx, message)
		}

		count++
	}
	//Sleep
	if count == len(Topics) {
		time.Sleep(1 * time.Second)
	}
}

func loopTransactionChecker(ctx context.Context, topic string) {
	for {
		if ctx.Err() != nil {
			logAttrs(ctx, slog.LevelInfo, "redismq: transaction checker loop stopped, context cancelled")

			return
		}

		loopTransactionCheckerIteration(ctx, topic)
	}
}

func loopTransactionCheckerIteration(ctx context.Context, topic string) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: transaction checker iteration panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))

			return
		}
	}()

	messages := fetchTransactionPrepareMessagesForChecker(ctx, topic)
	for _, message := range messages {
		msgCtx := consumeContext(ctx, message)

		if ck := Checkers()[GetMessageKey(message.Topic, message.Tag)]; ck != nil {
			status := ck.Checker(message)
			switch status {
			case CommitTransaction:
				_, _ = commitTransactionPrepareMessage(msgCtx, message)
			case RollbackTransaction:
				_, _ = rollbackTransactionPrepareMessage(msgCtx, message)
			default:
				//todo mark save send time, max retry times limit 50
				if (CurrentTimeMillis() - message.SendTime) > 1000*60*60*8 {
					//After 8 Hours, Transaction Message Drop To Death
					putMessageToTransactionDeathQueue(msgCtx, topic, message)
				}
			}
		} else {
			if (CurrentTimeMillis() - message.SendTime) > 1000*60*60*24*7 {
				//After 7 Days, Transaction Rollback
				_, _ = rollbackTransactionPrepareMessage(msgCtx, message)
			}
		}

		time.Sleep(1 * time.Second)
	}

	select {
	case <-ctx.Done():
		logAttrs(ctx, slog.LevelInfo, "redismq: transaction checker iteration stopped, context cancelled")

		return
	case <-time.After(60 * time.Second):
	}
}

func getConsumer(message *Message) IMessageListener {
	if isBlankMessage(message) {
		return nil
	}

	return Listeners()[GetMessageKey(message.Topic, message.Tag)]
}

func dropMessage(ctx context.Context, message *Message) {
	if isBlankMessage(message) {
		messageAck(ctx, message)

		return
	}

	logAttrs(ctx, slog.LevelWarn, "redismq: message dropped, no registered consumer", messageAttrs(message)...)
	messageAck(ctx, message)
}

func isBlankMessage(message *Message) bool {
	return message.Tag == TagBlank
}

func runConsumeMessage(ctx context.Context, consumer IMessageListener, message *Message) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: run consume message panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

			return
		}
	}()

	if message.isBoardCastingMessage() {
		// todo mark it's a bug
		logAttrs(ctx, slog.LevelWarn, "redismq: broadcast message received by group consumer, dropped", messageAttrs(message)...)

		return
	}

	cost := CurrentTimeMillis()
	if message.SendTime > 0 {
		cost = CurrentTimeMillis() - message.SendTime
		// history no expire time
		if (CurrentTimeMillis() - message.SendTime) > 1000*60*60*24*3 {
			//message should expire after 3 days, drop
			logAttrs(ctx, slog.LevelWarn, "redismq: message expired, dropped",
				append(messageAttrs(message), slog.Int64("age_ms", CurrentTimeMillis()-message.SendTime))...)

			return
		}
	} else {
		cost = 0
	}

	go func() {
		defer func() {
			if exception := recover(); exception != nil {
				var err error
				if v, ok := exception.(error); ok && gerror.HasStack(v) {
					err = v
				} else {
					err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
				}

				logAttrs(ctx, slog.LevelError, "redismq: consume message panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

				if pushTaskToResumeLater(ctx, message) {
					messageAck(ctx, message)
				} else {
					// todo mark enter Resume failure, avoid message loss
				}

				return
			}
		}()

		if message.Topic == TopicInternal && message.Tag == TagInvoke {
			if message.ConsumerDelayMilliSeconds == DefaultConsumerDelayMilliSeconds {
				message.ConsumerDelayMilliSeconds = 20
			}
		}

		if message.ConsumerDelayMilliSeconds > 0 && message.ConsumerDelayMilliSeconds < 10000 {
			time.Sleep(time.Duration(message.ConsumerDelayMilliSeconds) * time.Millisecond)
		} else if message.ConsumerDelayMilliSeconds == 0 {
			time.Sleep(time.Duration(1000) * time.Millisecond)
		}

		action := consumer.Consume(ctx, message)
		logAttrs(ctx, slog.LevelInfo, "redismq: message consumed",
			append(messageAttrs(message), slog.Int64("cost_ms", cost), slog.Int("consume_action", int(action)))...)

		if action == ReconsumeLater {
			if pushTaskToResumeLater(ctx, message) {
				messageAck(ctx, message)
			} else {
				// todo mark enter Resume failure, avoid message loss
			}
		} else {
			messageAck(ctx, message)
		}
	}()
}

func messageAck(ctx context.Context, message *Message) {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: message ack panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

			return
		}
	}()

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

	streamName := GetQueueName(message.Topic)

	ackResult, err := client.XAck(ctx, streamName, Group, message.MessageId).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: message ack failed", append(messageAttrs(message), causeAttr(err))...)

		return
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: message acked",
		slog.String("message_id", message.MessageId), slog.String("stream", streamName), slog.Int64("ack_result", ackResult))
}

func blockReceiveConsumerMessage(ctx context.Context, client *redis.Client, topic string) *Message {
	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: block receive consumer message panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))

			return
		}
	}()

	streamName := GetQueueName(topic)

	result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    Group,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    60 * time.Second,
		NoAck:    true,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
			return nil
		}

		logAttrs(ctx, slog.LevelWarn, "redismq: read consumer group failed", append(topicAttrs(topic), causeAttr(err))...)

		return nil
	}

	if len(result) == 1 && len(result[0].Messages) == 1 {
		messageId := result[0].Messages[0].ID
		value := result[0].Messages[0].Values
		message := Message{}
		message.MessageId = messageId
		message.getUniqueKey()
		message.passStreamMessage(ctx, value)

		return &message
	}

	return nil
}

func pushTaskToResumeLater(ctx context.Context, message *Message) bool {
	ResumeTimesMax := MaxInt(40, message.ReconsumeMax)
	logAttrs(ctx, slog.LevelInfo, "redismq: message pushed to resume later",
		append(messageAttrs(message), slog.Int("reconsume_limit", ResumeTimesMax))...)

	if message.ReconsumeTimes >= ResumeTimesMax {
		return putMessageToDeathQueue(ctx, message)
	} else {
		message.ReconsumeTimes = message.ReconsumeTimes + 1

		var appendTime = MaxInt64(60, int64(60*message.ReconsumeTimes))

		message.StartDeliverTime = gtime.Now().Timestamp() + appendTime // resume every min till end

		send, err := sendDelayMessage(ctx, message)
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: delay message send failed", append(messageAttrs(message), causeAttr(err))...)

			return false
		}

		return send
	}
}

func putMessageToDeathQueue(ctx context.Context, message *Message) bool {
	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return false
	}

	client := redis.NewClient(options)
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	streamMessageId, err := client.XAdd(ctx, message.toStreamAddArgsValues(GetDeathQueueName())).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: push message to death queue failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: message pushed to death queue",
		append(messageAttrs(message), slog.String("death_message_id", streamMessageId), slog.String("stream", GetDeathQueueName()))...)

	return true
}

func putMessageToTransactionDeathQueue(ctx context.Context, topic string, message *Message) bool {
	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return false
	}

	client := redis.NewClient(options)
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LRem(ctx, GetTransactionPrepareQueueName(topic), 1, message.MessageId)
		pipe.RPush(ctx, getTransactionDeathQueueName(), message.MessageId)

		return nil
	})
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction message to death queue failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	return true
}

func fetchTransactionPrepareMessagesForChecker(ctx context.Context, topic string) []*Message {
	options, err := GetRedisConfig()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return []*Message{}
	}

	client := redis.NewClient(options)
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: redis client close failed", causeAttr(err))
		}
	}(client)

	result, err := client.LRange(ctx, GetTransactionPrepareQueueName(topic), 0, -1).Result()
	if err != nil {
		return []*Message{}
	}

	var messages = make([]*Message, 0)

	for _, messageId := range result {
		if len(messageId) > 0 {
			value, _ := client.Get(ctx, messageId).Result()
			if len(value) > 0 {
				var message *Message

				err = gjson.Unmarshal([]byte(value), &message)
				if err == nil {
					messages = append(messages, message)
				}
			} else {
				logAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message body missing",
					slog.String("message_id", messageId), slog.String("topic", topic))
			}
		}
	}

	return messages
}

func startScheduleTrimStream(ctx context.Context) {
	go func() {
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

		for {
			startScheduleTrimStreamIteration(ctx, client)

			select {
			case <-ctx.Done():
				logAttrs(ctx, slog.LevelInfo, "redismq: trim stream scheduler stopped, context cancelled")

				return
			case <-time.After(1000 * 60 * 10 * time.Second):
			}
		}
	}()
}

func startScheduleTrimStreamIteration(ctx context.Context, client *redis.Client) {
	const maxLen = 10000

	var err error

	defer func() {
		if exception := recover(); exception != nil {
			if v, ok := exception.(error); ok && gerror.HasStack(v) {
				err = v
			} else {
				err = gerror.NewCodef(gcode.CodeInternalPanic, "%+v", exception)
			}

			logAttrs(ctx, slog.LevelError, "redismq: trim stream iteration panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

	for _, topic := range Topics {
		queueName := GetQueueName(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
		queueName = getBackupQueueName(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
	}

	queueName := GetDeathQueueName()
	client.XTrimMaxLen(ctx, queueName, int64(maxLen))
}
