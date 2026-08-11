package redismq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Orfeo42/go-redismq/v3/internal/streamname"
)

var (
	consumerNameMu sync.RWMutex
	consumerName   = ""
)

func setConsumerName(name string) {
	consumerNameMu.Lock()
	consumerName = name
	consumerNameMu.Unlock()
}

func getConsumerName() string {
	consumerNameMu.RLock()
	defer consumerNameMu.RUnlock()

	return consumerName
}

const (
	transactionDeathAfter    = 1000 * 60 * 60 * 8
	transactionRollbackAfter = 1000 * 60 * 60 * 24 * 7
	messageExpireAfter       = 1000 * 60 * 60 * 24 * 3
)

func StartRedisMqConsumer(ctx context.Context) {
	go func() {
		innerSettingConsumerName(ctx)

		if len(getConsumerName()) == 0 {
			logAttrs(ctx, slog.LevelError, "redismq: consumer name invalid, startup aborted")

			return
		}

		StartDelayBackgroundThread(ctx)
		logAttrs(ctx, slog.LevelInfo, "redismq: delay background thread started")

		deathQueueName := streamname.DeathQueue()
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

		resolveConsumerNameFromInterface(ctx, face)
	}
}

func resolveConsumerNameFromInterface(ctx context.Context, face net.Interface) {
	addrList, err := face.Addrs()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: listing interface addresses failed", causeAttr(err))

		return
	}

	for _, one := range addrList {
		ip, _, err := net.ParseCIDR(one.String())
		if err != nil {
			logAttrs(ctx, slog.LevelWarn, "redismq: parsing interface address failed", causeAttr(err))

			continue
		}

		if ip.To4() == nil {
			continue
		}

		logAttrs(ctx, slog.LevelInfo, "redismq: consumer name resolved from ipv4 address", slog.String("consumer_name", ip.String()))
		setConsumerName(ip.String())
	}
}

func createStreamGroup(ctx context.Context, queueName string, topic string) {
	tryCreateGroup(ctx, queueName, topic)
	tryCreateConsumer(ctx, queueName)
}

func tryCreateGroup(ctx context.Context, queueName string, topic string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: try create group panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	message := &Message{
		Topic: topic,
		Tag:   TagBlank,
		Body:  "test",
	}

	grp := getGroup()

	streamAddArgs, err := message.toStreamAddArgsValues(queueName)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: group setup probe message marshal failed",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), causeAttr(err))

		return
	}

	_, err = client.XAdd(ctx, streamAddArgs).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelInfo, "redismq: group setup probe message failed, group may already exist",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), causeAttr(err))
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
		logAttrs(ctx, slog.LevelInfo, "redismq: create consumer group failed, group likely exists",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic), causeAttr(err))

		return
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: consumer group initialized",
		slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("topic", topic))
}

func tryCreateConsumer(ctx context.Context, queueName string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: try create consumer panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	grp := getGroup()
	name := getConsumerName()

	if _, err = client.XGroupCreateConsumer(ctx, queueName, grp, name).Result(); err != nil {
		logAttrs(ctx, slog.LevelInfo, "redismq: create consumer failed, consumer likely exists",
			slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("consumer_name", name), causeAttr(err))

		return
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: consumer initialized",
		slog.String("stream", queueName), slog.String("consumer_group", grp), slog.String("consumer_name", name))
}

func innerLoadConsumer(ctx context.Context) {
	for _, topic := range getTopics() {
		blockConsumerTopic(ctx, topic)
	}
}

func blockConsumerTopic(ctx context.Context, topic string) {
	createStreamGroup(ctx, streamname.Queue(topic), topic)

	createStreamGroup(ctx, streamname.BackupQueue(topic), topic)
	go loopConsumer(ctx, topic)
	go loopTransactionChecker(ctx, topic)
}

func loopConsumer(ctx context.Context, topic string) {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	for {
		if ctx.Err() != nil {
			logAttrs(ctx, slog.LevelInfo, "redismq: consumer loop stopped, context cancelled")

			return
		}

		customerIteration(ctx, client, topic)
	}
}

func customerIteration(ctx context.Context, client *redis.Client, topic string) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: consumer iteration panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))
		}
	}()

	count := 0

	message := blockReceiveConsumerMessage(ctx, client, topic)
	if message != nil {
		dispatchConsumerMessage(ctx, message)

		count++
	}

	if count == topicCount() {
		time.Sleep(1 * time.Second)
	}
}

func dispatchConsumerMessage(ctx context.Context, message *Message) {
	msgCtx := consumeContext(ctx, message)

	consumer := getConsumer(message)
	if consumer == nil {
		dropMessage(msgCtx, message)

		return
	}

	runConsumeMessage(msgCtx, consumer, message)
	//todo mark use group get message , should drop message which has no consumer
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
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: transaction checker iteration panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))

			return
		}
	}()

	messages := fetchTransactionPrepareMessagesForChecker(ctx, topic)
	for _, message := range messages {
		checkTransactionPrepareMessage(ctx, topic, message)

		time.Sleep(1 * time.Second)
	}

	select {
	case <-ctx.Done():
		logAttrs(ctx, slog.LevelInfo, "redismq: transaction checker iteration stopped, context cancelled")

		return
	case <-time.After(60 * time.Second):
	}
}

func checkTransactionPrepareMessage(ctx context.Context, topic string, message *Message) {
	msgCtx := consumeContext(ctx, message)

	ck := getCheckerFor(message.Topic, message.Tag)
	if ck == nil {
		if (currentTimeMillis() - message.SendTime) > transactionRollbackAfter {
			//After 7 Days, Transaction Rollback
			_, _ = rollbackTransactionPrepareMessage(msgCtx, message)
		}

		return
	}

	switch ck.Checker(message) {
	case CommitTransaction:
		_, _ = commitTransactionPrepareMessage(msgCtx, message)
	case RollbackTransaction:
		_, _ = rollbackTransactionPrepareMessage(msgCtx, message)
	default:
		//todo mark save send time, max retry times limit 50
		if (currentTimeMillis() - message.SendTime) > transactionDeathAfter {
			//After 8 Hours, Transaction Message Drop To Death
			putMessageToTransactionDeathQueue(msgCtx, topic, message)
		}
	}
}

func getConsumer(message *Message) IMessageListener {
	if isBlankMessage(message) {
		return nil
	}

	return getListenerFor(message.Topic, message.Tag)
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
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: run consume message panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

			return
		}
	}()

	if message.isBroadcastingMessage() {
		// todo mark it's a bug
		logAttrs(ctx, slog.LevelWarn, "redismq: broadcast message received by group consumer, dropped", messageAttrs(message)...)

		return
	}

	cost, expired := messageCost(message)
	if expired {
		logAttrs(ctx, slog.LevelWarn, "redismq: message expired, dropped",
			append(messageAttrs(message), slog.Int64("age_ms", cost))...)

		return
	}

	go consumeMessage(ctx, consumer, message, cost)
}

func messageCost(message *Message) (cost int64, expired bool) {
	if message.SendTime <= 0 {
		return 0, false
	}

	elapsed := currentTimeMillis() - message.SendTime

	return elapsed, elapsed > messageExpireAfter
}

func consumeMessage(ctx context.Context, consumer IMessageListener, message *Message, cost int64) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: consume message panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

			resumeOrLoseMessage(ctx, message)

			return
		}
	}()

	time.Sleep(consumerDelay(message))

	action := consumer.Consume(ctx, message)
	logAttrs(ctx, slog.LevelInfo, "redismq: message consumed",
		append(messageAttrs(message), slog.Int64("cost_ms", cost), slog.Int("consume_action", int(action)))...)

	if action != ReconsumeLater {
		messageAck(ctx, message)

		return
	}

	resumeOrLoseMessage(ctx, message)
}

func resumeOrLoseMessage(ctx context.Context, message *Message) {
	if pushTaskToResumeLater(ctx, message) {
		messageAck(ctx, message)

		return
	}
	// todo mark enter Resume failure, avoid message loss
}

func consumerDelay(message *Message) time.Duration {
	if message.Topic == TopicInternal && message.Tag == TagInvoke && message.ConsumerDelayMilliSeconds == DefaultConsumerDelayMilliSeconds {
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

func messageAck(ctx context.Context, message *Message) {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: message ack panicked", append(messageAttrs(message), causeAttr(err), stackAttr(2))...)

			return
		}
	}()

	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return
	}

	streamName := streamname.Queue(message.Topic)

	ackResult, err := client.XAck(ctx, streamName, getGroup(), message.MessageId).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: message ack failed", append(messageAttrs(message), causeAttr(err))...)

		return
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: message acked",
		slog.String("message_id", message.MessageId), slog.String("stream", streamName), slog.Int64("ack_result", ackResult))
}

func blockReceiveConsumerMessage(ctx context.Context, client *redis.Client, topic string) *Message {
	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: block receive consumer message panicked", slog.String("topic", topic), causeAttr(err), stackAttr(2))

			return
		}
	}()

	streamName := streamname.Queue(topic)

	result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    getGroup(),
		Consumer: getConsumerName(),
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

	if len(result) != 1 || len(result[0].Messages) != 1 {
		return nil
	}

	messageId := result[0].Messages[0].ID
	value := result[0].Messages[0].Values
	message := Message{}
	message.MessageId = messageId
	message.getUniqueKey()
	message.passStreamMessage(ctx, value)

	return &message
}

func pushTaskToResumeLater(ctx context.Context, message *Message) bool {
	ResumeTimesMax := maxInt(40, message.ReconsumeMax)
	logAttrs(ctx, slog.LevelInfo, "redismq: message pushed to resume later",
		append(messageAttrs(message), slog.Int("reconsume_limit", ResumeTimesMax))...)

	if message.ReconsumeTimes >= ResumeTimesMax {
		return putMessageToDeathQueue(ctx, message)
	}

	message.ReconsumeTimes = message.ReconsumeTimes + 1

	var appendTime = maxInt64(60, int64(60*message.ReconsumeTimes))

	message.StartDeliverTime = time.Now().Unix() + appendTime // resume every min till end

	send, err := sendDelayMessage(ctx, message)
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: delay message send failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	return send
}

func putMessageToDeathQueue(ctx context.Context, message *Message) bool {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return false
	}

	streamAddArgs, err := message.toStreamAddArgsValues(streamname.DeathQueue())
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: death queue message marshal failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	streamMessageId, err := client.XAdd(ctx, streamAddArgs).Result()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: push message to death queue failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	logAttrs(ctx, slog.LevelInfo, "redismq: message pushed to death queue",
		append(messageAttrs(message), slog.String("death_message_id", streamMessageId), slog.String("stream", streamname.DeathQueue()))...)

	return true
}

func putMessageToTransactionDeathQueue(ctx context.Context, topic string, message *Message) bool {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return false
	}

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LRem(ctx, streamname.TransactionPrepareQueue(topic), 1, message.MessageId)
		pipe.RPush(ctx, streamname.TransactionDeathQueue(), message.MessageId)

		return nil
	})
	if err != nil {
		logAttrs(ctx, slog.LevelWarn, "redismq: transaction message to death queue failed", append(messageAttrs(message), causeAttr(err))...)

		return false
	}

	return true
}

func fetchTransactionPrepareMessagesForChecker(ctx context.Context, topic string) []*Message {
	client, err := newRedisClient()
	if err != nil {
		logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

		return []*Message{}
	}

	result, err := client.LRange(ctx, streamname.TransactionPrepareQueue(topic), 0, -1).Result()
	if err != nil {
		return []*Message{}
	}

	messages := make([]*Message, 0)

	for _, messageId := range result {
		if len(messageId) == 0 {
			continue
		}

		value, _ := client.Get(ctx, messageId).Result()
		if len(value) == 0 {
			logAttrs(ctx, slog.LevelWarn, "redismq: transaction prepare message body missing",
				slog.String("message_id", messageId), slog.String("topic", topic))

			continue
		}

		message, ok := decodeTransactionPrepareMessage(value)
		if !ok {
			continue
		}

		messages = append(messages, message)
	}

	return messages
}

func decodeTransactionPrepareMessage(value string) (*Message, bool) {
	var message *Message

	err := json.Unmarshal([]byte(value), &message)
	if err != nil {
		return nil, false
	}

	return message, true
}

func startScheduleTrimStream(ctx context.Context) {
	go func() {
		client, err := newRedisClient()
		if err != nil {
			logAttrs(ctx, slog.LevelError, "redismq: redis config not registered", causeAttr(err))

			return
		}

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

	defer func() {
		if exception := recover(); exception != nil {
			err := panicError(exception)

			logAttrs(ctx, slog.LevelError, "redismq: trim stream iteration panicked", causeAttr(err), stackAttr(2))

			return
		}
	}()

	for _, topic := range getTopics() {
		queueName := streamname.Queue(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
		queueName = streamname.BackupQueue(topic)
		client.XTrimMaxLen(ctx, queueName, int64(maxLen))
	}

	queueName := streamname.DeathQueue()
	client.XTrimMaxLen(ctx, queueName, int64(maxLen))
}
