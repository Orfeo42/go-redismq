package mqtype

import (
	"testing"
)

func TestPassStreamMessage(t *testing.T) {
	t.Run("round trip through ToStreamAddArgsValues survives all fields", func(t *testing.T) {
		original := &Message{
			Topic:                     "topic1",
			Tag:                       "tag1",
			Body:                      "body1",
			Key:                       "key1",
			StartDeliverTime:          1234,
			ReconsumeTimes:            2,
			CustomData:                map[string]any{"foo": "bar"},
			ConsumerDelayMilliSeconds: 3000,
		}
		original.SetTraceID("trace-xyz")

		args, err := original.ToStreamAddArgsValues("stream1")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		values, ok := args.Values.(map[string]any)
		if !ok {
			t.Fatalf("expected args.Values to be map[string]any, got %T", args.Values)
		}

		fresh := &Message{}

		if _, err := fresh.PassStreamMessage(values); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if fresh.Topic != original.Topic {
			t.Fatalf("expected topic %q, got %q", original.Topic, fresh.Topic)
		}

		if fresh.Tag != original.Tag {
			t.Fatalf("expected tag %q, got %q", original.Tag, fresh.Tag)
		}

		if fresh.Body != original.Body {
			t.Fatalf("expected body %q, got %q", original.Body, fresh.Body)
		}

		if fresh.Key != original.Key {
			t.Fatalf("expected key %q, got %q", original.Key, fresh.Key)
		}

		if fresh.StartDeliverTime != original.StartDeliverTime {
			t.Fatalf("expected startDeliverTime %d, got %d", original.StartDeliverTime, fresh.StartDeliverTime)
		}

		if fresh.ReconsumeTimes != original.ReconsumeTimes {
			t.Fatalf("expected reconsumeTimes %d, got %d", original.ReconsumeTimes, fresh.ReconsumeTimes)
		}

		if fresh.ConsumerDelayMilliSeconds != original.ConsumerDelayMilliSeconds {
			t.Fatalf("expected consumerDelayMilliSeconds %d, got %d", original.ConsumerDelayMilliSeconds, fresh.ConsumerDelayMilliSeconds)
		}

		if fresh.CustomData["foo"] != "bar" {
			t.Fatalf("expected CustomData[foo] %q, got %v", "bar", fresh.CustomData["foo"])
		}

		if fresh.TraceID() != "trace-xyz" {
			t.Fatalf("expected trace id %q, got %q", "trace-xyz", fresh.TraceID())
		}
	})

	t.Run("consumerDelayMilliSeconds key absent leaves pre-set value untouched", func(t *testing.T) {
		message := &Message{ConsumerDelayMilliSeconds: 4242}

		value := map[string]any{
			"topic":    "t",
			"tag":      "tag1",
			"body":     "b",
			"metadata": `{"reconsumeTimes":0,"reconsumeMax":0,"startDeliverTime":0,"sendTime":0,"key":""}`,
		}

		if _, err := message.PassStreamMessage(value); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if message.ConsumerDelayMilliSeconds != 4242 {
			t.Fatalf("expected consumerDelayMilliSeconds to remain %d, got %d", 4242, message.ConsumerDelayMilliSeconds)
		}
	})

	t.Run("consumerDelayMilliSeconds key present overwrites pre-set value", func(t *testing.T) {
		message := &Message{ConsumerDelayMilliSeconds: 4242}

		value := map[string]any{
			"topic":    "t",
			"tag":      "tag1",
			"body":     "b",
			"metadata": `{"reconsumeTimes":0,"reconsumeMax":0,"startDeliverTime":0,"sendTime":0,"key":"","consumerDelayMilliSeconds":0}`,
		}

		if _, err := message.PassStreamMessage(value); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if message.ConsumerDelayMilliSeconds != 0 {
			t.Fatalf("expected consumerDelayMilliSeconds to be overwritten to %d, got %d", 0, message.ConsumerDelayMilliSeconds)
		}
	})

	t.Run("malformed metadata leaves message fields unmutated and returns an error", func(t *testing.T) {
		message := &Message{
			Topic:                     "pre-topic",
			Tag:                       "pre-tag",
			ReconsumeTimes:            9,
			ConsumerDelayMilliSeconds: 111,
		}

		value := map[string]any{
			"topic":    "t",
			"tag":      "tag1",
			"body":     "b",
			"metadata": `{not valid json`,
		}

		stack, err := message.PassStreamMessage(value)
		if err == nil {
			t.Fatal("expected an error for malformed metadata")
		}

		if stack != nil {
			t.Fatalf("expected no panic stack for a plain decode error, got %v", stack)
		}

		if message.ReconsumeTimes != 9 {
			t.Fatalf("expected reconsumeTimes to remain %d, got %d", 9, message.ReconsumeTimes)
		}

		if message.ConsumerDelayMilliSeconds != 111 {
			t.Fatalf("expected consumerDelayMilliSeconds to remain %d, got %d", 111, message.ConsumerDelayMilliSeconds)
		}
	})
}

func TestGetUniqueKey(t *testing.T) {
	t.Run("derives unique key from message id when absent", func(t *testing.T) {
		message := &Message{MessageId: "id-1"}

		if got := message.GetUniqueKey(); got != "id-1" {
			t.Fatalf("expected %q, got %q", "id-1", got)
		}
	})

	t.Run("preserves an explicitly set unique key", func(t *testing.T) {
		message := &Message{
			MessageId:  "id-1",
			CustomData: map[string]any{"uniqueKey": "explicit"},
		}

		if got := message.GetUniqueKey(); got != "explicit" {
			t.Fatalf("expected %q, got %q", "explicit", got)
		}
	})
}

func TestIsBroadcastingMessage(t *testing.T) {
	t.Run("true when messageModel is BROADCASTING", func(t *testing.T) {
		message := &Message{CustomData: map[string]any{"messageModel": "BROADCASTING"}}

		if !message.IsBroadcastingMessage() {
			t.Fatal("expected broadcasting message")
		}
	})

	t.Run("false when messageModel absent", func(t *testing.T) {
		message := &Message{}

		if message.IsBroadcastingMessage() {
			t.Fatal("expected non-broadcasting message")
		}
	})
}

func TestTraceID(t *testing.T) {
	t.Run("empty when never set", func(t *testing.T) {
		message := &Message{}

		if message.TraceID() != "" {
			t.Fatalf("expected empty trace id, got %q", message.TraceID())
		}
	})

	t.Run("round trips through SetTraceID", func(t *testing.T) {
		message := &Message{}
		message.SetTraceID("trace-1")

		if message.TraceID() != "trace-1" {
			t.Fatalf("expected %q, got %q", "trace-1", message.TraceID())
		}
	})

	t.Run("initializes CustomData when nil", func(t *testing.T) {
		message := &Message{}
		message.SetTraceID("trace-2")

		if message.CustomData == nil {
			t.Fatal("expected CustomData to be initialized")
		}
	})
}

func TestNewRedisMQMessage(t *testing.T) {
	topic := MQTopicEnum{Topic: "t", Tag: "tag1"}

	message := NewRedisMQMessage(topic, "body1")

	if message.Topic != "t" {
		t.Fatalf("expected topic %q, got %q", "t", message.Topic)
	}

	if message.Tag != "tag1" {
		t.Fatalf("expected tag %q, got %q", "tag1", message.Tag)
	}

	if message.Body != "body1" {
		t.Fatalf("expected body %q, got %q", "body1", message.Body)
	}
}
