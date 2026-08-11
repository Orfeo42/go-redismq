package go_redismq

import (
	"context"
	"testing"
)

func TestPassStreamMessage(t *testing.T) {
	t.Run("round trip through toStreamAddArgsValues survives all fields", func(t *testing.T) {
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
		original.setTraceID("trace-xyz")

		args := original.toStreamAddArgsValues("stream1")

		values, ok := args.Values.(map[string]any)
		if !ok {
			t.Fatalf("expected args.Values to be map[string]any, got %T", args.Values)
		}

		fresh := &Message{}
		fresh.passStreamMessage(context.Background(), values)

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

		if fresh.traceID() != "trace-xyz" {
			t.Fatalf("expected trace id %q, got %q", "trace-xyz", fresh.traceID())
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

		message.passStreamMessage(context.Background(), value)

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

		message.passStreamMessage(context.Background(), value)

		if message.ConsumerDelayMilliSeconds != 0 {
			t.Fatalf("expected consumerDelayMilliSeconds to be overwritten to %d, got %d", 0, message.ConsumerDelayMilliSeconds)
		}
	})

	t.Run("malformed metadata leaves message fields unmutated and does not panic", func(t *testing.T) {
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

		message.passStreamMessage(context.Background(), value)

		if message.ReconsumeTimes != 9 {
			t.Fatalf("expected reconsumeTimes to remain %d, got %d", 9, message.ReconsumeTimes)
		}

		if message.ConsumerDelayMilliSeconds != 111 {
			t.Fatalf("expected consumerDelayMilliSeconds to remain %d, got %d", 111, message.ConsumerDelayMilliSeconds)
		}
	})
}
