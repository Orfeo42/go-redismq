package mqtype

import "context"

type IMessageListener interface {
	GetTopic() string
	GetTag() string
	Consume(ctx context.Context, message *Message) Action
}
