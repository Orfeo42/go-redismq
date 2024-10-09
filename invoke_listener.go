package go_redismq

import (
	"context"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
)

type InvoiceRequest struct {
	Group  string
	Method string
	Param  []byte
}

type MessageInvokeListener struct {
}

func (t MessageInvokeListener) GetTopic() string {
	return "internal"
}

func (t MessageInvokeListener) GetTag() string {
	return "invoke"
}

func (t MessageInvokeListener) Consume(ctx context.Context, message *Message) Action {
	if message.CustomData != nil && fmt.Sprintf("%s", message.CustomData["Group"]) != Group {

	} else {
		g.Log().Debugf(ctx, "MessageInvokeListener ignore invoice methodName:%s")
	}
	return CommitMessage
}

func init() {
	RegisterListener(New())
	fmt.Println("MessageInvokeListener RegisterListener")
}

func New() *MessageInvokeListener {
	return &MessageInvokeListener{}
}

var invokeMap map[string]func(ctx context.Context, param []byte) (response []byte, err error)

func RegisterInvoke(methodName string, op func(ctx context.Context, param []byte) (response []byte, err error)) {
	if len(methodName) <= 0 || op == nil {
		fmt.Printf("MQStream RegisterInvoke error methodName:%s or op:%p is nil", methodName, op)
	} else if _, ok := invokeMap[methodName]; ok {
		fmt.Printf("MQStream RegisterInvoke error exist Old One:%s for op:%p\n", methodName, op)
	} else {
		invokeMap[methodName] = op
	}
}
