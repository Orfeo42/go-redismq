package go_redismq

import (
	"context"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/redis/go-redis/v9"
)

type InvoiceRequest struct {
	MessageId string `json:"messageId"`
	Group     string `json:"group"`
	Method    string `json:"method"`
	Request   string `json:"request"`
}

type InvoiceResponse struct {
	Status   bool   `json:"status"`
	Response string `json:"response"`
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
	var req *InvoiceRequest
	err := UnmarshalFromJsonString(message.Body, &req)
	if err != nil {
		g.Log().Errorf(ctx, "MessageInvokeListener UnmarshalFromJsonString Body error %s", err.Error())
		//ignore
		return CommitMessage
	}
	if req == nil || req.Group != Group || len(req.MessageId) == 0 || len(req.Method) == 0 {
		g.Log().Errorf(ctx, "MessageInvokeListener Invalid Request:%s", MarshalToJsonString(req))
		//ignore
		return CommitMessage
	}
	res := &InvoiceResponse{}
	client := redis.NewClient(GetRedisConfig())
	// Close Conn
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("MQStream Closs Redis Stream Client error:%s\n", err.Error())
		}
	}(client)
	replyChannel := fmt.Sprintf("%s_%s:%s", req.Group, req.Method, req.MessageId)
	if op, ok := invokeMap[req.Method]; ok {
		// invoke method
		response, err := op(ctx, req.Request)
		if err != nil {
			res.Response = fmt.Sprintf("%s", err.Error())
			res.Status = false
			client.Publish(ctx, replyChannel, MarshalToJsonString(res))
		} else {
			res.Response = response
			res.Status = true
			client.Publish(ctx, replyChannel, MarshalToJsonString(res))
		}
	} else {
		res.Response = "error: method not found"
		res.Status = false
		client.Publish(ctx, replyChannel, MarshalToJsonString(res))
	}
	return CommitMessage
}

func init() {
	RegisterListener(&MessageInvokeListener{})
	fmt.Println("MessageInvokeListener RegisterListener")
}

var invokeMap map[string]func(ctx context.Context, param string) (response string, err error)

func RegisterInvoke(methodName string, op func(ctx context.Context, param string) (response string, err error)) {
	if len(methodName) <= 0 || op == nil {
		fmt.Printf("MQStream RegisterInvoke error methodName:%s or op:%p is nil\n", methodName, op)
	} else if _, ok := invokeMap[methodName]; ok {
		fmt.Printf("MQStream RegisterInvoke error exist Old One:%s for op:%p\n", methodName, op)
	} else {
		invokeMap[methodName] = op
		fmt.Printf("MQStream RegisterInvoke methodName:%s for op:%p\n", methodName, op)
	}
}
