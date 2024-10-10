package go_redismq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/redis/go-redis/v9"
	"time"
)

func listenForResponse(ctx context.Context, req *InvoiceRequest, responseChan chan *InvoiceResponse) {
	defer close(responseChan)

	client := redis.NewClient(GetRedisConfig())
	// Close Conn
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("MQStream Closs Redis Stream Client error:%s\n", err.Error())
		}
	}(client)
	replyChannel := fmt.Sprintf("%s_%s:%s", req.Group, req.Method, req.MessageId)
	pubSub := client.Subscribe(ctx, replyChannel)
	defer func(pubSub *redis.PubSub) {
		err := pubSub.Close()
		if err != nil {
			g.Log().Errorf(ctx, "Error pubSub: %s\n", err.Error())
		}
	}(pubSub)

	ch := pubSub.Channel()
	for msg := range ch {
		var res *InvoiceResponse
		err := json.Unmarshal([]byte(msg.Payload), &res)
		if err != nil {
			g.Log().Errorf(ctx, "Error deserializing response: %s\n", err.Error())
			return
		}

		responseChan <- res
		return
	}
}

func Invoke(ctx context.Context, req *InvoiceRequest, timeoutSeconds int) *InvoiceResponse {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}
	invokeId := fmt.Sprintf("%s%d", GenerateRandomAlphanumeric(6), CurrentTimeMillis())
	req.MessageId = invokeId
	// todo mark reply faster than listen
	send, err := Send(&Message{
		Topic: "internal",
		Tag:   "invoke",
		Body:  MarshalToJsonString(req),
	})
	if err != nil {
		return &InvoiceResponse{
			Status:   false,
			Response: fmt.Sprintf("Invoke error:%s", err.Error()),
		}
	} else if !send {
		return &InvoiceResponse{
			Status:   false,
			Response: fmt.Sprintf("Invoke send failed"),
		}
	}
	responseChan := make(chan *InvoiceResponse)
	go listenForResponse(ctx, req, responseChan)
	go func() {
		time.Sleep(time.Duration(timeoutSeconds) * time.Second)
		responseChan <- &InvoiceResponse{
			Status:   false,
			Response: "Timeout",
		}
	}()
	select {
	case <-ctx.Done():
		return &InvoiceResponse{
			Status:   false,
			Response: "Invoke context timeout",
		}
	case response := <-responseChan:
		return response
	}
}
