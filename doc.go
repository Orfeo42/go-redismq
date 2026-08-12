// Package redismq is a Redis Streams-backed message queue. It supports
// plain message production and consumption, delayed message delivery,
// transactional message sending with a checker callback, and an
// RPC-style invoke mechanism built on top of the same streams.
//
// Basic usage: build a *Client with New, register listeners with
// RegisterListener, start the consume loops with Start, then publish
// with Send. Always defer Close so background loops and in-flight
// handlers drain before the process exits.
//
//	client, err := redismq.New(redismq.RedisMqConfig{
//		Group: "YourGroup",
//		Addr:  "127.0.0.1:6379",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close(ctx)
//
//	if err := client.RegisterListener(ctx, myListener); err != nil {
//		log.Fatal(err)
//	}
//
//	if err := client.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	if _, err := client.Send(ctx, &redismq.Message{Topic: "topic", Tag: "tag", Body: "hello"}); err != nil {
//		log.Fatal(err)
//	}
//
// All state is owned by *Client, not by package-level globals, so
// multiple independent clients can coexist in the same process without
// sharing a registry, logger, tracer, or Redis connection.
//
// The library emits structured logging through an injected Logger (see
// the With* options). When a logged event carries a root-cause error,
// it is always attached under the key "cause", never "error".
package redismq
