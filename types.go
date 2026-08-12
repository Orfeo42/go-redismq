package redismq

import (
	"github.com/Orfeo42/go-redismq/v3/internal/invoke"
	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

const DefaultConsumerDelayMilliSeconds = mqtype.DefaultConsumerDelayMilliSeconds

type Message = mqtype.Message

func NewRedisMQMessage(topicWrapper MQTopicEnum, body string) *Message {
	return mqtype.NewRedisMQMessage(topicWrapper, body)
}

type Action = mqtype.Action

const (
	CommitMessage  = mqtype.CommitMessage
	ReconsumeLater = mqtype.ReconsumeLater
)

type TransactionStatus = mqtype.TransactionStatus

const (
	CommitTransaction   = mqtype.CommitTransaction
	RollbackTransaction = mqtype.RollbackTransaction
	Unknown             = mqtype.Unknown
)

type MQTopicEnum = mqtype.MQTopicEnum

const TopicInternal = mqtype.TopicInternal
const TagInvoke = mqtype.TagInvoke
const TagBlank = mqtype.TagBlank

type IMessageListener = mqtype.IMessageListener

type IMessageChecker = mqtype.IMessageChecker

type InvokeRequest = invoke.Request

type InvokeResponse = invoke.Response
