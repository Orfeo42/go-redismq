package redismq

import (
	"errors"

	"github.com/Orfeo42/go-redismq/v3/internal/consumer"
	"github.com/Orfeo42/go-redismq/v3/internal/delayqueue"
	"github.com/Orfeo42/go-redismq/v3/internal/invoke"
	"github.com/Orfeo42/go-redismq/v3/internal/producer"
	"github.com/Orfeo42/go-redismq/v3/internal/registry"
)

var (
	ErrNilChecker       = registry.ErrNilChecker
	ErrDuplicateChecker = registry.ErrDuplicateChecker
)

var (
	ErrConfigAddrBlank  = errors.New("redismq: config addr is blank")
	ErrConfigGroupBlank = errors.New("redismq: config group is blank")
)

var (
	ErrMethodNameBlank         = invoke.ErrMethodNameBlank
	ErrHandlerNil              = invoke.ErrHandlerNil
	ErrMethodAlreadyRegistered = invoke.ErrMethodAlreadyRegistered
)

var (
	ErrNilListener       = registry.ErrNilListener
	ErrTooManyTopics     = registry.ErrTooManyTopics
	ErrInvalidTopic      = registry.ErrInvalidTopic
	ErrDuplicateListener = registry.ErrDuplicateListener
)

var (
	ErrMessageIDNotBlank              = producer.ErrMessageIDNotBlank
	ErrBlankTag                       = producer.ErrBlankTag
	ErrDelayNotSupportedInTransaction = producer.ErrDelayNotSupportedInTransaction
	ErrUnknownTransactionStatus       = producer.ErrUnknownTransactionStatus
)

var ErrConsumerNameUnresolved = consumer.ErrConsumerNameUnresolved

var ErrDeliverTimeInThePast = delayqueue.ErrDeliverTimeInThePast
