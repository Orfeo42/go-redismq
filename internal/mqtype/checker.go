package mqtype

type IMessageChecker interface {
	GetTopic() string
	GetTag() string
	Checker(message *Message) TransactionStatus
}
