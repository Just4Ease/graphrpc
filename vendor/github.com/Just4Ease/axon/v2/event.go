package axon

import "github.com/Just4Ease/axon/v2/messages"

type Event interface {
	Ack()
	NAck()
	Message() *messages.Message
	Data() []byte
	Topic() string
}
