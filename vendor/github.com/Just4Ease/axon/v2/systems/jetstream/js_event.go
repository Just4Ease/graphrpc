package jetstream

import (
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/nats-io/nats.go"
	"log"
)

type jsEvent struct {
	m   *nats.Msg
	msg messages.Message
}

func (j jsEvent) Message() *messages.Message {
	return &j.msg
}

func (j jsEvent) Ack() {
	if err := j.m.Ack(); err != nil {
		log.Printf("[axon] failed to NAck jetstream event with the following error: %v", err)
	}
}

func (j jsEvent) NAck() {
	if err := j.m.Nak(); err != nil {
		log.Printf("[axon] failed to NAck jetstream event with the following error: %v", err)
	}
}

func (j jsEvent) Data() []byte {
	return j.m.Data
}

func (j jsEvent) Topic() string {
	return j.m.Subject
}

func newEvent(m *nats.Msg, msg messages.Message) axon.Event {
	return &jsEvent{
		m:   m,
		msg: msg,
	}
}
