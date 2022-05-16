package messages

import (
	"github.com/Just4Ease/axon/v2/utils"
	"time"
)

type MessageType int

const (
	ErrorMessage MessageType = iota
	RequestMessage
	ResponseMessage
	EventMessage
)

func (d MessageType) String() string {
	return [...]string{"ErrorMessage", "RequestMessage", "ResponseMessage", "EventMessage"}[d]
}

/**
A Message is the payload transmitted over the axon wire, it  represents detailed information about
the communication, likely followed by the body. In the case of an error, body may be nil.
It has the following properties:

Id - the unique identifier for this message

Source - the service this message is coming from
Subject - the service's topic this message is coming from

SpecVersion - the version specification of this message

ContentType - the content type of the body []byte so it can be decoded appropriately using the proper encoder/decoder
Body - the actual payload for use internal your application
Time - when this message was created
*/
type Message struct {
	Id          string            `json:"id"`
	Source      string            `json:"source"`
	Subject     string            `json:"subject"`
	SpecVersion string            `json:"specVersion"`
	ContentType ContentType       `json:"contentType"`
	Error       string            `json:"error"`
	Body        []byte            `json:"body"`
	Header      map[string]string `json:"header"`
	Type        MessageType       `json:"type"`
	Time        time.Time         `json:"time"`
}

type ContentType string

func (c ContentType) String() string {
	return string(c)
}

func NewMessage() *Message {
	return &Message{
		Id:          utils.GenerateRandomString(),
		ContentType: ContentType("application/msgpack"),
		Time:        time.Now(),
	}
}

func (e *Message) WithSpecVersion(version string) *Message {
	e.SpecVersion = version
	return e
}

func (e *Message) WithType(t MessageType) *Message {
	e.Type = t
	return e
}

func (e *Message) WithSource(source string) *Message {
	e.Source = source
	return e
}

func (e *Message) WithId(id string) *Message {
	e.Id = id
	return e
}

func (e *Message) WithTime(t time.Time) *Message {
	e.Time = t
	return e
}

func (e *Message) WithContentType(contentType ContentType) *Message {
	e.ContentType = contentType
	return e
}

func (e *Message) WithSubject(subject string) *Message {
	e.Subject = subject
	return e
}

func (e *Message) WithBody(body []byte) *Message {
	e.Body = body
	return e
}
