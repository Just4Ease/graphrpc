// Package codec is an interface for encoding messages
package codec

import (
	"errors"
)

var (
	ErrInvalidMessage = errors.New("invalid message")
)

// Codec encodes/decodes various types of messages used within axon
// ReadHeader and ReadBody are called in pairs to read requests/responses
// from the connection. Close is called when finished with the
// connection. ReadBody may be called with a nil argument to force the
// body to be read and discarded.
type Codec interface {
	Reader
	Writer
	Close() error
	String() string
}

type Reader interface {
	Read(interface{}) error
}

type Writer interface {
	Write(interface{}) error
}

// Marshaler is a simple encoding interface used for the broker/transport
// where headers are not supported by the underlying implementation.
type Marshaler interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
	String() string
}
