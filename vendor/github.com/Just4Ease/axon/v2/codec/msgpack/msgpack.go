// Package msgpack provides a msgpack codec
package msgpack

import (
	"github.com/Just4Ease/axon/v2/codec"
	"github.com/vmihailenco/msgpack/v5"
	"io"
)

type Codec struct {
	Conn    io.ReadWriteCloser
	Encoder *msgpack.Encoder
	Decoder *msgpack.Decoder
}

func (c *Codec) Read(b interface{}) error {
	if b == nil {
		return nil
	}
	return c.Decoder.Decode(b)
}

func (c *Codec) Write(b interface{}) error {
	if b == nil {
		return nil
	}
	return c.Encoder.Encode(b)
}

func (c *Codec) Close() error {
	return c.Conn.Close()
}

func (c *Codec) String() string {
	return "msgpack"
}

func NewCodec(c io.ReadWriteCloser) codec.Codec {
	return &Codec{
		Conn:    c,
		Decoder: msgpack.NewDecoder(c),
		Encoder: msgpack.NewEncoder(c),
	}
}
