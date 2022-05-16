package msgpack

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
)

type Marshaler struct{}

func (Marshaler) Marshal(v interface{}) ([]byte, error) {
	enc := msgpack.GetEncoder()

	enc.SetCustomStructTag("json")

	var buf bytes.Buffer
	enc.Reset(&buf)

	err := enc.Encode(v)
	b := buf.Bytes()

	msgpack.PutEncoder(enc)

	if err != nil {
		return nil, err
	}
	return b, err
}

func (Marshaler) Unmarshal(d []byte, v interface{}) error {
	dec := msgpack.GetDecoder()

	dec.SetCustomStructTag("json")

	dec.Reset(bytes.NewReader(d))
	defer msgpack.PutDecoder(dec)
	err := dec.Decode(v)
	return err
}

func (Marshaler) String() string {
	return "msgpack"
}
