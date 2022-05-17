package utils

import (
	"bytes"
	"errors"
	"github.com/vmihailenco/msgpack/v5"
)

func UnPack(in interface{}, target interface{}) error {
	var b []byte
	switch in := in.(type) {
	case []byte:
		b = in
		goto jungle
	// Do something.
	default:
		// Do the rest.
		bNew, err := Marshal(in)
		if err != nil {
			return err
		}

		b = bNew
	}

jungle:
	return Unmarshal(b, target)
}

func Marshal(in interface{}) ([]byte, error) {
	// Do the rest.
	enc := msgpack.GetEncoder()
	enc.SetCustomStructTag("json")
	defer msgpack.PutEncoder(enc)
	var buf bytes.Buffer
	enc.Reset(&buf)
	if err := enc.Encode(in); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Unmarshal(b []byte, target interface{}) error {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.SetCustomStructTag("json")
	dec.SetMapDecoder(func(decoder *msgpack.Decoder) (interface{}, error) {
		return decoder.DecodeUntypedMap()
	})

	dec.Reset(bytes.NewReader(b))
	return dec.Decode(target)
}


// RawMessage is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
type RawMessage []byte

func (m *RawMessage) UnmarshalMSGPACK(data []byte) error {
	if m == nil {
		return errors.New("msgpack.RawMessage: UnmarshalMSGPACK on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

func (m RawMessage) MarshalMSGPACK() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// MarshalJSON returns m as the JSON encoding of m.
func (m RawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (m *RawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

var _ Marshaler = (*RawMessage)(nil)
var _ Unmarshaler = (*RawMessage)(nil)

// Marshaler is the interface implemented by types that
// can marshal themselves into valid JSON.
type Marshaler interface {
	MarshalJSON() ([]byte, error)
	MarshalMSGPACK() ([]byte, error)
}
// Marshaler is the interface implemented by types that
// can marshal themselves into valid JSON.
type Unmarshaler interface {
	UnmarshalJSON(data []byte) error
	UnmarshalMSGPACK(data []byte) error
}

