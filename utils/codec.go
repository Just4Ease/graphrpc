package utils

import (
	"errors"
	"github.com/fxamacker/cbor/v2" // imports as cbor
	"mime"
	"net/http"
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
	//enc := msgpack.GetEncoder()
	//enc.SetCustomStructTag("json")
	//defer msgpack.PutEncoder(enc)
	//var buf bytes.Buffer
	//enc.Reset(&buf)
	//if err := enc.Encode(in); err != nil {
	//	return nil, err
	//}
	//return buf.Bytes(), nil
	return cbor.Marshal(in)
}

func Unmarshal(b []byte, target interface{}) error {
	//dec := msgpack.GetDecoder()
	//defer msgpack.PutDecoder(dec)
	//dec.SetCustomStructTag("json")
	//dec.SetMapDecoder(func(decoder *msgpack.Decoder) (interface{}, error) {
	//	return decoder.DecodeUntypedMap()
	//})
	//
	//dec.Reset(bytes.NewReader(b))
	//return dec.Decode(target)
	return cbor.Unmarshal(b, target)
}

// RawMessage is a raw encoded JSON or msgpack value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON or msgpack decoding or precompute a JSON or msgpack encoding.
type RawMessage []byte

func (m *RawMessage) UnmarshalCBOR(data []byte) error {
	if m == nil {
		return errors.New("cbor.RawMessage: UnmarshalCBOR on nil pointer")
	}
	*m = data
	return nil
}

func (m RawMessage) MarshalCBOR() ([]byte, error) {
	if m == nil {
		return nil, nil
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
	MarshalCBOR() ([]byte, error)
}

// Unmarshaler is the interface implemented by types that
// can marshal themselves into valid JSON.
type Unmarshaler interface {
	UnmarshalJSON(data []byte) error
	UnmarshalCBOR(data []byte) error
}

//func (r RawMessage) Encoding() string {
//
//}

func UseMsgpackEncoding(r *http.Request) bool {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return mediaType == "application/cbor"
}

//func SelectEncoder(contentType string) Marshaler {
//	if strings.ContainsAny(contentType, "cbor") {
//		// use cbor.
//	}
//
//	if strings.ContainsAny(contentType, "msgpack") {
//		// use msgpack.
//	}
//
//	return nil
//}
//
//func SelectDecoder(contentType string) Unmarshaler {
//	return nil
//}
