package utils

import (
	"bytes"
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
