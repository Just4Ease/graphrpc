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
		enc := msgpack.GetEncoder()
		enc.SetCustomStructTag("json")
		defer msgpack.PutEncoder(enc)
		var buf bytes.Buffer
		enc.Reset(&buf)
		if err := enc.Encode(in); err != nil {
			return err
		}
		b = buf.Bytes()
	}

jungle:
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.SetCustomStructTag("json")
	dec.SetMapDecoder(func(decoder *msgpack.Decoder) (interface{}, error) {
		return decoder.DecodeUntypedMap()
	})

	dec.Reset(bytes.NewReader(b))
	return dec.Decode(target)
}
