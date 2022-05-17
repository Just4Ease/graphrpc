package transport

import (
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/graphrpc/utils"
	"github.com/vmihailenco/msgpack/v5"
	"io"

	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func writeJson(w io.Writer, response *graphql.Response) {
	b, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(b)
}

func writeMsgpack(w io.Writer, response *graphql.Response) {
	b, err := utils.Marshal(response)
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(b)
}

func writeResponse(enableMsgpackEncoding bool, w io.Writer, response *graphql.Response) {
	if enableMsgpackEncoding {
		writeMsgpack(w, response)
	} else {
		writeJson(w, response)
	}
}

func writeError(enableMsgpackEncoding bool, w io.Writer, msg string) {
	writeResponse(enableMsgpackEncoding, w, &graphql.Response{Errors: gqlerror.List{{Message: msg}}})
}

func writeErrorf(enableMsgpackEncoding bool, w io.Writer, format string, args ...interface{}) {
	writeResponse(enableMsgpackEncoding, w, &graphql.Response{Errors: gqlerror.List{{Message: fmt.Sprintf(format, args...)}}})
}

func writeGraphqlError(enableMsgpackEncoding bool, w io.Writer, err ...*gqlerror.Error) {
	writeResponse(enableMsgpackEncoding, w, &graphql.Response{Errors: err})
}

func decode(enableMsgpackEncoding bool, r io.Reader, val interface{}) error {
	if enableMsgpackEncoding {
		return msgpackDecode(r, val)
	}

	return jsonDecode(r, val)
}

func jsonDecode(r io.Reader, val interface{}) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	return dec.Decode(val)
}

func msgpackDecode(r io.Reader, val interface{}) error {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.SetCustomStructTag("json")
	dec.SetMapDecoder(func(decoder *msgpack.Decoder) (interface{}, error) {
		return decoder.DecodeUntypedMap()
	})

	dec.Reset(r)
	return dec.Decode(val)
}
