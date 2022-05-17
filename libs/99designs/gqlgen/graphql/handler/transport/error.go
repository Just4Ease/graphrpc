package transport

import (
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/graphrpc/utils"
	"net/http"

	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// SendError sends a best effort error to a raw response writer. It assumes the client can understand the standard
// json error response
func SendError(enableMsgpackEncoding bool, w http.ResponseWriter, code int, errors ...*gqlerror.Error) {
	w.WriteHeader(code)
	var b []byte
	var err error

	r := &graphql.Response{Errors: errors}
	if enableMsgpackEncoding {
		b, err = utils.Marshal(r)
	} else {
		b, err = json.Marshal(r)
	}

	if err != nil {
		panic(err)
	}
	w.Write(b)
}

// SendErrorf wraps SendError to add formatted messages
func SendErrorf(enableMsgpackEncoding bool, w http.ResponseWriter, code int, format string, args ...interface{}) {
	SendError(enableMsgpackEncoding, w, code, &gqlerror.Error{Message: fmt.Sprintf(format, args...)})
}
