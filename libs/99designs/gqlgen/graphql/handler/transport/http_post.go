package transport

import (
	"mime"
	"fmt"
	"net/http"

	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
)

// POST implements the POST side of the default HTTP transport
// defined in https://github.com/APIs-guru/graphql-over-http#post
type POST struct{ applyMsgpackEncoder bool }

var _ graphql.Transport = POST{applyMsgpackEncoder: false}

func (h POST) Supports(r *http.Request) bool {
	if r.Header.Get("Upgrade") != "" {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return false
	}

	if mediaType == "application/msgpack" {
		h.applyMsgpackEncoder = true
	}

	fmt.Println("transport tested", h.applyMsgpackEncoder, r.Method, mediaType)
	return r.Method == "POST" && mediaType == "application/json"
}

func (h POST) Do(w http.ResponseWriter, r *http.Request, exec graphql.GraphExecutor) {
	if h.applyMsgpackEncoder {
		w.Header().Set("Content-Type", "application/msgpack")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}

	var params *graphql.RawParams
	start := graphql.Now()
	if err := decode(h.applyMsgpackEncoder, r.Body, &params); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeErrorf(h.applyMsgpackEncoder, w, "body could not be decoded: "+err.Error())
		return
	}

	params.Headers = r.Header

	params.ReadTime = graphql.TraceTiming{
		Start: start,
		End:   graphql.Now(),
	}
	
	rc, err := exec.CreateOperationContext(r.Context(), params)
	if err != nil {
		w.WriteHeader(statusFor(err))
		resp := exec.DispatchError(graphql.WithOperationContext(r.Context(), rc), err)
		writeResponse(h.applyMsgpackEncoder, w, resp)
		return
	}
	responses, ctx := exec.DispatchOperation(r.Context(), rc)
	writeResponse(h.applyMsgpackEncoder, w, responses(ctx))
}
