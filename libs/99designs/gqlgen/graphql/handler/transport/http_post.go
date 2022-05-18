package transport

import (
	"github.com/borderlesshq/graphrpc/utils"
	"net/http"

	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
)

// POST implements the POST side of the default HTTP transport
// defined in https://github.com/APIs-guru/graphql-over-http#post
type POST struct{}

var _ graphql.Transport = POST{}

func (h POST) Supports(r *http.Request) bool {
	if r.Header.Get("Upgrade") != "" {
		return false
	}
	return r.Method == "POST"
}

func (h POST) Do(w http.ResponseWriter, r *http.Request, exec graphql.GraphExecutor) {
	applyMsgpackEncoder := utils.UseMsgpackEncoding(r)

	if applyMsgpackEncoder {
		w.Header().Set("Content-Type", "application/msgpack")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}

	var params *graphql.RawParams
	start := graphql.Now()
	if err := decode(applyMsgpackEncoder, r.Body, &params); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeErrorf(applyMsgpackEncoder, w, "body could not be decoded: "+err.Error())
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
		writeResponse(applyMsgpackEncoder, w, resp)
		return
	}
	responses, ctx := exec.DispatchOperation(r.Context(), rc)
	writeResponse(applyMsgpackEncoder, w, responses(ctx))
}
