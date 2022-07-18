package transport

import (
	"encoding/json"
	"fmt"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Mock map[string]Func

func (m Mock) Request(req Request) Response {
	h, ok := m[req.Query]
	if !ok {
		panic(fmt.Sprintf("query not handled: \n%v", req.Query))
	}

	return h(req)
}

func NewMockOperationResponse(v interface{}, errs gqlerror.List) OperationResponse {
	data, _ := json.Marshal(v)

	return OperationResponse{
		Data:   data,
		Errors: errs,
	}
}
