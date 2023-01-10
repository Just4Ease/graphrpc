package graphql

import (
	"context"
	"fmt"
	"github.com/borderlesshq/graphrpc/utils"

	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Errors are intentionally serialized first based on the advice in
// https://github.com/facebook/graphql/commit/7b40390d48680b15cb93e02d46ac5eb249689876#diff-757cea6edf0288677a9eea4cfc801d87R107
// and https://github.com/facebook/graphql/pull/384
type Response struct {
	Errors     gqlerror.List          `json:"errors,omitempty" msgpack:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty" msgpack:"extensions,omitempty"`
	Data       utils.RawMessage       `json:"data" msgpack:"data"`
}

func ErrorResponse(ctx context.Context, messages string, args ...interface{}) *Response {
	return &Response{
		Errors: gqlerror.List{{Message: fmt.Sprintf(messages, args...)}},
	}
}
