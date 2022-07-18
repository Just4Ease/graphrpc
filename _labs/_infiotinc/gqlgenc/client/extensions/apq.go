package extensions

import (
	"crypto/sha256"
	"fmt"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
)

const APQKey = "persistedQuery"

type APQExtension struct {
	Version    int64  `json:"version"`
	Sha256Hash string `json:"sha256Hash"`
}

type APQ struct{}

var _ client.AroundRequest = (*APQ)(nil)

func (a *APQ) ExtensionName() string {
	return "apq"
}

func (a *APQ) AroundRequest(req transport.Request, next client.RequestHandler) transport.Response {
	if _, ok := req.Extensions[APQKey]; !ok {
		sum := sha256.Sum256([]byte(req.Query))
		req.Extensions[APQKey] = APQExtension{
			Version:    1,
			Sha256Hash: fmt.Sprintf("%x", sum),
		}
	}

	res := next(transport.Request{
		Context:       req.Context,
		Operation:     req.Operation,
		OperationName: req.OperationName,
		Variables:     req.Variables,
		Extensions:    req.Extensions,
	})

	nres := transport.NewProxyResponse()

	nres.Bind(res, func(opres transport.OperationResponse, send func()) {
		for _, err := range opres.Errors {
			if code, ok := err.Extensions["code"]; ok {
				if code == "PERSISTED_QUERY_NOT_FOUND" {
					nres.Unbind(res)
					go res.Close()

					nres.Bind(next(req), nil)
					return
				}
			}
		}

		send()
	})

	return nres
}
