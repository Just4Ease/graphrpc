package extensions

import (
	"crypto/sha256"
	"fmt"
	"github.com/borderlesshq/graphrpc/libs/infiotinc/gqlgenc/client"
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

func (a *APQ) AroundRequest(req client.Request, next client.RequestHandler) client.Response {
	if _, ok := req.Extensions[APQKey]; !ok {
		sum := sha256.Sum256([]byte(req.Query))
		req.Extensions[APQKey] = APQExtension{
			Version:    1,
			Sha256Hash: fmt.Sprintf("%x", sum),
		}
	}

	res := next(client.Request{
		Context:       req.Context,
		Operation:     req.Operation,
		OperationName: req.OperationName,
		Variables:     req.Variables,
		Extensions:    req.Extensions,
	})

	nres := client.NewProxyResponse()

	nres.Bind(res, func(opres client.OperationResponse, send func()) {
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
