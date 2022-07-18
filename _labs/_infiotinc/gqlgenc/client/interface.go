package client

import (
	"github.com/infiotinc/gqlgenc/client/transport"
)

type (
	Extension interface{
		ExtensionName() string
	}

	RequestHandler func(req transport.Request) transport.Response

	AroundRequest interface {
		AroundRequest(req transport.Request, next RequestHandler) transport.Response
	}
)

