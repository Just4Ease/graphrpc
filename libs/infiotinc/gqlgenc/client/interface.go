package client

type (
	Extension interface {
		ExtensionName() string
	}

	RequestHandler func(req Request) Response

	AroundRequest interface {
		AroundRequest(req Request, next RequestHandler) Response
	}
)
