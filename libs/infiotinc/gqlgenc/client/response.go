package client

type Response interface {
	Get() OperationResponse
	Close()
	Err() error
	Done() <-chan struct{}
}

type SendResponse interface {
	Response
	Send(OperationResponse)
}

type responseError struct {
	err error
}
