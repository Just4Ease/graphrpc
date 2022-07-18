package transport

import "sync"

type Response interface {
	Next() bool
	Get() OperationResponse
	Close()
	CloseWithError(err error)
	Err() error
	Done() <-chan struct{}
}

type SendResponse interface {
	Response
	Send(OperationResponse)
}

type responseError struct {
	err error
	m   sync.Mutex
}

func (r *responseError) CloseWithError(err error) {
	if err == nil {
		panic("CloseWithError: err must be non-nil")
	}

	r.m.Lock()
	defer r.m.Unlock()

	if r.err != nil {
		return
	}

	r.err = err
}

func (r *responseError) Err() error {
	r.m.Lock()
	defer r.m.Unlock()

	return r.err
}
