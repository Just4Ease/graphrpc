package transport

import "sync"

type SingleResponse struct {
	or OperationResponse

	calledNext bool
	dm         sync.Mutex
	dc         chan struct{}

	responseError
}

func NewSingleResponse(or OperationResponse) *SingleResponse {
	return &SingleResponse{or: or}
}

func NewErrorResponse(err error) Response {
	res := NewSingleResponse(OperationResponse{})
	res.CloseWithError(err)

	return res
}

func (r *SingleResponse) Next() bool {
	if r.calledNext || r.err != nil {
		return false
	}

	r.calledNext = true

	return true
}

func (r *SingleResponse) Get() OperationResponse {
	return r.or
}

func (r *SingleResponse) Close() {}

func (r *SingleResponse) Done() <-chan struct{} {
	r.dm.Lock()
	if r.dc == nil {
		r.dc = make(chan struct{})
		close(r.dc)
	}
	r.dm.Unlock()

	return r.dc
}
