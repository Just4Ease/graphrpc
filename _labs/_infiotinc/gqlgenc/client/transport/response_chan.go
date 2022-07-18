package transport

import "sync"

type ChanResponse struct {
	responseError

	ch     chan OperationResponse
	close  func() error
	closed bool

	cor OperationResponse
	m   sync.Mutex
	dc  chan struct{}
}

func NewChanResponse(onClose func() error) *ChanResponse {
	return &ChanResponse{
		ch:    make(chan OperationResponse),
		dc:    make(chan struct{}),
		close: onClose,
	}
}

func (r *ChanResponse) Next() bool {
	if r.err != nil {
		return false
	}

	or, ok := <-r.ch
	r.cor = or
	return ok
}

func (r *ChanResponse) Get() OperationResponse {
	return r.cor
}

func (r *ChanResponse) Close() {
	if r.close != nil {
		r.err = r.close()
	}
	r.CloseCh()
}

func (r *ChanResponse) CloseWithError(err error) {
	r.responseError.CloseWithError(err)
	r.CloseCh()

	if r.close != nil {
		_ = r.close()
	}
}

func (r *ChanResponse) CloseCh() {
	r.m.Lock()
	defer r.m.Unlock()

	if r.closed {
		return
	}

	close(r.ch)
	close(r.dc)
	r.closed = true
}

func (r *ChanResponse) Done() <-chan struct{} {
	return r.dc
}

func (r *ChanResponse) Send(op OperationResponse) {
	r.m.Lock()
	defer r.m.Unlock()

	select {
	case r.ch <- op:
	case <-r.Done():
	}
}
