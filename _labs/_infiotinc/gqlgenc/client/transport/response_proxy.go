package transport

import (
	"sync"
	"sync/atomic"
)

type proxyTarget SendResponse

type ProxyResponse struct {
	proxyTarget
	binds    []Response
	m        sync.RWMutex
	inFlight int32
}

func (p *ProxyResponse) Bound(res Response) bool {
	p.m.RLock()
	defer p.m.RUnlock()

	for _, b := range p.binds {
		if b == res {
			return true
		}
	}

	return false
}

func (p *ProxyResponse) Bind(res Response, onOpres func(response OperationResponse, send func())) {
	atomic.AddInt32(&p.inFlight, 1)

	if onOpres == nil {
		onOpres = func(_ OperationResponse, send func()) {
			send()
		}
	}

	p.m.Lock()
	p.binds = append(p.binds, res)
	p.m.Unlock()

	go func() {
		go func() {
			select {
			case <-res.Done():
			case <-p.Done():
				res.Close()
			}
		}()

		for res.Next() {
			opres := res.Get()

			onOpres(opres, func() {
				if p.Bound(res) {
					p.Send(opres)
				}
			})

			if !p.Bound(res) {
				break
			}
		}

		if p.Bound(res) {
			if err := res.Err(); err != nil {
				p.CloseWithError(err)
			}
		}

		atomic.AddInt32(&p.inFlight, -1)
		p.Unbind(res)
	}()
}

func (p *ProxyResponse) Unbind(res Response) {
	p.m.Lock()
	binds := make([]Response, 0)
	for _, b := range p.binds {
		if b == res {
			continue
		}

		binds = append(binds, b)
	}
	p.binds = binds
	p.m.Unlock()

	if atomic.LoadInt32(&p.inFlight) == 0 {
		p.Close()
	}
}

func NewProxyResponse() *ProxyResponse {
	return &ProxyResponse{
		proxyTarget: NewChanResponse(nil),
	}
}
