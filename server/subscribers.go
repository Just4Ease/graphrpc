package server

import (
	"bufio"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

type RPCResponseWriter struct {
	req             *http.Request
	header          http.Header
	writeIsComplete bool

	// bufw writes to checkConnErrorWriter{c}, which populates werr on error.
	bufw          *bufio.Writer
	contentLength int
	status        int
	Done          chan bool
}

func (r RPCResponseWriter) Read(p []byte) (n int, err error) {
	panic("we need funding comrade.")
}

func (r RPCResponseWriter) Header() http.Header {
	var t http.Header
	for k, vv := range r.header {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			if t == nil {
				t = make(http.Header)
			}
			t[strings.TrimPrefix(k, http.TrailerPrefix)] = vv
		}
	}
	//for _, k := range w.trailers {
	//	if t == nil {
	//		t = make(http.Header)
	//	}
	//	for _, v := range r.header[k] {
	//		t.Add(k, v)
	//	}
	//}
	return t
}

func (r RPCResponseWriter) Write(p []byte) (n int, err error) {
	if r.req.Method == "HEAD" {
		// Eat writes.
		return len(p), nil
	}
	//if r.chunking {
	//	_, err = fmt.Fprintf(r.bufw, "%x\r\n", len(p))
	//	if err != nil {
	//		return
	//	}
	//}

	n, err = r.bufw.Write(p)
	return
}
func (r RPCResponseWriter) WriteHeader(statusCode int) {
	if cl := r.header.Get("Content-Length"); cl != "" {
		v, err := strconv.ParseInt(cl, 10, 64)
		if err == nil && v >= 0 {
			r.contentLength = int(v)
		} else {
			r.header.Del("Content-Length")
		}
	}

	r.status = statusCode
}

// Flush sends any buffered data to the client.
func (r RPCResponseWriter) Flush() {
	r.bufw.Flush()
}

func (r RPCResponseWriter) Close() error {
	r.Flush()
	r.Done <- true
	return nil
}

//var (
//	crlf       = []byte("\r\n")
//	colonSpace = []byte(": ")
//)

var (
	bufioReaderPool   sync.Pool
	bufioWriter2kPool sync.Pool
	bufioWriter4kPool sync.Pool
)

var copyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return &b
	},
}

func bufioWriterPool(size int) *sync.Pool {
	switch size {
	case 2 << 10:
		return &bufioWriter2kPool
	case 4 << 10:
		return &bufioWriter4kPool
	}
	return nil
}

func newBufioReader(r io.Reader) *bufio.Reader {
	if v := bufioReaderPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	// Note: if this reader size is ever changed, update
	// TestHandlerBodyClose's assumptions.
	return bufio.NewReader(r)
}

func putBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	bufioReaderPool.Put(br)
}

func newBufioWriterSize(w io.Writer, size int) *bufio.Writer {
	pool := bufioWriterPool(size)
	if pool != nil {
		if v := pool.Get(); v != nil {
			bw := v.(*bufio.Writer)
			bw.Reset(w)
			return bw
		}
	}
	return bufio.NewWriterSize(w, size)
}

func putBufioWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	if pool := bufioWriterPool(bw.Available()); pool != nil {
		pool.Put(bw)
	}
}
