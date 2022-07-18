package example

import (
	"context"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"
)

type splitStats map[string]int

func (ss splitStats) wrapTr(name string, tr transport.Transport) transport.Transport {
	return transport.Func(func(req transport.Request) transport.Response {
		if _, ok := ss[name]; !ok {
			ss[name] = 0
		}

		ss[name]++

		return tr.Request(req)
	})
}

func (ss splitStats) get(name string) int {
	if v, ok := ss[name]; ok {
		return v
	}

	return 0
}

func splitcli(ctx context.Context) (*client.Client, func(), *splitStats) {
	ss := &splitStats{}

	c, td := clifactory(ctx, func(ts *httptest.Server) (transport.Transport, func()) {
		wstr := wstr(ctx, ts.URL)
		httptr := httptr(ctx, ts.URL)

		s_wstr := ss.wrapTr("ws", wstr)
		s_httptr := ss.wrapTr("http", httptr)

		tr := transport.SplitSubscription(s_wstr, s_httptr)

		return tr, func() {
			wstr.Close()
		}
	})

	return c, td, ss
}

func TestRawSplitQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown, ss := splitcli(ctx)
	defer teardown()

	runAssertQuery(t, ctx, cli)

	t.Log(ss)

	assert.Equal(t, 1, ss.get("http"))
	assert.Equal(t, 0, ss.get("ws"))
}

func TestRawSplitSubscription(t *testing.T) {
	ctx := context.Background()

	cli, teardown, ss := splitcli(ctx)
	defer teardown()

	runAssertSub(t, ctx, cli)

	t.Log(ss)

	assert.Equal(t, 0, ss.get("http"))
	assert.Equal(t, 1, ss.get("ws"))
}
