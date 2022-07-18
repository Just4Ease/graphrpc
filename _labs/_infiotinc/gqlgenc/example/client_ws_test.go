package example

import (
	"context"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
	"net/http/httptest"
	"testing"
	"time"
)

func wscli(ctx context.Context) (*client.Client, func()) {
	return clifactory(ctx, func(ts *httptest.Server) (transport.Transport, func()) {
		tr := wstr(ctx, ts.URL)

		return tr, func() {
			tr.Close()
		}
	})
}

func TestRawWSQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown := wscli(ctx)
	defer teardown()

	runAssertQuery(t, ctx, cli)
}

func TestRawWSSubscription(t *testing.T) {
	ctx := context.Background()

	cli, teardown := wscli(ctx)
	defer teardown()

	runAssertSub(t, ctx, cli)
}

func TestWSCtxCancel1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cli, teardown := wscli(ctx)

	runAssertQuery(t, ctx, cli)

	teardown()
	time.Sleep(time.Second)
	cancel()
}

func TestWSCtxCancel2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cli, teardown := wscli(ctx)

	runAssertQuery(t, ctx, cli)

	cancel()
	time.Sleep(time.Second)
	teardown()
}
