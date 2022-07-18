package example

import (
	"context"
	"fmt"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
	"net/http/httptest"
	"nhooyr.io/websocket"
	"testing"
	"time"
)

type unstableWebsocketConn struct {
	ctx    context.Context
	wsconn *transport.WebsocketHandler
}

func (u *unstableWebsocketConn) dropConn() {
	fmt.Println("## DROP CONN")
	_ = u.wsconn.Conn.Close(websocket.StatusProtocolError, "conn drop")
}

func (u *unstableWebsocketConn) ReadJSON(v interface{}) error {
	return u.wsconn.ReadJSON(v)
}

func (u *unstableWebsocketConn) WriteJSON(v interface{}) error {
	return u.wsconn.WriteJSON(v)
}

func (u *unstableWebsocketConn) Close() error {
	return u.wsconn.Close()
}

func (u *unstableWebsocketConn) SetReadLimit(limit int64) {
	u.wsconn.SetReadLimit(limit)
}

func newUnstableConn(ctx context.Context, URL string) (transport.WebsocketConn, error) {
	wsconn, err := transport.DefaultWebsocketConnProvider(time.Second)(ctx, URL)
	if err != nil {
		return nil, err
	}

	return &unstableWebsocketConn{
		ctx:    ctx,
		wsconn: wsconn.(*transport.WebsocketHandler),
	}, nil
}

func unstablewscli(ctx context.Context, newWebsocketConn transport.WebsocketConnProvider) (*client.Client, func()) {
	return clifactory(ctx, func(ts *httptest.Server) (transport.Transport, func()) {
		tr := cwstr(ctx, ts.URL, newWebsocketConn)

		return tr, func() {
			tr.Close()
		}
	})
}

func TestRawWSUnstableQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown := unstablewscli(ctx, newUnstableConn)
	defer teardown()
	tr := cli.Transport.(*transport.Ws)

	for i := 0; i < 5; i++ {
		fmt.Println("> Attempt", i)
		tr.GetConn().(*unstableWebsocketConn).dropConn()

		tr.WaitFor(transport.StatusReady, time.Second)

		runAssertQuery(t, ctx, cli)
	}
}

func TestRawWSUnstableSubscription(t *testing.T) {
	ctx := context.Background()

	cli, teardown := unstablewscli(ctx, newUnstableConn)
	defer teardown()
	tr := cli.Transport.(*transport.Ws)

	for i := 0; i < 5; i++ {
		fmt.Println("> Attempt", i)
		tr.GetConn().(*unstableWebsocketConn).dropConn()

		tr.WaitFor(transport.StatusReady, time.Second)

		runAssertSub(t, ctx, cli)
	}
}
