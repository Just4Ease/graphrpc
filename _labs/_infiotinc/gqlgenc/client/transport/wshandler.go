package transport

// Original work from https://github.com/hasura/go-graphql-client/blob/0806e5ec7/subscription.go

import (
	"context"
	"fmt"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
	"time"
)

type closedWs struct{}

var ErrClosedConnection = fmt.Errorf("closed connection")

func (c closedWs) ReadJSON(v interface{}) error {
	return ErrClosedConnection
}

func (c closedWs) WriteJSON(v interface{}) error {
	return ErrClosedConnection
}

func (c closedWs) Close() error {
	return nil
}

func (c closedWs) SetReadLimit(limit int64) {

}

var _ WebsocketConn = (*closedWs)(nil)

// WebsocketHandler is default websocket handler implementation using https://github.com/nhooyr/websocket
type WebsocketHandler struct {
	ctx     context.Context
	timeout time.Duration
	*websocket.Conn
}

func (wh *WebsocketHandler) WriteJSON(v interface{}) error {
	ctx, cancel := context.WithTimeout(wh.ctx, wh.timeout)
	defer cancel()

	return wsjson.Write(ctx, wh.Conn, v)
}

func (wh *WebsocketHandler) ReadJSON(v interface{}) error {
	if wh.timeout > 0 {
		ctx, cancel := context.WithTimeout(wh.ctx, wh.timeout)
		defer cancel()

		return wsjson.Read(ctx, wh.Conn, v)
	}

	return wsjson.Read(wh.ctx, wh.Conn, v)
}

func (wh *WebsocketHandler) Close() error {
	return wh.Conn.Close(websocket.StatusNormalClosure, "close websocket")
}

type WsDialOption func(o *websocket.DialOptions)

// DefaultWebsocketConnProvider is the connections factory
// A timeout of 0 means no timeout, reading will block until a message is received or the context is canceled
// If your server supports the keepalive, set the timeout to something greater than the server keepalive
// (for example 15s for a 10s keepalive)
func DefaultWebsocketConnProvider(timeout time.Duration, optionfs ...WsDialOption) WebsocketConnProvider {
	return func(ctx context.Context, URL string) (WebsocketConn, error) {
		options := &websocket.DialOptions{
			Subprotocols: []string{"graphql-ws"},
		}
		for _, f := range optionfs {
			f(options)
		}

		c, _, err := websocket.Dial(ctx, URL, options)
		if err != nil {
			return nil, err
		}

		return &WebsocketHandler{
			ctx:     ctx,
			Conn:    c,
			timeout: timeout,
		}, nil
	}
}
