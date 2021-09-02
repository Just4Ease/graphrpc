package graphrpc

import (
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/graphrpc/client"
	"github.com/Just4Ease/graphrpc/server"
	"net/http"
)

type ServerOption = server.Option

func NewServer(conn axon.EventStore, handler http.Handler, option ...ServerOption) *server.Server {
	return server.NewServer(conn, handler, option...)
}

type ClientOption = client.Option

func NewClient(options ...ClientOption) (*client.Client, error) {
	return client.NewClient(options...)
}
