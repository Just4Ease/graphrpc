package graphrpc

import (
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/graphrpc/client"
	"github.com/Just4Ease/graphrpc/server"
)

type ServerOption = server.Option

func NewServer(conn axon.EventStore, h *handler.Server, option ...ServerOption) *server.Server {
	return server.NewServer(conn, h, option...)
}

type ClientOption = client.Option

func NewClient(conn axon.EventStore, options ...ClientOption) (*client.Client, error) {
	return client.NewClient(conn, options...)
}
