package graphrpc

import (
	"github.com/Just4Ease/axon/v2"
	"github.com/borderlesshq/graphrpc/client"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql/handler"
	"github.com/borderlesshq/graphrpc/server"
)

type ServerOption = server.Option

func NewServer(conn axon.EventStore, h *handler.Server, option ...ServerOption) *server.Server {
	return server.NewServer(conn, h, option...)
}

type ClientOption = client.Option

func NewClient(conn axon.EventStore, options ...ClientOption) (*client.Client, error) {
	return client.NewClient(conn, options...)
}
