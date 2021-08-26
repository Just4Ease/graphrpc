package graphrpc

import (
	"github.com/Just4Ease/graphrpc/client"
	"github.com/Just4Ease/graphrpc/server"
	"net/http"
)

type ServerOption = server.Options

func NewServer(address string, handler http.Handler, option server.Options) *server.Server {
	return server.NewServer(address, handler, option)
}

type ClientOption = client.Option

func NewClient(remoteServiceName, remoteServiceGraphEntrypoint string, options ...ClientOption) (*client.Client, error) {
	return client.NewClient(remoteServiceName, remoteServiceGraphEntrypoint, options...)
}
