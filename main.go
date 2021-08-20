package main

import (
	"graphrpc/server"
	"net/http"
)

func NewServer(address string, handler http.Handler, option *server.Options) *server.Server {
	return server.NewServer(address, handler, option)
}

func NewClient() {

}
