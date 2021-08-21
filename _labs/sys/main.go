package main

import (
	"github.com/go-chi/chi"
	"graphrpc/server"
	"graphrpc/subz"
	"log"
)

func main() {

	router := chi.NewRouter()

	router.Get("/graph", subz.Graph)

	srv := server.NewServer("127.0.0.1:9000", router, nil)
	log.Fatal(srv.Serve())
}
