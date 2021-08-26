package main

import (
	"github.com/Just4Ease/graphrpc/generator"
	"log"
)

func main() {
	gen := generator.NewClientGenerator("./services")

	clients := []struct {
		ServiceName string
		opts        []generator.ClientGeneratorOption
	}{
		{
			ServiceName: "ms-vendors",
			opts: []generator.ClientGeneratorOption{
				generator.Package("vendorService", "/vendors"),
				generator.QueriesPath("definitions/**/*.graphql"),
				generator.SetNatsConnectionURL("127.0.0.1:7000"),
				generator.RemoteGraphQLPath("/graphql", nil),
			},
		},
	}

	for _, client := range clients {
		if err := gen.AddClient(client.ServiceName, client.opts...); err != nil {
			log.Fatalf("failed to add client to generator process: serviceName %s, err: %v", client.ServiceName, err)
		}
	}

	gen.Generate()
}
