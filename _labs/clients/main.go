package main

import (
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/systems/jetstream"
	"github.com/borderlesshq/graphrpc/generator"
	"log"
)

func main() {

	eventStore, err := jetstream.Init(
		options.SetStoreName("gateway"),
		options.SetAddress("localhost:4222"),
	)

	if err != nil {
		log.Fatal(err)
	}

	gen := generator.NewClientGenerator("./services")

	clients := []struct {
		opts []generator.ClientGeneratorOption
	}{
		{
			opts: []generator.ClientGeneratorOption{
				generator.RemoteServiceName("ms-users"),
				generator.SetAxonConn(eventStore),
				generator.Package("msUsers", "/users"),
				generator.QueriesPath("schema/**/*.graphql"),
				generator.RemoteGraphQLPath("/graphql", nil),
			},
		},
	}

	for _, client := range clients {
		if err := gen.AddClient(client.opts...); err != nil {
			log.Fatalf("failed to add client to generator process: serviceName err: %v", err)
		}
	}

	gen.Generate()
}
