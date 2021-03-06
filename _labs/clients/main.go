package main

import (
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"github.com/Just4Ease/graphrpc/generator"
	"log"
)

func main() {

	eventStore, err := jetstream.Init(options.Options{
		ServiceName: "gateway",
		Address:     "nats://127.0.0.1:4222",
	})

	if err != nil {
		log.Fatal(err)
	}

	gen := generator.NewClientGenerator("./services")

	clients := []struct {
		opts []generator.ClientGeneratorOption
	}{
		{
			opts: []generator.ClientGeneratorOption{
				generator.RemoteServiceName("ms-vendors"),
				generator.SetAxonConn(eventStore),
				generator.Package("vendorService", "/vendors"),
				generator.QueriesPath("definitions/**/*.graphql"),
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
