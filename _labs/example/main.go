package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"github.com/borderlesshq/graphrpc/client"
	vendorService "github.com/borderlesshq/graphrpc/services/vendors"
	"log"
)

func main() {

	ax, err := jetstream.Init(options.Options{
		ServiceName: "gateway",
		Address:     "localhost:4222",
	})

	s, err := vendorService.NewClient(ax, client.SetRemoteServiceName("ms-vendors"), client.SetRemoteGraphQLPath("graphql"))
	if err != nil {
		log.Fatal(err)
	}

	//v, err := s.ListVendors(context.Background(), nil)
	//if err != nil {
	//	log.Fatal(err)
	//}

	v, err := s.CreateVendor(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	PrettyJson(v.CreateVendor)
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}
