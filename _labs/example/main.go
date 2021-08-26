package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/graphrpc/client"
	vendorService "github.com/Just4Ease/graphrpc/services/vendors"
	"log"
)

func main() {
	s, err := vendorService.NewClient(
		client.SetNatsUrl("127.0.0.1:7000"),
	)
	if err != nil {
		log.Fatal(err)
	}

	v, err := s.ListVendors(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	PrettyJson(v.ListVendors)
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
