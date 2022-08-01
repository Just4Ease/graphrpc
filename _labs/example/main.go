package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/systems/jetstream"
	"github.com/borderlesshq/graphrpc/client"
	userService "github.com/borderlesshq/graphrpc/services/users"
	"log"
	"time"
)

func main() {

	ax, _ := jetstream.Init(
		options.SetStoreName("gateway"),
		options.SetAddress("localhost:4222"),
	)

	s := userService.NewServiceClient(ax, client.SetRemoteServiceName("ms-users"), client.SetRemoteGraphQLPath("graphql"), client.ApplyMsgPackEncoder())

	//v, err := s.ListVendors(context.Background(), nil)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//v, err := s.Login(context.Background(), "justice@borderlesshq.com", "12121")
	//if err != nil {
	//	log.Fatal(err)
	//}

	v, cancel := s.WatchUserStatus(context.Background(), "01g690x5geqp1aev2mtsj5mme3", nil)
	defer cancel()

	for {
		out := <-v
		if out.Error != nil {
			log.Fatalln(out.Error)
		}

		fmt.Println(out.Data.WatchUserStatus.String())
		time.Sleep(time.Second * 5)
	}
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
