package main

import (
	"context"
	"example"
	"example/server"
	"example/server/generated"
	_ "expvar" // Register the expvar handlers
	"fmt"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	htransport "github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/websocket"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/extensions"
	"github.com/infiotinc/gqlgenc/client/transport"
	"net/http"
	_ "net/http/pprof" // Register the pprof handlers
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func round(cli *client.Client) {
	var v interface{}
	_, err := cli.Query(context.Background(), "", example.RoomQuery, map[string]interface{}{"name": "test"}, &v)
	if err != nil {
		fmt.Println("ERROR QUERY: ", err)
	}

	res := cli.Subscription(context.Background(), "", example.MessagesSub, nil)
	if err != nil {
		fmt.Println("ERROR SUB: ", err)
	}
	defer res.Close()

	for res.Next() {
		// Consume res
	}

	if res.Err() != nil {
		fmt.Println("ERROR SUB RES: ", res.Err())
	}
}

func main() {
	srv := handler.New(generated.NewExecutableSchema(generated.Config{
		Resolvers: &server.Resolver{},
	}))

	srv.AddTransport(htransport.POST{})
	srv.AddTransport(htransport.Websocket{
		KeepAlivePingInterval: 500 * time.Millisecond,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	})
	srv.Use(extension.AutomaticPersistedQuery{Cache: graphql.MapCache{}})
	srv.Use(extension.Introspection{})

	http.Handle("/playground", playground.Handler("Playground", "/"))
	http.Handle("/", srv)

	go func() {
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			panic(err)
		}
	}()

	httptr := &transport.Http{
		URL: "http://localhost:8080",
	}
	wstr := &transport.Ws{
		URL:                   "ws://localhost:8080",
		WebsocketConnProvider: transport.DefaultWebsocketConnProvider(time.Second),
	}
	wstr.Start(context.Background())
	defer wstr.Close()

	tr := transport.SplitSubscription(wstr, httptr)

	cli := &client.Client{
		Transport: tr,
	}
	cli.Use(&extensions.APQ{})

	pm := collectMemStats()

	fmt.Println("Starting queries")

	var wg sync.WaitGroup
	ch := make(chan struct{}, 5) // Concurrency
	var di int64
	for i := 0; i < 100_000; i++ {
		wg.Add(1)

		ch <- struct{}{}
		go func() {
			round(cli)
			di := atomic.AddInt64(&di, 1)
			if di%1000 == 0 {
				fmt.Println(di)
			}
			<-ch
			wg.Done()
		}()
	}

	wg.Wait()

	time.Sleep(2 * time.Second)

	m := collectMemStats()

	printMemStats("Before", pm)
	printMemStats("After ", m)
}

func collectMemStats() runtime.MemStats {
	fmt.Println("Running GC")
	runtime.GC()

	fmt.Println("Collecting MemStats")
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func printMemStats(s string, m runtime.MemStats) {
	fmt.Printf("%v: ", s)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
