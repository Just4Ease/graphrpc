package example

import (
	"context"
	"example/server"
	"example/server/generated"
	"fmt"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	htransport "github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/websocket"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func wstr(ctx context.Context, u string) *transport.Ws {
	return cwstr(
		ctx,
		u,
		nil,
	)
}

func cwstr(ctx context.Context, u string, newWebsocketConn transport.WebsocketConnProvider) *transport.Ws {
	_ = os.Setenv("GQLGENC_WS_LOG", "1")

	if strings.HasPrefix(u, "http") {
		u = "ws" + strings.TrimPrefix(u, "http")
	}

	tr := &transport.Ws{
		URL:                   u,
		WebsocketConnProvider: newWebsocketConn,
	}
	errCh := tr.Start(ctx)
	go func() {
		for err := range errCh {
			log.Println("Ws Transport error: ", err)
		}
	}()

	tr.WaitFor(transport.StatusReady, time.Second)

	return tr
}

func httptr(ctx context.Context, u string) *transport.Http {
	tr := &transport.Http{
		URL: u,
	}

	return tr
}

func clifactory(ctx context.Context, trf func(server *httptest.Server) (transport.Transport, func())) (*client.Client, func()) {
	return clifactorywith(ctx, trf, func(h http.Handler) http.Handler {
		return h
	})
}

func clifactorywith(ctx context.Context, trf func(server *httptest.Server) (transport.Transport, func()), hw func(http.Handler) http.Handler) (*client.Client, func()) {
	h := handler.New(generated.NewExecutableSchema(generated.Config{
		Resolvers: &server.Resolver{},
	}))

	h.AddTransport(htransport.POST{})
	h.AddTransport(htransport.MultipartForm{})
	h.AddTransport(htransport.Websocket{
		KeepAlivePingInterval: 500 * time.Millisecond,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		InitFunc: func(ctx context.Context, initPayload htransport.InitPayload) (context.Context, error) {
			fmt.Println("WS Server init received")

			return ctx, nil
		},
	})
	h.Use(extension.AutomaticPersistedQuery{Cache: graphql.MapCache{}})
	h.Use(extension.Introspection{})

	h.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
		stats := extension.GetApqStats(ctx)

		if stats != nil {
			graphql.RegisterExtension(ctx, "apqStats", stats)
		}

		return next(ctx)
	})

	srv := http.NewServeMux()
	srv.Handle("/playground", playground.Handler("Playground", "/"))
	srv.Handle("/", hw(h))

	ts := httptest.NewServer(srv)

	fmt.Println("TS URL: ", ts.URL)

	tr, trteardown := trf(ts)

	return &client.Client{
			Transport: tr,
		}, func() {
			if trteardown != nil {
				fmt.Println("CLOSE TR")
				trteardown()
			}

			if ts != nil {
				fmt.Println("CLOSE HTTPTEST")
				ts.Close()
			}
		}
}

type QueryAsserter func(transport.OperationResponse, RoomQueryResponse)

func runAssertQuery(t *testing.T, ctx context.Context, cli *client.Client, asserters ...QueryAsserter) {
	fmt.Println("ASSERT QUERY")
	var data RoomQueryResponse
	opres, err := cli.Query(ctx, "", RoomQuery, map[string]interface{}{"name": "test"}, &data)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "test", data.Room.Name)

	for _, a := range asserters {
		a(opres, data)
	}
}

type SubAsserter func(transport.OperationResponse, MessagesSubResponse)

func runAssertSub(t *testing.T, ctx context.Context, cli *client.Client, asserters ...SubAsserter) {
	fmt.Println("ASSERT SUB")
	res := cli.Subscription(ctx, "", MessagesSub, nil)

	ids := make([]string, 0)

	for res.Next() {
		opres := res.Get()

		var data MessagesSubResponse
		err := opres.UnmarshalData(&data)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, data.MessageAdded.ID)

		for _, a := range asserters {
			a(opres, data)
		}
	}

	if err := res.Err(); err != nil {
		t.Fatal(err)
	}

	assert.Len(t, ids, 3)
}
