package example

import (
	"context"
	"encoding/json"
	"github.com/infiotinc/gqlgenc/client"
	"github.com/infiotinc/gqlgenc/client/transport"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func httpcli(ctx context.Context) (*client.Client, func()) {
	return clifactory(ctx, func(ts *httptest.Server) (transport.Transport, func()) {
		return httptr(ctx, ts.URL), nil
	})
}

func TestRawHttpQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown := httpcli(ctx)
	defer teardown()

	runAssertQuery(t, ctx, cli)
}

func TestRawHttpQueryError(t *testing.T) {
	ctx := context.Background()

	cli, teardown := httpcli(ctx)
	defer teardown()

	var opres RoomQueryResponse
	_, err := cli.Query(ctx, "", RoomQuery, map[string]interface{}{"name": "error"}, &opres)
	assert.EqualError(t, err, "input: room that's an invalid room\n")
}

func TestRawHttpError(t *testing.T) {
	ctx := context.Background()

	cli, teardown := clifactorywith(ctx, func(ts *httptest.Server) (transport.Transport, func()) {
		return httptr(ctx, ts.URL), nil
	}, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := json.Marshal(map[string]string{
				"error": "Yeah that went wrong",
			})

			w.WriteHeader(http.StatusForbidden)
			w.Write(b)
		})
	})
	defer teardown()

	var opres RoomQueryResponse
	_, err := cli.Query(ctx, "", RoomQuery, map[string]interface{}{"name": "name"}, &opres)
	assert.EqualError(t, err, `no data nor errors, got 403: {"error":"Yeah that went wrong"}`)
}
