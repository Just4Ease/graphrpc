package example

import (
	"context"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/infiotinc/gqlgenc/client/extensions"
	"github.com/infiotinc/gqlgenc/client/transport"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHttpAPQQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown := httpcli(ctx)
	defer teardown()

	cli.Use(&extensions.APQ{})

	runAssertQuery(t, ctx, cli, func(opres transport.OperationResponse, data RoomQueryResponse) {
		var stats extension.ApqStats
		err := opres.Extensions.Unmarshal("apqStats", &stats)
		if err != nil {
			assert.Fail(t, err.Error())
		}
		assert.NotNil(t, stats)
		assert.True(t, stats.SentQuery)
	})

	runAssertQuery(t, ctx, cli, func(opres transport.OperationResponse, data RoomQueryResponse) {
		var stats extension.ApqStats
		err := opres.Extensions.Unmarshal("apqStats", &stats)
		if err != nil {
			assert.Fail(t, err.Error())
		}
		assert.NotNil(t, stats)
		assert.False(t, stats.SentQuery)
	})
}

func TestSplitAPQQuery(t *testing.T) {
	ctx := context.Background()

	cli, teardown, _ := splitcli(ctx)
	defer teardown()

	cli.Use(&extensions.APQ{})
}

func TestSplitAPQSubscription(t *testing.T) {
	ctx := context.Background()

	cli, teardown, _ := splitcli(ctx)
	defer teardown()

	cli.Use(&extensions.APQ{})

	runAssertSub(t, ctx, cli, func(opres transport.OperationResponse, data MessagesSubResponse) {
		var stats extension.ApqStats
		err := opres.Extensions.Unmarshal("apqStats", &stats)
		if err != nil {
			assert.Fail(t, err.Error())
		}
		assert.NotNil(t, stats)
		assert.True(t, stats.SentQuery)
	})

	runAssertSub(t, ctx, cli, func(opres transport.OperationResponse, data MessagesSubResponse) {
		var stats extension.ApqStats
		err := opres.Extensions.Unmarshal("apqStats", &stats)
		if err != nil {
			assert.Fail(t, err.Error())
		}
		assert.NotNil(t, stats)
		assert.False(t, stats.SentQuery)
	})
}
