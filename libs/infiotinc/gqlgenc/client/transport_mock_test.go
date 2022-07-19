package client

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSingleResponse(t *testing.T) {
	_ = os.Setenv("GQLGENC_WS_LOG", "1")

	cli := &Client{
		Transport: Mock{
			"query": func(req Request) Response {
				return NewSingleResponse(NewMockOperationResponse("hey", nil))
			},
		},
	}

	var res string
	_, err := cli.Query(context.Background(), "", "query", nil, &res)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "hey", res)
}

func TestChanResponse(t *testing.T) {
	_ = os.Setenv("GQLGENC_WS_LOG", "1")

	cli := &Client{
		Transport: Mock{
			"query": func(req Request) Response {
				res := NewChanResponse(nil)

				go func() {
					for i := 0; i < 3; i++ {
						res.Send(NewMockOperationResponse(fmt.Sprintf("hey%v", i), nil))
					}
					res.CloseCh()
				}()

				return res
			},
		},
	}

	sub := cli.Subscription(context.Background(), "", "query", nil)
	defer sub.Close()

	msgs := make([]string, 0)

	for sub.Next() {
		res := sub.Get()

		var data string
		err := res.UnmarshalData(&data)
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, data)
	}

	if err := sub.Err(); err != nil {
		t.Fatal(err)
	}

	assert.Len(t, msgs, 3)
}
