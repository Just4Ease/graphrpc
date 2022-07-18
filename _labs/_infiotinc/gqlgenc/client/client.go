package client

import (
	"context"
	"fmt"
	"github.com/infiotinc/gqlgenc/client/transport"
)

type extensions struct {
	aroundRequest []AroundRequest
}

func (es *extensions) Use(e Extension) {
	if a, ok := e.(AroundRequest); ok {
		es.aroundRequest = append(es.aroundRequest, a)
	}
}

func (es *extensions) RunAroundRequest(req transport.Request, h RequestHandler) transport.Response {
	run := h

	for _, _e := range es.aroundRequest {
		e := _e // Local ref
		next := run // Local ref
		run = func(req transport.Request) transport.Response {
			return e.AroundRequest(req, next)
		}
	}

	return run(req)
}

type Client struct {
	Transport transport.Transport

	extensions
}

func (c *Client) doSingle(
	ctx context.Context,
	operation transport.Operation,
	operationName string,
	query string,
	variables map[string]interface{},
	t interface{},
) (transport.OperationResponse, error) {
	res := c.do(transport.Request{
		Context:       ctx,
		Operation:     operation,
		Query:         query,
		OperationName: operationName,
		Variables:     variables,
	})
	defer res.Close()

	if !res.Next() {
		if err := res.Err(); err != nil {
			return transport.OperationResponse{}, err
		}

		return transport.OperationResponse{}, fmt.Errorf("no response")
	}

	opres := res.Get()

	err := opres.UnmarshalData(t)

	if len(opres.Errors) > 0 {
		return opres, opres.Errors
	}

	return opres, err
}

func (c *Client) do(req transport.Request) transport.Response {
	if req.Extensions == nil {
		req.Extensions = map[string]interface{}{}
	}

	res := c.RunAroundRequest(req, c.Transport.Request)

	go func() {
		select {
		case <-req.Context.Done():
			res.Close()
		case <-res.Done():
		}
	}()

	return res
}

// Query runs a query
// operationName is optional
func (c *Client) Query(ctx context.Context, operationName string, query string, variables map[string]interface{}, t interface{}) (transport.OperationResponse, error) {
	return c.doSingle(ctx, transport.Query, operationName, query, variables, t)
}

// Mutation runs a mutation
// operationName is optional
func (c *Client) Mutation(ctx context.Context, operationName string, query string, variables map[string]interface{}, t interface{}) (transport.OperationResponse, error) {
	return c.doSingle(ctx, transport.Mutation, operationName, query, variables, t)
}

// Subscription starts a GQL subscription
// operationName is optional
func (c *Client) Subscription(ctx context.Context, operationName string, query string, variables map[string]interface{}) transport.Response {
	return c.do(transport.Request{
		Context:       ctx,
		Operation:     transport.Subscription,
		Query:         query,
		OperationName: operationName,
		Variables:     variables,
	})
}
