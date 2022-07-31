package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"log"
	"net/http"
	"strings"
)

type extensions struct {
	aroundRequest []AroundRequest
}

func (es *extensions) Use(e Extension) {
	if a, ok := e.(AroundRequest); ok {
		es.aroundRequest = append(es.aroundRequest, a)
	}
}

func (es *extensions) RunAroundRequest(req Request, h RequestHandler) Response {
	run := h

	for _, _e := range es.aroundRequest {
		e := _e     // Local ref
		next := run // Local ref
		run = func(req Request) Response {
			return e.AroundRequest(req, next)
		}
	}

	return run(req)
}

type Client struct {
	Transport
	BaseURL  string
	axonConn axon.EventStore

	opts *Options
	//RequestOptions   []HttpRequestOption
	Headers             Header
	applyMsgPackEncoder bool
	extensions
}

type Options struct {
	Headers               Header
	remoteGraphEntrypoint string
	remoteServiceName     string
	applyMsgpackEncoder   bool
}

type Option func(*Options) error

// SetHeader sets the headers for this client. Note, duplicate headers are replaced with the newest value provided
func SetHeader(key, value string) Option {
	return func(options *Options) error {
		if options.Headers == nil {
			options.Headers = make(Header)
		}
		options.Headers.Add(key, value)
		return nil
	}
}

// SetRemoteGraphQLPath is used to set the graphql path of the remote service for generation to occur.
func SetRemoteGraphQLPath(path string) Option {
	return func(o *Options) error {
		if strings.TrimSpace(path) == "" {
			return errors.New("GraphQL entrypoint path is required!")
		}

		// Detect 1st in api/graph entrypoint and strip it
		if path[:1] == "/" {
			path = path[1:]
		}

		o.remoteGraphEntrypoint = path
		return nil
	}
}

// SetRemoteServiceName is used to set the service name of the remote service for this client.
func SetRemoteServiceName(remoteServiceName string) Option {
	return func(o *Options) error {
		if strings.TrimSpace(remoteServiceName) == "" {
			return errors.New("remote GraphRPC Service name is required")
		}

		o.remoteServiceName = remoteServiceName
		return nil
	}
}

// ApplyMsgPackEncoder is used to enable internal msgpack encoding over encoding/json
func ApplyMsgPackEncoder() Option {
	return func(o *Options) error {
		o.applyMsgpackEncoder = true
		return nil
	}
}

type Header = http.Header

// NewClient creates a new http client wrapper
func NewClient(conn axon.EventStore, options ...Option) (*Client, error) {
	opts := &Options{Headers: make(Header)}

	for _, option := range options {
		if err := option(opts); err != nil {
			log.Printf("failed to apply client option: %v", err)
			return nil, err
		}
	}

	if opts.remoteGraphEntrypoint == "" {
		log.Print("using default GraphRPC remote graph entrypoint path: '/graphql'...")
		opts.remoteGraphEntrypoint = "graphql"
	}

	if opts.remoteServiceName == "" {
		return nil, errors.New("remote graphrpc service name is required")
	}

	if conn == nil {
		panic("axon must not be nil. see github.com/borderlesshq/axon for more details on how to connect")
	}

	return &Client{
		axonConn:            conn,
		BaseURL:             opts.remoteServiceName + "." + opts.remoteGraphEntrypoint,
		opts:                opts,
		Headers:             opts.Headers,
		applyMsgPackEncoder: opts.applyMsgpackEncoder,
	}, nil
}

func (c *Client) ServiceName() string {
	return c.opts.remoteServiceName
}

func (c *Client) exec(ctx context.Context, operation Operation, operationName string, query string, variables map[string]interface{}, t interface{}, header Header) (OperationResponse, error) {
	opres, err := c.request(Request{
		Context:       ctx,
		Operation:     operation,
		Query:         query,
		OperationName: operationName,
		Variables:     variables,
		Headers:       header,
	})

	if err != nil {
		return OperationResponse{}, err
	}

	if err := opres.UnmarshalData(t); err != nil {
		return OperationResponse{}, err
	}

	if len(opres.Errors) > 0 {
		return OperationResponse{}, opres.Errors
	}

	return *opres, err
}

func (c *Client) stream(req Request) Response {
	if req.Extensions == nil {
		req.Extensions = map[string]interface{}{}
	}

	res := c.RunAroundRequest(req, c.Request)

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
func (c *Client) Query(ctx context.Context, operationName string, query string, variables map[string]interface{}, t interface{}, header Header) (OperationResponse, error) {
	return c.exec(ctx, Query, operationName, query, variables, t, header)
}

// Mutation runs a mutation
// operationName is optional
func (c *Client) Mutation(ctx context.Context, operationName string, query string, variables map[string]interface{}, t interface{}, header Header) (OperationResponse, error) {
	return c.exec(ctx, Mutation, operationName, query, variables, t, header)
}

// Subscription starts a GQL subscription
// operationName is optional
func (c *Client) Subscription(ctx context.Context, operationName string, query string, variables map[string]interface{}, header Header) Response {
	return c.stream(Request{
		Context:       ctx,
		Operation:     Subscription,
		Query:         query,
		OperationName: operationName,
		Variables:     variables,
		Headers:       header,
	})
}
