package client

import (
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/graphrpc/libs/Yamashou/gqlgenc/client"
)

type Options = client.Options

type Option = client.Option

// SetHeader sets the headers for this client. Note, duplicate headers are replaced with the newest value provided
var SetHeader = client.SetHeader

// SetRemoteGraphQLPath is used to set the graphql path of the remote service for generation to occur.
var SetRemoteGraphQLPath = client.SetRemoteGraphQLPath

// SetRemoteServiceName is used to set the service name of the remote service for this client.
var SetRemoteServiceName = client.SetRemoteServiceName

// ApplyMsgPackEncoder is used to enable internal msgpack encoding over encoding/json
var ApplyMsgPackEncoder = client.ApplyMsgPackEncoder

type Header = client.Header

// Client is the http client wrapper
type Client = client.Client

// Request represents an outgoing GraphQL request
type Request = client.Request

// NewClient creates a new http client wrapper
func NewClient(conn axon.EventStore, options ...Option) (*Client, error) {
	return client.NewClient(conn, options...)
}
