package client

import (
	"context"
	"encoding/json"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Operation string

const (
	Query        Operation = "query"
	Mutation     Operation = "mutation"
	Subscription Operation = "subscription"
)

type OperationRequest struct {
	Query         string                 `json:"query,omitempty"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	Extensions    map[string]interface{} `json:"extensions,omitempty"`
	Headers       Header                 `json:"headers,omitempty"`
}

func NewOperationRequestFromRequest(req Request) OperationRequest {
	return OperationRequest{
		Query:         req.Query,
		OperationName: req.OperationName,
		Variables:     req.Variables,
		Extensions:    req.Extensions,
		Headers:       req.Headers,
	}
}

type OperationResponse struct {
	Data       json.RawMessage `json:"data,omitempty"`
	Errors     gqlerror.List   `json:"errors,omitempty"`
	Extensions RawExtensions   `json:"extensions,omitempty"`
}

func (r OperationResponse) UnmarshalData(t interface{}) error {
	if r.Data == nil {
		return nil
	}

	return json.Unmarshal(r.Data, t)
}

type RawExtensions map[string]json.RawMessage

func (es RawExtensions) Unmarshal(name string, t interface{}) error {
	if es == nil {
		return nil
	}

	ex, ok := es[name]
	if !ok {
		return nil
	}

	return json.Unmarshal(ex, t)
}

type Request struct {
	Context   context.Context
	Operation Operation

	OperationName string
	Query         string
	Variables     map[string]interface{}
	Extensions    map[string]interface{}
	Headers       Header
}

type Transport interface {
	Request(req Request) Response
}
