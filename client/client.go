package client

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/Yamashou/gqlgenc/graphqljson"
	"github.com/pkg/errors"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"log"
	"strings"
)

type Options struct {
	Headers               Header
	remoteGraphEntrypoint string
}

type Option func(*Options) error

// SetHeader sets the headers for this client. Note, duplicate headers are replaced with the newest value provided
func SetHeader(key, value string) Option {
	return func(options *Options) error {
		if options.Headers == nil {
			options.Headers = make(map[string]string)
		}
		options.Headers[key] = value
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

type Header = map[string]string

// Client is the http client wrapper
type Client struct {
	axonConn axon.EventStore
	opts     *Options
	BaseURL  string
	Headers  Header
}

// Request represents an outgoing GraphQL request
type Request struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	OperationName string                 `json:"operationName,omitempty"`
}

// NewClient creates a new http client wrapper
func NewClient(conn axon.EventStore, options ...Option) (*Client, error) {
	opts := &Options{Headers: map[string]string{}}

	for _, option := range options {
		if err := option(opts); err != nil {
			log.Printf("failed to apply client option: %v", err)
			return nil, err
		}
	}

	if opts.remoteGraphEntrypoint == "" {
		log.Print("Using Default GraphRPC Remote Graph Entrypoint Path: '/graphql' ")
		opts.remoteGraphEntrypoint = "graphql"
	}

	if conn == nil {
		panic("axon must not be nil. see github.com/Just4Ease/axon for more details on how to connect")
	}

	return &Client{
		axonConn: conn,
		BaseURL:  fmt.Sprintf("%s.%s", conn.GetServiceName(), opts.remoteGraphEntrypoint),
		opts:     opts,
		Headers:  opts.Headers,
	}, nil
}

func (c *Client) exec(_ context.Context, operationName, query string, variables map[string]interface{}, headers Header) ([]byte, error) {
	r := &Request{
		Query:         query,
		Variables:     variables,
		OperationName: operationName,
	}

	requestBody, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	message := messages.NewMessage()
	message.WithBody(requestBody)
	message.WithType(messages.RequestMessage)
	message.WithSubject(c.BaseURL)
	message.WithSpecVersion("v0.1")

	message.Header = c.Headers
	if message.Header == nil {
		message.Header = make(map[string]string)
	}

	for k, v := range headers {
		message.Header[k] = v
	}

	mg, err := c.axonConn.Request(message)
	if err != nil {
		return nil, err
	}

	if mg.Type == messages.ErrorMessage {
		return nil, errors.New(mg.Error)
	}

	return mg.Body, nil

	//msg, err := c.nc.RequestWithContext(ctx, c.BaseURL, requestBody)
	//
	//
	//if err != nil {
	//	return nil, err
	//}
	//
	//return msg.Data, nil
}

// GqlErrorList is the struct of a standard graphql error response
type GqlErrorList struct {
	Errors gqlerror.List `json:"errors"`
}

func (e *GqlErrorList) Error() string {
	return e.Errors.Error()
}

// HTTPError is the error when a GqlErrorList cannot be parsed
type HTTPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ErrorResponse represent an handled error
type ErrorResponse struct {
	// populated when http status code is not OK
	NetworkError *HTTPError `json:"networkErrors"`
	// populated when http status code is OK but the server returned at least one graphql error
	GqlErrors *gqlerror.List `json:"graphqlErrors"`
}

// HasErrors returns true when at least one error is declared
func (er *ErrorResponse) HasErrors() bool {
	return er.NetworkError != nil || er.GqlErrors != nil
}

func (er *ErrorResponse) Error() string {
	content, err := json.Marshal(er)
	if err != nil {
		return err.Error()
	}

	return string(content)
}

// Post sends a http POST request to the graphql endpoint with the given query then unpacks
// the response into the given object.
func (c *Client) Exec(ctx context.Context, operationName, query string, respData interface{}, vars map[string]interface{}, headers Header) error {
	result, err := c.exec(ctx, operationName, query, vars, headers)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return parseResponse(result, 200, respData)
}

func (c *Client) ServiceName() string {
	return c.axonConn.GetServiceName()
}

func parseResponse(body []byte, httpCode int, result interface{}) error {
	errResponse := &ErrorResponse{}
	isKOCode := httpCode < 200 || 299 < httpCode
	if isKOCode {
		errResponse.NetworkError = &HTTPError{
			Code:    httpCode,
			Message: fmt.Sprintf("Response body %s", string(body)),
		}
	}

	// some servers return a graphql error with a non OK http code, try anyway to parse the body
	if err := unmarshal(body, result); err != nil {
		if gqlErr, ok := err.(*GqlErrorList); ok {
			errResponse.GqlErrors = &gqlErr.Errors
		} else if !isKOCode { // if is KO code there is already the http error, this error should not be returned
			return err
		}
	}

	if errResponse.HasErrors() {
		return errResponse
	}

	return nil
}

// response is a GraphQL layer response from a handler.
type response struct {
	Data   json.RawMessage `json:"data"`
	Errors json.RawMessage `json:"errors"`
}

func unmarshal(data []byte, res interface{}) error {
	resp := response{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("failed to decode data %s: %w", string(data), err)
	}

	if resp.Errors != nil && len(resp.Errors) > 0 {
		// try to parse standard graphql error
		errors := &GqlErrorList{}
		if e := json.Unmarshal(data, errors); e != nil {
			return fmt.Errorf("faild to parse graphql errors. Response content %s - %w ", string(data), e)
		}

		return errors
	}

	if err := graphqljson.UnmarshalData(resp.Data, res); err != nil {
		return fmt.Errorf("failed to decode data into response %s: %w", string(data), err)
	}

	return nil
}
