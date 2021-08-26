package client

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"log"
	"net/http"

	"github.com/Yamashou/gqlgenc/graphqljson"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Options struct {
	ncOpts  []nats.Option
	nc      *nats.Conn
	natsUrl string
	Headers Header
}

type Option func(*Options) error

func SetNatsUrl(url string) Option {
	return func(options *Options) error {
		options.natsUrl = url
		return nil
	}
}

func SetNatsOptions(opts ...nats.Option) Option {
	return func(options *Options) error {
		options.ncOpts = append(options.ncOpts, opts...)
		return nil
	}
}

func UseNatsCon(nc *nats.Conn) Option {
	return func(options *Options) error {
		if nc == nil {
			return errors.New("invalid nats connection provided")
		}

		options.nc = nc
		return nil
	}
}

func SetHeader(key, value string) Option {
	return func(options *Options) error {
		options.Headers.Add(key, value)
		return nil
	}
}

type Header = http.Header

// Client is the http client wrapper
type Client struct {
	nc      *nats.Conn
	opts    *Options
	BaseURL string
	Headers Header
}

// Request represents an outgoing GraphQL request
type Request struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	OperationName string                 `json:"operationName,omitempty"`
}

// NewClient creates a new http client wrapper
func NewClient(remoteServiceName, remoteServiceGraphEntrypoint string, options ...Option) (*Client, error) {
	opts := &Options{ncOpts: []nats.Option{}, natsUrl: "", Headers: nil}

	var err error
	for _, option := range options {
		if err = option(opts); err != nil {
			log.Printf("failed to apply client option: %v", err)
			return nil, err
		}
	}

	if opts.nc == nil {
		if opts.natsUrl == "" {
			log.Print("nats url not provided using default option")
		}

		opts.ncOpts = append(opts.ncOpts, nats.Name(fmt.Sprintf("%s-client", remoteServiceName)))
		opts.nc, err = nats.Connect(opts.natsUrl, opts.ncOpts...)
		if err != nil {
			return nil, err
		}
	}

	// Detect 1st in api/graph remote entrypoint and strip it
	if remoteServiceGraphEntrypoint[:1] == "/" {
		remoteServiceGraphEntrypoint = remoteServiceGraphEntrypoint[1:]
	}

	return &Client{
		nc:      opts.nc,
		BaseURL: fmt.Sprintf("%s.%s", remoteServiceName, remoteServiceGraphEntrypoint),
		opts:    opts,
		Headers: opts.Headers,
	}, nil
}

func (c *Client) exec(ctx context.Context, operationName, query string, variables map[string]interface{}, headers Header) ([]byte, error) {
	r := &Request{
		Query:         query,
		Variables:     variables,
		OperationName: operationName,
	}

	requestBody, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	msg, err := c.nc.RequestWithContext(ctx, c.BaseURL, requestBody)
	if err != nil {
		return nil, err
	}

	return msg.Data, nil
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
