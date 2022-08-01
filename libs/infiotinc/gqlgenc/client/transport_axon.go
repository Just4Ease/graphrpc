package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"mime/multipart"
)

func (c *Client) Request(req Request) Response {
	opres, err := c.request(req)
	if err != nil {
		return NewErrorResponse(err)
	}

	return NewSingleResponse(*opres)
}

func (c *Client) request(graphqlRequest Request) (*OperationResponse, error) {
	if graphqlRequest.Headers == nil {
		graphqlRequest.Headers = make(Header)
		graphqlRequest.Headers.Add("Content-Type", "application/json")
	}

	requestBodyByte, err := json.Marshal(NewOperationRequestFromRequest(graphqlRequest))
	if err != nil {
		return nil, err
	}

	//for _, ro := range h.RequestOptions {
	//	ro(req)
	//}

	pubOptions := make([]options.PublisherOption, 0)
	pubOptions = append(pubOptions, options.SetPubContentType("application/json"))
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	mg, err := c.axonConn.Request(c.BaseURL, requestBodyByte, pubOptions...)
	if err != nil {
		return nil, err
	}

	if mg.Type == messages.ErrorMessage {
		return nil, errors.New(mg.Error)
	}

	var opres OperationResponse
	err = json.Unmarshal(mg.Body, &opres)
	if err != nil {
		return nil, err
	}

	if len(opres.Data) == 0 && len(opres.Errors) == 0 {
		return nil, fmt.Errorf("no data nor errors, got: %.1000s", mg.Body)
	}

	return &opres, nil
}

func (c *Client) subscribe(graphqlRequest Request) (*OperationResponse, <-chan []byte, axon.Close, error) {
	if graphqlRequest.Headers == nil {
		graphqlRequest.Headers = make(Header)
		graphqlRequest.Headers.Add("Content-Type", "application/json")
	}

	requestBodyByte, err := json.Marshal(NewOperationRequestFromRequest(graphqlRequest))
	if err != nil {
		return nil, nil, nil, err
	}

	//for _, ro := range h.RequestOptions {
	//	ro(req)
	//}

	pubOptions := make([]options.PublisherOption, 0)
	pubOptions = append(pubOptions, options.SetPubContentType("application/json"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode: %w", err)
	}

	mg, err := c.axonConn.Request(c.BaseURL+"-subscriptions", requestBodyByte, pubOptions...)
	if err != nil {
		return nil, nil, nil, err
	}

	if mg.Type == messages.ErrorMessage {
		return nil, nil, nil, errors.New(mg.Error)
	}

	if mg.Body == nil {
		streamId := mg.Header["stream"]
		stream, err := c.streamer.JoinStream(streamId)
		if err != nil {
			return nil, nil, nil, err
		}

		out, err := stream.Recv()
		if err != nil {
			return nil, nil, nil, err
		}

		return nil, out, stream.Close, nil
	}

	var opres OperationResponse
	err = json.Unmarshal(mg.Body, &opres)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(opres.Data) == 0 && len(opres.Errors) == 0 {
		return nil, nil, nil, fmt.Errorf("no data nor errors, got: %.1000s", mg.Body)
	}

	return &opres, nil, nil, nil
}

func (c *Client) jsonFormField(w *multipart.Writer, name string, v interface{}) error {
	fw, err := w.CreateFormField(name)
	if err != nil {
		return err
	}

	return json.NewEncoder(fw).Encode(v)
}
