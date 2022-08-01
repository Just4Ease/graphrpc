package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/pkg/errors"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"log"
	"net/http"
)

func (s *Server) mountGraphQueryAndMutationsSubscriber() {
	err := s.axonClient.Reply(fmt.Sprintf("%s.%s", s.opts.serverName, s.opts.graphEntrypoint), func(mg *messages.Message) (*messages.Message, error) {
		ctx := context.Background()
		var params *graphql.RawParams
		start := graphql.Now()
		dec := json.NewDecoder(bytes.NewReader(mg.Body))
		dec.UseNumber()
		if err := dec.Decode(&params); err != nil {
			return nil, errors.Wrap(err, "body could not be decoded")
		}

		params.Headers = make(http.Header)
		for k, v := range mg.Header {
			params.Headers.Add(k, v)
		}

		params.ReadTime = graphql.TraceTiming{
			Start: start,
			End:   graphql.Now(),
		}

		response, err := s.graphHandler.ExecGraphCommand(ctx, params)
		if err != nil {
			return nil, err
		}

		out, err := json.Marshal(response)
		if err != nil {
			return nil, err
		}

		return mg.WithBody(out), nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) mountGraphSubscriptionsSubscriber() {
	err := s.axonClient.Reply(fmt.Sprintf("%s.%s-subscriptions", s.opts.serverName, s.opts.graphEntrypoint), func(mg *messages.Message) (*messages.Message, error) {
		ctx := context.Background()
		var params *graphql.RawParams
		start := graphql.Now()
		dec := json.NewDecoder(bytes.NewReader(mg.Body))
		dec.UseNumber()
		if err := dec.Decode(&params); err != nil {
			return nil, errors.Wrap(err, "body could not be decoded")
		}

		params.Headers = make(http.Header)
		for k, v := range mg.Header {
			params.Headers.Add(k, v)
		}

		params.ReadTime = graphql.TraceTiming{
			Start: start,
			End:   graphql.Now(),
		}

		subHandler, err := s.graphHandler.ExecGraphSubscriptionsCommand(ctx, params)
		if err != nil {
			return nil, err
		}

		if subHandler.Response != nil {
			b, _ := json.Marshal(subHandler.Response)
			return mg.WithBody(b), nil
		}

		// This part has opened up the stream to service...
		stream := s.streamer.NewStream(func(send axon.Send, c axon.Close) {
			defer func() {
				gqlErr := subHandler.PanicHandler()
				if gqlErr != nil {
					if err := sendErr(send, gqlErr); err != nil {
						log.Printf("failed to err response: %s", err)
						return
					}
				}
			}()
			responses, ctx := subHandler.Exec()

			for {
				response := responses(ctx)
				if response == nil {
					break
				}

				if err := sendResponse(send, response); err != nil {
					log.Printf("failed to send response: %s", err)
					return
				}
			}
		})

		mg.Header["stream"] = stream.ID()
		mg.WithBody(nil)
		return mg, nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func sendErr(send axon.Send, errors ...*gqlerror.Error) error {
	errs := make([]error, len(errors))
	for i, err := range errors {
		errs[i] = err
	}
	b, err := json.Marshal(errs)
	if err != nil {
		return err
	}

	return send(b)
}

func sendResponse(send axon.Send, response *graphql.Response) error {
	// TODO: Ensure we can use custom encoding here.
	b, err := json.Marshal(response)
	if err != nil {
		//streamCloser() // Close before panic.
		return err
	}

	return send(b)
}
