package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/borderlesshq/graphrpc/server/streams"
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
		if streamChannel, ok := mg.Header["streamChannel"]; ok {
			heartBeatChannel, err := s.streams.RunStream(streamChannel)
			if err != nil {
				return nil, err
			}

			mg.Header["streamChannel"] = streamChannel
			mg.Header["heartBeatChannel"] = heartBeatChannel
			mg.WithBody(nil)
			return mg.WithBody([]byte("done")), nil
		}

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
		streamChannel, heartBeatChannel := s.streams.NewStream(func(send streams.Send, streamCloser streams.Close) {

			defer func() {
				gqlErr := subHandler.PanicHandler()
				if gqlErr != nil {
					sendErr(send, gqlErr)
				}
			}()
			responses, ctx := subHandler.Exec()

			for {
				response := responses(ctx)
				if response == nil {
					break
				}

				sendResponse(send, response)
			}
		})

		mg.Header["streamChannel"] = streamChannel
		mg.Header["heartBeatChannel"] = heartBeatChannel
		mg.WithBody(nil)
		return mg, nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func sendErr(send streams.Send, errors ...*gqlerror.Error) {
	errs := make([]error, len(errors))
	for i, err := range errors {
		errs[i] = err
	}
	b, err := json.Marshal(errs)
	if err != nil {
		panic(err)
	}

	_ = send(b)
}

func sendResponse(send streams.Send, response *graphql.Response) {
	// TODO: Ensure we can use custom encoding here.
	b, err := json.Marshal(response)
	if err != nil {
		//streamCloser() // Close before panic.
		panic(err) // TODO: remove this panic bros.
	}

	_ = send(b)
}
