package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/pkg/errors"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"log"
	"net/http"
	"time"
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

		streamChannel, heartBeatChannel := s.streams.newStream(s.axonClient, &subHandler)

		mg.Header["streamChannel"] = streamChannel
		mg.Header["heartBeatChannel"] = heartBeatChannel
		mg.WithBody(nil)

		return mg, nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) subscribe(start time.Time, msg *message) {
	ctx := graphql.StartOperationTrace(context.Background())
	var params *graphql.RawParams
	//if err := jsonDecode(bytes.NewReader(msg.payload), &params); err != nil {
	//	c.sendError(msg.id, &gqlerror.Error{Message: "invalid json"})
	//	c.complete(msg.id)
	//	return
	//}

	params.ReadTime = graphql.TraceTiming{
		Start: start,
		End:   graphql.Now(),
	}

	ctx = graphql.WithOperationContext(ctx, rc)

	if c.initPayload != nil {
		ctx = withInitPayload(ctx, c.initPayload)
	}

	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.active[msg.id] = cancel
	c.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := rc.Recover(ctx, r)
				var gqlerr *gqlerror.Error
				if !errors.As(err, &gqlerr) {
					gqlerr = &gqlerror.Error{}
					if err != nil {
						gqlerr.Message = err.Error()
					}
				}
				c.sendError(msg.id, gqlerr)
			}
			c.complete(msg.id)
			c.mu.Lock()
			delete(c.active, msg.id)
			c.mu.Unlock()
			cancel()
		}()

		responses, ctx := c.exec.DispatchOperation(ctx, rc)
		for {
			response := responses(ctx)
			if response == nil {
				break
			}

			c.sendResponse(msg.id, response)

			s.axonClient.Publish(msg.id, response)
		}

		// complete and context cancel comes from the defer
	}()
}
