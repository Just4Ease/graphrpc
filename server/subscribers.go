package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/pkg/errors"
	"log"
	"net/http"
)

func (s *Server) mountGraphSubscriber() {
	err := s.axonClient.Reply(fmt.Sprintf("%s.%s", s.opts.serverName, s.opts.graphEntrypoint), func(mg *messages.Message) (*messages.Message, error) {

		ctx := context.Background()

		var params *graphql.RawParams
		start := graphql.Now()
		dec := json.NewDecoder(bytes.NewReader(mg.Body))
		dec.UseNumber()
		if err := dec.Decode(params); err != nil {
			return nil, errors.WithMessage(err, "body could not be decoded")
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

	<-make(chan bool)
}
