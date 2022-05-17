package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql/introspection"
	"github.com/pkg/errors"
	"io/ioutil"
	goLog "log"
	"net/http"
)

func (s *Server) mountGraphIntrospectionSubscriber() {
	root := fmt.Sprintf("%s.introspect", s.opts.serverName)

	endpoint := fmt.Sprintf("http://%s/%s", s.graphListener.Addr().String(), s.opts.graphEntrypoint)

	if err := s.axonClient.Reply(root, func(mg *messages.Message) (*messages.Message, error) {
		type Body struct {
			Query     string                 `json:"query" msgpack:"query"`
			Variables map[string]interface{} `json:"variables,omitempty" msgpack:"variables,omitempty"`
		}

		payload := &Body{
			Query:     introspection.Query,
			Variables: nil,
		}

		if s.opts. {
			
		}

		marsh, _ := json.Marshal(payload)
		r, err := http.Post(endpoint, "application/json", bytes.NewReader(marsh))
		if err != nil {
			return nil, err
		}

		body, err := ioutil.ReadAll(r.Body)
		if len(body) != 0 {
			return mg.WithBody(body), nil
		}

		if err != nil {
			return nil, err
		}

		return nil, errors.New("internal server error")
	}); err != nil {
		goLog.Fatal(err)
	}

	<-make(chan bool)
}
