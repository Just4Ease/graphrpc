package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/messages"
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
			Query     string                 `json:"query"`
			Variables map[string]interface{} `json:"variables"`
		}

		payload := &Body{
			Query:     IntrospectionQuery,
			Variables: nil,
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

const IntrospectionQuery = `
 query IntrospectionQuery {
    __schema {
      queryType { name }
      mutationType { name }
      types {
        ...FullType
      }
      directives {
        name
        description
        locations
        args {
          ...InputValue
        }
      }
    }
  }
  fragment FullType on __Type {
    kind
    name
    description
    fields(includeDeprecated: true) {
      name
      description
      args {
        ...InputValue
      }
      type {
        ...TypeRef
      }
      isDeprecated
      deprecationReason
    }
    inputFields {
      ...InputValue
    }
    interfaces {
      ...TypeRef
    }
    enumValues(includeDeprecated: true) {
      name
      description
      isDeprecated
      deprecationReason
    }
    possibleTypes {
      ...TypeRef
    }
  }
  fragment InputValue on __InputValue {
    name
    description
    type { ...TypeRef }
    defaultValue
  }
  fragment TypeRef on __Type {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
    }
  }
`
