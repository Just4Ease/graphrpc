package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"io/ioutil"
	goLog "log"
	"net/http"
	"strings"
)

func (s *Server) mountGraphIntrospectionSubscriber() {
	root := fmt.Sprintf("%s.introspect", s.opts.serverName)
	// TODO: Use axon format.

	protocol := "https://"
	if !s.opts.natsServerOption.TLS {
		protocol = strings.ReplaceAll(protocol, "s", "")
	}

	endpoint := fmt.Sprintf("%s%s/%s", protocol, s.graphListener.Addr().String(), s.opts.graphEntrypoint)
	_, err := s.snc.QueueSubscribe(root, s.opts.serverName, func(msg *nats.Msg) {
		if !s.opts.enablePlayground {
			_ = msg.Respond([]byte("introspection disabled"))
			return
		}

		type Body struct {
			Query     string                 `json:"query"`
			Variables map[string]interface{} `json:"variables"`
		}

		body := &Body{
			Query:     IntrospectionQuery,
			Variables: nil,
		}

		marsh, _ := json.Marshal(body)
		r, err := http.Post(endpoint, "application/json", bytes.NewReader(marsh))

		if err != nil {
			_ = msg.Respond([]byte("not available at this time"))
			return
		}

		b, _ := ioutil.ReadAll(r.Body)
		_ = msg.Respond(b)
	})
	if err != nil {
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
