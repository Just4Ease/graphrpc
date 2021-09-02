// Copyright 2012-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"io/ioutil"
	goLog "log"
	"net"
	"net/http"

	"strings"
	"sync"
)

type preRunHook func() error
type postRunHook func(eventStore axon.EventStore) error

type Options struct {
	serverName       string // default: GraphRPC
	graphEntrypoint  string // graph entrypoint
	enablePlayground bool   // disable playground
	preRunHook       preRunHook
	postRunHook      postRunHook
	middlewares      []func(http.Handler) http.Handler
	address          string // http server address
}

type Option func(*Options) error

// PreRunHook
func PreRunHook(f preRunHook) Option {
	return func(o *Options) error {
		o.preRunHook = f
		return nil
	}
}

// PostRunHook
func PostRunHook(f postRunHook) Option {
	return func(o *Options) error {
		o.postRunHook = f
		return nil
	}
}

// UseMiddlewares
func UseMiddlewares(middlewares ...func(http.Handler) http.Handler) Option {
	return func(o *Options) error {
		if middlewares == nil {
			return errors.New("cannot use nil as middlewares")
		}

		o.middlewares = append(o.middlewares, middlewares...)
		return nil
	}
}

// SetGraphQLPath
func SetGraphQLPath(path string) Option {
	return func(o *Options) error {
		if strings.TrimSpace(path) == empty {
			return errors.New("GraphQL entrypoint path is required!")
		}

		// Detect 1st in api/graph entrypoint and strip it
		if path[:1] == "/" {
			path = path[1:]
		}

		o.graphEntrypoint = path
		return nil
	}
}

// SetGraphQLPath
func SetGraphHTTPServerAddress(address string) Option {
	return func(o *Options) error {
		if strings.TrimSpace(address) == empty {
			return errors.New("GraphQL entrypoint path is required!")
		}

		o.address = address
		return nil
	}
}

// DisableGraphPlayground
func DisableGraphPlayground() Option {
	return func(o *Options) error {
		o.enablePlayground = false
		return nil
	}
}

type Server struct {
	mu               *sync.Mutex
	axonClient       axon.EventStore // AxonClient
	opts             *Options        // graph & nats options
	graphHTTPHandler http.Handler    // graphql/rest handler
	graphListener    net.Listener    // graphql listener
	address          string
}

func NewServer(axon axon.EventStore, handler http.Handler, options ...Option) *Server {

	if axon == nil {
		panic("failed to start server: axon.EventStore must not be nil")
	}

	opts := &Options{
		serverName:      axon.GetServiceName(),
		graphEntrypoint: "graph",
	}

	for _, opt := range options {
		if err := opt(opts); err != nil {
			log.Fatalf("failed to start server: %v", err)
		}
	}

	return &Server{
		mu:               &sync.Mutex{},
		axonClient:       axon,
		opts:             opts,
		graphHTTPHandler: handler,
	}
}

func (s *Server) Serve() error {
	if s.opts.preRunHook != nil {
		if err := s.opts.preRunHook(); err != nil {
			return errors.Wrap(err, "failed to execute preRunHook")
		}
	}

	tx := `
         _              _           _                   _          _       _            _           _           _      
        /\ \           /\ \        / /\                /\ \       / /\    / /\         /\ \        /\ \       /\ \     
       /  \ \         /  \ \      / /  \              /  \ \     / / /   / / /        /  \ \      /  \ \     /  \ \    
      / /\ \_\       / /\ \ \    / / /\ \            / /\ \ \   / /_/   / / /        / /\ \ \    / /\ \ \   / /\ \ \   
     / / /\/_/      / / /\ \_\  / / /\ \ \          / / /\ \_\ / /\ \__/ / /        / / /\ \_\  / / /\ \_\ / / /\ \ \  
    / / / ______   / / /_/ / / / / /  \ \ \        / / /_/ / // /\ \___\/ /        / / /_/ / / / / /_/ / // / /  \ \_\ 
   / / / /\_____\ / / /__\/ / / / /___/ /\ \      / / /__\/ // / /\/___/ /        / / /__\/ / / / /__\/ // / /    \/_/ 
  / / /  \/____ // / /_____/ / / /_____/ /\ \    / / /_____// / /   / / /        / / /_____/ / / /_____// / /          
 / / /_____/ / // / /\ \ \  / /_________/\ \ \  / / /      / / /   / / /        / / /\ \ \  / / /      / / /________   
/ / /______\/ // / /  \ \ \/ / /_       __\ \_\/ / /      / / /   / / /        / / /  \ \ \/ / /      / / /_________\  
\/___________/ \/_/    \_\/\_\___\     /____/_/\/_/       \/_/    \/_/         \/_/    \_\/\/_/       \/____________/  

`
	log.Infof("%s\n", tx)

	var err error

	if s.graphListener, err = net.Listen("tcp", s.address); err != nil {
		return err
	}

	go s.mountGraphIntrospectionSubscriber()
	go s.mountGraphSubscriber()

	if s.opts.postRunHook != nil {
		if err := s.opts.postRunHook(s.axonClient); err != nil {
			return errors.Wrap(err, "failed to execute post run hook")
		}
	}

	return s.mountGraphHTTPServer()
}

func (s *Server) mountGraphSubscriber() {
	root := fmt.Sprintf("%s.%s", s.opts.serverName, s.opts.graphEntrypoint)
	endpoint := fmt.Sprintf("http://%s/%s", s.graphListener.Addr().String(), s.opts.graphEntrypoint)
	err := s.axonClient.Reply(root, func(mg *messages.Message) (*messages.Message, error) {
		r, err := http.Post(endpoint, mg.ContentType.String(), bytes.NewReader(mg.Body))
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
	})
	if err != nil {
		goLog.Fatal(err)
	}
	<-make(chan bool)
}

func (s *Server) mountGraphHTTPServer() error {
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	if s.opts.middlewares != nil && len(s.opts.middlewares) != 0 {
		router.Use(s.opts.middlewares...)
	}

	if !s.opts.enablePlayground {
		router.Get("/", func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("content-type", "text/html")
			_, _ = fmt.Fprintf(writer, "<h1 align='center'>%s is running... Please contact administrator for more details</h1>", s.graphHTTPHandler)
		})
	}

	graphEndpoint := fmt.Sprintf("/%s", s.opts.graphEntrypoint)
	if s.opts.enablePlayground {
		router.Handle("/", playground.Handler("GraphQL playground", graphEndpoint))
	}

	router.Handle(graphEndpoint, s.graphHTTPHandler)

	// TODO: Serve https with tls.
	log.Infof("http(s)://%s/ -> GraphQL playground\n", s.address)
	log.Infof("http(s)://%s/%s -> GraphRPC HTTP Endpoint\n", s.address, s.opts.graphEntrypoint)
	return http.Serve(s.graphListener, router)
}

func (s *Server) WaitForShutdown() {
	//s.axonClient.Close()
	_ = s.graphListener.Close()
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}
