// Copyright 2012-2019 The GraphRPC Authors
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
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql/handler"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
)

type preRunHook func() error
type postRunHook func(eventStore axon.EventStore) error

type Options struct {
	serverName           string // default: GraphRPC
	graphEntrypoint      string // graph entrypoint
	enablePlayground     bool   // disable playground
	preRunHook           preRunHook
	postRunHook          postRunHook
	middlewares          []func(http.Handler) http.Handler
	address              string // http server address
	applyMsgpackEncoding bool
}

type Option func(*Options) error

// PreRunHook is used to execute code blocks before the server starts.
func PreRunHook(f preRunHook) Option {
	return func(o *Options) error {
		o.preRunHook = f
		return nil
	}
}

// PostRunHook is used to execute code blocks after the messaging system is connected.
func PostRunHook(f postRunHook) Option {
	return func(o *Options) error {
		o.postRunHook = f
		return nil
	}
}

// UseMiddlewares is used to apply middlewares. Regular HTTP middlewares are allowed
func UseMiddlewares(middlewares ...func(http.Handler) http.Handler) Option {
	return func(o *Options) error {
		if middlewares == nil {
			return errors.New("cannot use nil as middlewares")
		}

		o.middlewares = append(o.middlewares, middlewares...)
		return nil
	}
}

// SetGraphQLPath is used to define the endpoint for your GraphRPC Server
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

// SetGraphHTTPServerAddress is used to set the base http address for the graph http server.
// Note when the GraphRPC client connects, it doesn't pass through http, it passes through nats events over axon library.
// See https://github.com/Just4Ease/axon
// See https://nats.io
func SetGraphHTTPServerAddress(address string) Option {
	return func(o *Options) error {
		if strings.TrimSpace(address) == empty {
			return errors.New("GraphQL entrypoint path is required!")
		}

		o.address = address
		return nil
	}
}

// DisableGraphPlayground is used to disable the graphql playground.
func DisableGraphPlayground() Option {
	return func(o *Options) error {
		o.enablePlayground = false
		return nil
	}
}

// ApplyMsgpackEncoder is used to apply msgpack as the encoder.
func ApplyMsgpackEncoder() Option {
	return func(o *Options) error {
		o.applyMsgpackEncoding = true
		return nil
	}
}

type Server struct {
	axonClient           axon.EventStore // AxonClient
	opts                 *Options        // graph & nats options
	graphHandler         *handler.Server // graphql/rest handler
	graphListener        net.Listener    // graphql listener
	router               *chi.Mux        // chi router.
	mu                   sync.Mutex
	applyMsgpackEncoding bool
}

func NewServer(axon axon.EventStore, h *handler.Server, options ...Option) *Server {

	if axon == nil {
		panic("failed to start server: axon.EventStore must not be nil")
	}

	opts := &Options{
		serverName:           axon.GetServiceName(),
		graphEntrypoint:      "graph",
		enablePlayground:     true,
		applyMsgpackEncoding: false,
	}

	for _, opt := range options {
		if err := opt(opts); err != nil {
			log.Fatalf("failed to start server: %v", err)
		}
	}

	return &Server{
		mu:                   sync.Mutex{},
		axonClient:           axon,
		opts:                 opts,
		graphHandler:         h,
		applyMsgpackEncoding: opts.applyMsgpackEncoding,
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
	color.Yellow.Printf("%s\n", tx)
	color.Green.Printf("🔥 Service Name          :  %s\n", color.Bold.Sprint(color.Cyan.Sprint(s.axonClient.GetServiceName())))

	var err error

	if s.graphListener, err = net.Listen("tcp", s.opts.address); err != nil {
		return err
	}

	if s.opts.postRunHook != nil {
		if err := s.opts.postRunHook(s.axonClient); err != nil {
			return errors.Wrap(err, "failed to execute post run hook")
		}
	}

	go s.mountGraphSubscriber()
	return s.mountGraphHTTPServer()
}

func (s *Server) mountGraphHTTPServer() error {
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	if s.opts.middlewares != nil && len(s.opts.middlewares) != 0 {
		router.Use(s.opts.middlewares...)
	}

	graphEndpoint := fmt.Sprintf("/%s", s.opts.graphEntrypoint)
	if s.opts.enablePlayground {
		router.Handle("/", playground.Handler("GraphQL playground", graphEndpoint))
	} else {
		router.Get("/", func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("content-type", "text/html")
			_, _ = fmt.Fprintf(writer, "<h1 align='center'>%s is running... Please contact administrator for more details</h1>", s.opts.serverName)
		})
	}

	router.Handle(graphEndpoint, s.graphHandler)
	color.Green.Printf("🚀 GraphQL Playground    :  http://%s/\n", s.opts.address)
	color.Green.Printf("🐙 GraphQL HTTP Endpoint :  http://%s/%s\n", s.opts.address, s.opts.graphEntrypoint)
	color.Green.Printf("🦾 GraphQL Entry Path    :  %s\n", color.OpUnderscore.Sprint(color.Cyan.Sprintf("/%s", s.opts.graphEntrypoint)))

	// For direct use by the eventStore for calls over events.
	// This is to override the need to open a local connection to the local http server.
	// See subscribers.go for more understanding.
	s.router = router
	return http.Serve(s.graphListener, router)
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
