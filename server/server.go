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
	"flag"
	"fmt"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"io/ioutil"
	goLog "log"
	"net"
	"net/http"

	"os"
	"strings"
	"sync"
)

type Options struct {
	serverName              string              // default: GraphRPC
	natsServerOption        *natsServer.Options // server nats-server options
	natsServerClientOptions []nats.Option       // server nats-client options
	graphEntrypoint         string              // graph entrypoint
	disablePlayground       bool                // disable playground
	preRunHook              preRunHook
	postRunHook             postRunHook
	middlewares             []func(http.Handler) http.Handler
}

type preRunHook func() error
type postRunHook func(natsServer *natsServer.Server, natsClientConn *nats.Conn, jetStreamClient nats.JetStreamContext) error

func NewServerOptions(serverName string, graphEntryPoint string) *Options {
	if strings.TrimSpace(serverName) == empty {
		goLog.Fatal("Server name is required!")
	}

	if strings.TrimSpace(graphEntryPoint) == empty {
		goLog.Fatal("API/GraphQL entrypoint is required!")
	}

	// Detect 1st in api/graph entrypoint and strip it
	if graphEntryPoint[:1] == "/" {
		graphEntryPoint = graphEntryPoint[1:]
	}

	return &Options{
		serverName:              serverName,
		graphEntrypoint:         graphEntryPoint, // forward slash graph
		natsServerOption:        nil,
		natsServerClientOptions: make([]nats.Option, 0),
		disablePlayground:       false,
		preRunHook:              nil,
		postRunHook:             nil,
		middlewares:             nil,
	}
}

func (o *Options) SetNatsServerOpts(opts *natsServer.Options) {
	args := os.Args[1:]
	args = append(args, "-n", "GraphRPC")
	// Create a FlagSet and sets the usage
	fs := flag.NewFlagSet(exe, flag.ExitOnError)
	fs.Usage = usage

	// Configure the options from the flags/config file
	argsOrConfigOptions, err := natsServer.ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, natsServer.PrintTLSHelpAndDie)
	if err != nil {
		PrintAndDie(fmt.Sprintf("%s: %s", exe, err))
	} else if argsOrConfigOptions.CheckConfig {
		fmt.Fprintf(os.Stderr, "%s: configuration file %s is valid\n", exe, argsOrConfigOptions.ConfigFile)
		os.Exit(0)
	}

	if opts == nil {
		opts = argsOrConfigOptions
	}

	if trim(argsOrConfigOptions.ServerName) != empty {
		opts.ServerName = trim(argsOrConfigOptions.ServerName)
	}

	if len(args) > 2 || trim(argsOrConfigOptions.ConfigFile) != empty {
		opts = argsOrConfigOptions

		opts.JetStream = true
		opts.JetStreamMaxMemory = 0
		opts.JetStreamMaxStore = 0
		opts.StoreDir = opts.ServerName
		if trim(argsOrConfigOptions.StoreDir) != empty {
			opts.StoreDir = trim(argsOrConfigOptions.StoreDir)
		}
	}

	o.natsServerOption = opts
}

func (o *Options) SetNatsServerClientOptions(option ...nats.Option) {
	o.natsServerClientOptions = append(o.natsServerClientOptions, option...)
}

func (o *Options) SetGraphEntrypointOption(entrypoint string) {
	o.graphEntrypoint = entrypoint
}

// SetPreRunHook is
func (o *Options) SetPreRunHook(f preRunHook) {
	o.preRunHook = f
}

// SetPostRunHook is
func (o *Options) SetPostRunHook(f postRunHook) {
	o.postRunHook = f
}

func (o *Options) SetMiddlewares(middlewares ...func(http.Handler) http.Handler) {
	o.middlewares = append(o.middlewares, middlewares...)
}

type Server struct {
	mu                       *sync.Mutex
	natsServer               *natsServer.Server    // server nats-server
	snc                      *nats.Conn            // server nats-client
	sjs                      nats.JetStreamContext // server nats-jetstream-client
	opts                     *Options              // graph & nats options
	graphHTTPHandler         http.Handler          // graphql/rest handler
	graphListener            net.Listener          // graphql listener
	graphListenerCloseSignal chan bool             // graphql listener close signal
	graphEntrypoint          string                // graphql entrypoint
	address                  string
}

var exe = "graphrpc-server"

func NewServer(address string, handler http.Handler, option Options) *Server {

	if option.natsServerOption == nil {
		goLog.Println("nats-server configuration not provided, using defaults...")
		option.SetNatsServerOpts(nil)
	}

	option.natsServerClientOptions = append(option.natsServerClientOptions, nats.Name(option.serverName))

	return &Server{
		mu:                       &sync.Mutex{},
		address:                  address,
		opts:                     &option,
		graphHTTPHandler:         handler,
		graphEntrypoint:          option.graphEntrypoint,
		graphListenerCloseSignal: make(chan bool),
	}
}

func (s *Server) Serve() error {

	if s.opts.preRunHook != nil {
		if err := s.opts.preRunHook(); err != nil {
			return errors.Wrap(err, "failed to execute preRunHook")
		}
	}

	ls, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.graphListener = ls

	s.natsServer = connServer(s.opts.natsServerOption)

	if s.snc, err = nats.Connect(s.natsServer.Addr().String(), s.opts.natsServerClientOptions...); err != nil {
		return err
	}

	if s.sjs, err = s.snc.JetStream(); err != nil {
		return err
	}

	go s.mountGraphSubscriber()

	if s.opts.postRunHook != nil {
		if err := s.opts.postRunHook(s.natsServer, s.snc, s.sjs); err != nil {
			return errors.Wrap(err, "failed to execute post run hook")
		}
	}

	return s.mountGraphHTTPServer()
}

func (s *Server) mountGraphSubscriber() {
	root := fmt.Sprintf("%s.%s", s.opts.serverName, s.graphEntrypoint)
	// TODO: Use axon format.

	protocol := "https://"
	if !s.opts.natsServerOption.TLS {
		protocol = strings.ReplaceAll(protocol, "s", "")
	}

	endpoint := fmt.Sprintf("%s%s/%s", protocol, s.graphListener.Addr().String(), s.graphEntrypoint)
	_, err := s.snc.QueueSubscribe(root, s.opts.serverName, func(msg *nats.Msg) {
		r, err := http.Post(endpoint, mimetype.Detect(msg.Data).String(), bytes.NewReader(msg.Data))
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

func (s *Server) mountGraphHTTPServer() error {
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	if s.opts.middlewares != nil && len(s.opts.middlewares) != 0 {
		router.Use(s.opts.middlewares...)
	}

	if !s.opts.disablePlayground {
		router.Get("/", func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("content-type", "text/html")
			_, _ = fmt.Fprintf(writer, "<a>%s is running... Please contact administrator for more details</a>", s.opts.serverName)
		})
	}

	graphEndpoint := fmt.Sprintf("/%s", s.opts.graphEntrypoint)
	if s.opts.disablePlayground {
		router.Handle("/", playground.Handler("GraphQL playground", graphEndpoint))
	}

	router.Handle(graphEndpoint, s.graphHTTPHandler)

	if !s.opts.natsServerOption.TLS {
		return http.Serve(s.graphListener, router)
	}

	return http.ServeTLS(s.graphListener, router, s.opts.natsServerOption.TLSCaCert, s.opts.natsServerOption.TLSKey)
}

func (s *Server) WaitForShutdown() {
	s.natsServer.WaitForShutdown()
	// close http server
	_ = s.graphListener.Close()
}

func connServer(opts *natsServer.Options) *natsServer.Server {
	// Create the server with appropriate options.
	s, err := natsServer.NewServer(opts)
	if err != nil {
		PrintAndDie(fmt.Sprintf("%s: %s", exe, err))
	}

	// Configure the logger based on the flags
	//lg := logger.NewSysLogger(true, true)
	//s.SetLoggerV2(lg, true, true, true)
	bindServerLogger(s, opts)
	//s.ConfigureLogger()

	// Start things up. Block here until done.
	if err := natsServer.Run(s); err != nil {
		PrintAndDie(err.Error())
	}

	return s
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

func trim(s string) string {
	return strings.TrimSpace(s)
}

var UsageStr = `
Usage: graphrpc-server [options]
GraphRPC Server Options:
    -a, --addr <host>                Bind to host address (default: 0.0.0.0)
    -p, --port <port>                Use port for clients (default: 4222)
	-path, --path <base_path>        Base path for playground ( default: /graph )
    -n, --name <server_name>         Server name (default: auto)
    -P, --pid <file>                 File to store PID
    -m, --http_port <port>           Use port for http monitoring
    -ms,--https_port <port>          Use port for https monitoring
    -c, --config <file>              Configuration file
    -t                               Test configuration and exit
    -sl,--signal <signal>[=<pid>]    Send signal to nats-server process (stop, quit, reopen, reload)
                                     <pid> can be either a PID (e.g. 1) or the path to a PID file (e.g. /var/run/graphrpc-server.pid)
        --client_advertise <string>  Client URL to advertise to other servers
Logging Options:
    -l, --log <file>                 File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Log to syslog or windows event log
    -r, --remote_syslog <addr>       Syslog server addr (udp://localhost:514)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -VV                              Verbose trace (traces system account as well)
    -DV                              Debug and trace
    -DVV                             Debug and verbose trace (traces system account as well)
JetStream Options:
    -js, --jetstream                 Enable JetStream functionality.
    -sd, --store_dir <dir>           Set the storage directory.
Authorization Options:
        --user <user>                User required for connections
        --pass <password>            Password required for connections
        --auth <token>               Authorization token required for connections
TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert <file>             Server certificate file
        --tlskey <file>              Private key for server certificate
        --tlsverify                  Enable TLS, verify client certificates
        --tlscacert <file>           Client certificate CA for verification
Cluster Options:
        --routes <rurl-1, rurl-2>    Routes to solicit and connect
        --cluster <cluster-url>      Cluster URL for solicited routes
        --cluster_name <string>      Cluster Name, if not set one will be dynamically generated
        --no_advertise <bool>        Do not advertise known cluster information to clients
        --cluster_advertise <string> Cluster URL to advertise to other servers
        --connect_retries <number>   For implicit routes, number of connect retries
Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help
`

// usage will print out the flag options for the server.
func usage() {
	fmt.Printf("%s\n", UsageStr)
	os.Exit(0)
}
