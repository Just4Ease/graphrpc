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
	"github.com/gabriel-vasile/mimetype"
	"github.com/nats-io/nats.go"
	nSrv "graphrpc/libs/nats-server/server"
	"io/ioutil"
	goLog "log"
	"net"
	"net/http"

	"os"
	"strings"
	"sync"
)

type Options struct {
	serverName              string        // default: GraphRPC
	natsServerOption        *nSrv.Options // server nats-server options
	natsServerClientOptions []nats.Option // server nats-client options
	graphEntrypoint         string        // graph entrypoint
}

func NewServerOptions() *Options {
	return &Options{
		serverName:              "GraphRPC",
		graphEntrypoint:         "graph", // forward slash graph
		natsServerOption:        nil,
		natsServerClientOptions: make([]nats.Option, 0),
	}
}

func (o *Options) SetNatsServerOpts(opts *nSrv.Options) {
	args := os.Args[1:]
	args = append(args, "-n", "GraphRPC")
	// Create a FlagSet and sets the usage
	fs := flag.NewFlagSet(exe, flag.ExitOnError)
	fs.Usage = usage

	// Configure the options from the flags/config file
	argsOrConfigOptions, err := nSrv.ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, nSrv.PrintTLSHelpAndDie)
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

type Server struct {
	mu                       *sync.Mutex
	natsServer               *nSrv.Server          // server nats-server
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

func NewServer(address string, handler http.Handler, option *Options) *Server {
	if option == nil {
		goLog.Print("GraphRPC options not provided, using defaults...")
		option = NewServerOptions()
		option.SetNatsServerOpts(nil)
	}

	if option.natsServerOption == nil {
		goLog.Print("nats server configuration not provided, using defaults...")
		option.SetNatsServerOpts(nil)
	}

	option.natsServerClientOptions = append(option.natsServerClientOptions, nats.Name(option.serverName))
	return &Server{
		mu:               &sync.Mutex{},
		address:          address,
		opts:             option,
		graphHTTPHandler: handler,
		graphEntrypoint:  option.graphEntrypoint,
	}
}

func (s *Server) Serve() error {

	ls, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.graphListener = ls

	s.natsServer = connServer(s.opts.natsServerOption)

	address := s.natsServer.Addr()
	if s.snc, err = nats.Connect(address.String(), s.opts.natsServerClientOptions...); err != nil {
		PrintAndDie(err.Error())
		return err
	}

	if s.sjs, err = s.snc.JetStream(); err != nil {
		PrintAndDie(err.Error())
		return err
	}

	// graphs...
	go func() {
		root := fmt.Sprintf("%s.%s", s.opts.serverName, s.graphEntrypoint)
		// TODO: Check tls here and switch base to https.
		// TODO: Use axon format.
		endpoint := fmt.Sprintf("http://%s/%s", s.address, s.graphEntrypoint)
		_, err := s.snc.QueueSubscribe(root, s.opts.serverName, func(msg *nats.Msg) {
			PrettyJson(string(msg.Data))
			r, err := http.Post(endpoint, mimetype.Detect(msg.Data).String(), bytes.NewReader(msg.Data))
			if err != nil {
				fmt.Print(err)
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
	}()

	if err := http.Serve(s.graphListener, s.graphHTTPHandler); err != nil {
		return err
	}

	return nil
}

func (s *Server) WaitForShutdown() {
	s.natsServer.WaitForShutdown()
	// close http server
	_ = s.graphListener.Close()
}

func connServer(opts *nSrv.Options) *nSrv.Server {
	// Create the server with appropriate options.
	s, err := nSrv.NewServer(opts)
	if err != nil {
		PrintAndDie(fmt.Sprintf("%s: %s", exe, err))
	}

	// Configure the logger based on the flags
	//lg := logger.NewSysLogger(true, true)
	//s.SetLoggerV2(lg, true, true, true)
	bindServerLogger(s, opts)
	//s.ConfigureLogger()

	// Start things up. Block here until done.
	if err := nSrv.Run(s); err != nil {
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
