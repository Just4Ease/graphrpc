// Copyright 2019-2021 The NATS Authors
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
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"
)

// DefaultTestOptions are default options for the unit tests.
var DefaultTestOptions = Options{
	Host:                  "127.0.0.1",
	Port:                  4222,
	NoLog:                 true,
	NoSigs:                true,
	MaxControlLine:        4096,
	DisableShortFirstPing: true,
}

func testDefaultClusterOptionsForLeafNodes() *Options {
	o := DefaultTestOptions
	o.Port = -1
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	o.Gateway.Host = o.Host
	o.Gateway.Port = -1
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = -1
	return &o
}

func RunRandClientPortServer() *Server {
	opts := DefaultTestOptions
	opts.Port = -1
	return RunServer(&opts)
}

// Used to setup clusters of clusters for tests.
type cluster struct {
	servers []*Server
	opts    []*Options
	name    string
	t       *testing.T
}

func checkNatsError(t *testing.T, e *ApiError, id ErrorIdentifier) {
	t.Helper()
	ae, ok := ApiErrors[id]
	if !ok {
		t.Fatalf("Unknown error ID identifier: %d", id)
	}

	if e.ErrCode != ae.ErrCode {
		t.Fatalf("Did not get NATS Error %d: %+v", e.ErrCode, e)
	}
}

// Creates a full cluster with numServers and given name and makes sure its well formed.
// Will have Gateways and Leaf Node connections active.
func createClusterWithName(t *testing.T, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()
	return createClusterEx(t, false, 5*time.Millisecond, true, clusterName, numServers, connectTo...)
}

// Creates a cluster and optionally additional accounts and users.
// Will have Gateways and Leaf Node connections active.
func createClusterEx(t *testing.T, doAccounts bool, gwSolicit time.Duration, waitOnGWs bool, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()

	if clusterName == "" || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Setup some accounts and users.
	// $SYS is always the system account. And we have default FOO and BAR accounts, as well
	// as DLC and NGS which do a service import.
	createAccountsAndUsers := func() ([]*Account, []*User) {
		if !doAccounts {
			return []*Account{NewAccount("$SYS")}, nil
		}

		sys := NewAccount("$SYS")
		ngs := NewAccount("NGS")
		dlc := NewAccount("DLC")
		foo := NewAccount("FOO")
		bar := NewAccount("BAR")

		accounts := []*Account{sys, foo, bar, ngs, dlc}

		ngs.AddServiceExport("ngs.usage.*", nil)
		dlc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.dlc")

		// Setup users
		users := []*User{
			{Username: "dlc", Password: "pass", Permissions: nil, Account: dlc},
			{Username: "ngs", Password: "pass", Permissions: nil, Account: ngs},
			{Username: "foo", Password: "pass", Permissions: nil, Account: foo},
			{Username: "bar", Password: "pass", Permissions: nil, Account: bar},
			{Username: "sys", Password: "pass", Permissions: nil, Account: sys},
		}
		return accounts, users
	}

	bindGlobal := func(s *Server) {
		ngs, err := s.LookupAccount("NGS")
		if err != nil {
			return
		}
		// Bind global to service import
		gacc, _ := s.LookupAccount("$G")
		gacc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.$G")
	}

	// If we are going to connect to another cluster set that up now for options.
	var gws []*RemoteGatewayOpts
	for _, c := range connectTo {
		// Gateways autodiscover here too, so just need one address from the set.
		gwAddr := fmt.Sprintf("nats-gw://%s:%d", c.opts[0].Gateway.Host, c.opts[0].Gateway.Port)
		gwurl, _ := url.Parse(gwAddr)
		gws = append(gws, &RemoteGatewayOpts{Name: c.name, URLs: []*url.URL{gwurl}})
	}

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(gwSolicit)
	defer ResetGatewaysSolicitDelay()

	// Create seed first.
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = clusterName
	o.Gateway.Gateways = gws
	// All of these need system accounts.
	o.Accounts, o.Users = createAccountsAndUsers()
	o.SystemAccount = "$SYS"
	o.ServerName = fmt.Sprintf("%s1", clusterName)
	// Run the server
	s := RunServer(o)
	bindGlobal(s)

	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	// For connecting to seed server above.
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	rurl, _ := url.Parse(routeAddr)
	routes := []*url.URL{rurl}

	for i := 1; i < numServers; i++ {
		o := testDefaultClusterOptionsForLeafNodes()
		o.Gateway.Name = clusterName
		o.Gateway.Gateways = gws
		o.Routes = routes
		// All of these need system accounts.
		o.Accounts, o.Users = createAccountsAndUsers()
		o.SystemAccount = "$SYS"
		o.ServerName = fmt.Sprintf("%s%d", clusterName, i+1)
		s := RunServer(o)
		bindGlobal(s)

		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	checkClusterFormed(t, c.servers...)

	if waitOnGWs {
		// Wait on gateway connections if we were asked to connect to other gateways.
		if numGWs := len(connectTo); numGWs > 0 {
			for _, s := range c.servers {
				waitForOutboundGateways(t, s, numGWs, 2*time.Second)
			}
		}
	}
	c.t = t
	return c
}

func (c *cluster) shutdown() {
	if c == nil {
		return
	}
	for i, s := range c.servers {
		sd := s.StoreDir()
		s.Shutdown()
		if cf := c.opts[i].ConfigFile; cf != _EMPTY_ {
			os.Remove(cf)
		}
		if sd != _EMPTY_ {
			os.RemoveAll(sd)
		}
	}
}

func shutdownCluster(c *cluster) {
	c.shutdown()
}

func (c *cluster) randomServer() *Server {
	return c.randomServerFromCluster(c.name)
}

func (c *cluster) randomServerFromCluster(cname string) *Server {
	// Since these can be randomly shutdown in certain tests make sure they are running first.
	// Copy our servers list and shuffle then walk looking for first running server.
	cs := append(c.servers[:0:0], c.servers...)
	rand.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })

	for _, s := range cs {
		if s.Running() && s.ClusterName() == cname {
			return s
		}
	}
	return nil
}

func runSolicitLeafServer(lso *Options) (*Server, *Options) {
	return runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
}

func runSolicitLeafServerToURL(surl string) (*Server, *Options) {
	o := DefaultTestOptions
	o.Port = -1
	o.NoSystemAccount = true
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(&o), &o
}
