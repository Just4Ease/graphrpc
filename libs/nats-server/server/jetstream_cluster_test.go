// Copyright 2020-2021 The NATS Authors
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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: "%s"}
		cluster { listen: 127.0.0.1:-1 }
	`))
	defer removeFile(t, conf)

	check := func(errStr string) {
		t.Helper()
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), errStr) {
			t.Fatalf("Expected an error of `%s`, got `%v`", errStr, err)
		}
	}

	check("requires `server_name`")

	conf = createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "TEST"
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: "%s"}
		cluster { listen: 127.0.0.1:-1 }
	`))
	defer removeFile(t, conf)

	check("requires `cluster.name`")
}

func TestJetStreamClusterLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Kill our current leader and force an election.
	c.leader().Shutdown()
	c.waitOnLeader()

	// Now killing our current leader should leave us leaderless.
	c.leader().Shutdown()
	c.expectNoLeader()
}

func TestJetStreamExpandCluster(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 2)
	defer c.shutdown()

	c.addInNewServer()
	c.waitOnPeerCount(3)
}

func TestJetStreamClusterAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	reply := nats.NewInbox()
	sub, _ := nc.SubscribeSync(reply)

	if err := nc.PublishRequest(JSApiAccountInfo, reply, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	resp, _ := sub.NextMsg(0)

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.JetStreamAccountStats == nil || info.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", info.Error)
	}
	// Make sure we only got 1 response.
	// Technically this will always work since its a singelton service export.
	if nmsgs, _, _ := sub.Pending(); nmsgs > 0 {
		t.Fatalf("Expected only a single response, got %d more", nmsgs)
	}
}

func TestJetStreamClusterStreamLimitWithAccountDefaults(t *testing.T) {
	// 2MB memory, 8MB disk
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
		MaxBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Replicas: 2,
		MaxBytes: 15 * 1024 * 1024,
	})
	if err == nil || !strings.Contains(err.Error(), "insufficient storage") {
		t.Fatalf("Expected %v but got %v", ApiErrors[JSStorageResourcesExceededErr], err)
	}
}

func TestJetStreamClusterSingleReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected si to have cluster info")
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be pinned to same server that our stream was allocated to.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}

	// Now make sure that if we kill and restart the server that this stream and consumer return.
	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.restartServer(sl)

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Now durable consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	if _, err = js.ConsumerInfo("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterMultiReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be affinitize to the same set of servers as the stream.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" || ci.NumPending != uint64(toSend) {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}
}

func TestJetStreamClusterMemoryStore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3M", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := []byte("Hello MemoryStore"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	if si.Cluster == nil || len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Cluster info is incorrect: %+v", si.Cluster)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)
}

func TestJetStreamClusterDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "C22",
		Subjects: []string{"foo", "bar", "baz"},
		Replicas: 2,
		Storage:  nats.FileStorage,
		MaxMsgs:  100,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Now create a consumer.
	if _, err := js.AddConsumer("C22", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}

	// Now delete the consumer.
	if err := js.DeleteConsumer("C22", "dlc"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	// Now delete the stream.
	if err := js.DeleteStream("C22"); err != nil {
		t.Fatalf("Error deleting stream: %v", err)
	}

	// This will get the current information about usage and limits for this account.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		if err != nil {
			return err
		}
		if info.Streams != 0 {
			return fmt.Errorf("Expected no remaining streams, got %d", info.Streams)
		}
		return nil
	})
}

func TestJetStreamClusterStreamPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if si.State.Msgs != 0 || si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected no msgs, got: %+v", si.State)
	}
}

func TestJetStreamClusterStreamUpdateSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can update subjects.
	cfg.Subjects = []string{"bar", "baz"}

	si, err := js.UpdateStream(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil {
		t.Fatalf("Expected a stream info, got none")
	}
	if !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Expected subjects to be updated: got %+v", si.Config.Subjects)
	}
	// Make sure it registered
	js2, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js2.Publish("foo", nil); err == nil {
		t.Fatalf("Expected this to fail")
	}
	if _, err = js2.Publish("baz", nil); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
}

func TestJetStreamClusterBadStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg, toSend := []byte("Keep Me"), 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Make sure a bad update will not remove our stream.
	cfg.Subjects = []string{"foo..bar"}
	if _, err := js.UpdateStream(cfg); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Make sure we did not delete our original stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(si.Config.Subjects, []string{"foo", "bar"}) {
		t.Fatalf("Expected subjects to be original ones, got %+v", si.Config.Subjects)
	}
}

func TestJetStreamClusterConsumerRedeliveredInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST"}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.Publish("TEST", []byte("CI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, _ := nc.SubscribeSync("R")
	sub.AutoUnsubscribe(2)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: "R",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 2)
	sub.Unsubscribe()

	ci, err = js.ConsumerInfo("TEST", ci.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumRedelivered != 1 {
		t.Fatalf("Expected 1 redelivered, got %d", ci.NumRedelivered)
	}
}

func TestJetStreamClusterConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull 5 messages and ack.
	for i := 0; i < 5; i++ {
		msgs := fetchMsgs(t, sub, 1, 5*time.Second)
		m := msgs[0]
		m.Ack()
	}

	// Let state propagate for exact comparison below.
	time.Sleep(200 * time.Millisecond)

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	if ci.AckFloor.Consumer != 5 {
		t.Fatalf("Expected ack floor of %d, got %d", 5, ci.AckFloor.Consumer)
	}

	c.consumerLeader("$G", "TEST", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	nci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	if nci.Delivered != ci.Delivered {
		t.Fatalf("Consumer delivered did not match after leader switch, wanted %+v, got %+v", ci.Delivered, nci.Delivered)
	}
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Consumer ackfloor did not match after leader switch, wanted %+v, got %+v", ci.AckFloor, nci.AckFloor)
	}

	// Now make sure we can receive new messages.
	// Pull last 5.
	for i := 0; i < 5; i++ {
		msgs := fetchMsgs(t, sub, 1, 5*time.Second)
		m := msgs[0]
		m.Ack()
	}
	nci, _ = sub.ConsumerInfo()
	if nci.Delivered.Consumer != 10 || nci.Delivered.Stream != 10 {
		t.Fatalf("Received bad delivered: %+v", nci.Delivered)
	}
	if nci.AckFloor.Consumer != 10 || nci.AckFloor.Stream != 10 {
		t.Fatalf("Received bad ackfloor: %+v", nci.AckFloor)
	}
	if nci.NumAckPending != 0 {
		t.Fatalf("Received bad ackpending: %+v", nci.NumAckPending)
	}
}

func TestJetStreamClusterFullConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchMsgs(t, sub, 1, 5*time.Second)

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterMetaSnapshotsAndCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Shut one down.
	rs := c.randomServer()
	rs.Shutdown()

	c.waitOnLeader()
	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	numStreams := 4
	// Create 4 streams
	// FIXME(dlc) - R2 make sure we place properly.
	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		_, err := js.AddStream(&nats.StreamConfig{Name: sn})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	c.leader().JetStreamSnapshotMeta()

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)

	rs.Shutdown()
	c.waitOnLeader()

	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		err := js.DeleteStream(sn)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterMetaSnapshotsMultiChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 2)
	defer c.shutdown()

	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Add in 2 streams with 1 consumer each.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S1"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err := js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.AddStream(&nats.StreamConfig{Name: "S2"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "S2C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Add in a new server to the group. This way we know we can delete the original streams and consumers.
	rs := c.addInNewServer()
	c.waitOnServerCurrent(rs)
	rsn := rs.Name()

	// Shut it down.
	rs.Shutdown()

	// Wait for the peer to be removed.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, p := range s.JetStreamClusterPeers() {
			if p == rsn {
				return fmt.Errorf("Old server still in peer set")
			}
		}
		return nil
	})

	// We want to make changes here that test each delta scenario for the meta snapshots.
	// Add new stream and consumer.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "S3"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.AddConsumer("S3", &nats.ConsumerConfig{Durable: "S3C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Delete stream S2
	resp, _ := nc.Request(fmt.Sprintf(JSApiStreamDeleteT, "S2"), nil, time.Second)
	var dResp JSApiStreamDeleteResponse
	if err := json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}
	// Delete the consumer on S1 but add another.
	resp, _ = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, "S1", "S1C1"), nil, time.Second)
	var cdResp JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", cdResp)
	}
	// Add new consumer on S1
	_, err = js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C2", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cl := c.leader()
	cl.JetStreamSnapshotMeta()
	c.waitOnServerCurrent(cl)

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterStreamSynchedTimeStamps(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Storage: nats.MemoryStorage, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Grab the message and timestamp from our current leader
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, _ := m.Metadata()

	sub.Unsubscribe()

	sl := c.streamLeader("$G", "foo")

	sl.Shutdown()

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "foo")

	nc, js = jsClientConnect(t, c.leader())
	defer nc.Close()

	sm, err := js.GetMsg("foo", 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !sm.Time.Equal(meta.Timestamp) {
		t.Fatalf("Expected same timestamps, got %v vs %v", sm.Time, meta.Timestamp)
	}
}

// Test to mimic what R.I. was seeing.
func TestJetStreamClusterRestoreSingleConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.Ack()
	}

	c.stopAll()
	c.restartAll()
	c.waitOnLeader()

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected 1 stream but got %d", len(infos))
	}
	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Now check for consumer.
	names = names[:0]
	for name := range js.ConsumerNames("foo") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterMaxBytesForStream(t *testing.T) {
	// Has max_file_store of 2GB
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	info, err := js.AccountInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we still are dynamic.
	if info.Limits.MaxStore != -1 || info.Limits.MaxMemory != -1 {
		t.Fatalf("Expected dynamic limits for the account, got %+v\n", info.Limits)
	}
	// Stream config.
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	}
	// 2GB
	cfg.MaxBytes = 2 * 1024 * 1024 * 1024
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure going over the single server limit though is enforced (for now).
	cfg.MaxBytes *= 2
	_, err = js.AddStream(cfg)
	if err == nil || !strings.Contains(err.Error(), "insufficient storage resources") {
		t.Fatalf("Expected %q error, got %q", "insufficient storage resources", err.Error())
	}
}

func TestJetStreamClusterStreamPublishWithActiveConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// FIXME(dlc) - Need to track this down.
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.Ack()
	}

	// Send 10 messages.
	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}
	for i := 1; i <= 10; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	c.consumerLeader("$G", "foo", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	ci2, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	ci.Cluster = nil
	ci2.Cluster = nil

	if !reflect.DeepEqual(ci, ci2) {
		t.Fatalf("Consumer info did not match: %+v vs %+v", ci, ci2)
	}

	// In case the server above was also stream leader.
	c.waitOnStreamLeader("$G", "foo")

	// Now send more..
	// Send 10 more messages.
	for i := 11; i <= 20; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}

	for i := 11; i <= 20; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}
}

func TestJetStreamClusterStreamOverlapSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST2", Subjects: []string{"foo"}}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout: %v", err)
	}

	// Now grab list of streams and make sure the second is not there.
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do a detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(infos))
	}
}

func TestJetStreamClusterStreamInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(name string) {
		t.Helper()
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	createStream("foo")
	createStream("bar")
	createStream("baz")

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 10)
	sendBatch("bar", 22)
	sendBatch("baz", 33)

	// Now get the stream list info.
	var infos []*nats.StreamInfo
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		infos = infos[:0]
		for info := range js.StreamsInfo() {
			infos = append(infos, info)
		}
		if len(infos) != 3 {
			return fmt.Errorf("StreamInfo expected 3 results, got %d", len(infos))
		}
		return nil
	})

	for _, si := range infos {
		switch si.Config.Name {
		case "foo":
			if si.State.Msgs != 10 {
				t.Fatalf("Expected %d msgs but got %d", 10, si.State.Msgs)
			}
		case "bar":
			if si.State.Msgs != 22 {
				t.Fatalf("Expected %d msgs but got %d", 22, si.State.Msgs)
			}
		case "baz":
			if si.State.Msgs != 33 {
				t.Fatalf("Expected %d msgs but got %d", 33, si.State.Msgs)
			}
		}
	}
}

func TestJetStreamClusterConsumerInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Place messages so we can generate consumer state.
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	createConsumer := func(name string) *nats.Subscription {
		t.Helper()
		sub, err := js.PullSubscribe("TEST", name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return sub
	}

	subFoo := createConsumer("foo")
	subBar := createConsumer("bar")
	subBaz := createConsumer("baz")

	// Place consumers in various states.
	for _, ss := range []struct {
		sub   *nats.Subscription
		fetch int
		ack   int
	}{
		{subFoo, 4, 2},
		{subBar, 2, 0},
		{subBaz, 8, 6},
	} {
		msgs := fetchMsgs(t, ss.sub, ss.fetch, 5*time.Second)
		for i := 0; i < ss.ack; i++ {
			msgs[i].Ack()
		}
	}

	// Now get the consumer list info.
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 3 {
		t.Fatalf("ConsumerInfo expected 3 results, got %d", len(infos))
	}
	for _, ci := range infos {
		switch ci.Name {
		case "foo":
			if ci.Delivered.Consumer != 4 {
				t.Fatalf("Expected %d delivered but got %d", 4, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 2 {
				t.Fatalf("Expected %d for ack floor but got %d", 2, ci.AckFloor.Consumer)
			}
		case "bar":
			if ci.Delivered.Consumer != 2 {
				t.Fatalf("Expected %d delivered but got %d", 2, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 0 {
				t.Fatalf("Expected %d for ack floor but got %d", 0, ci.AckFloor.Consumer)
			}
		case "baz":
			if ci.Delivered.Consumer != 8 {
				t.Fatalf("Expected %d delivered but got %d", 8, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 6 {
				t.Fatalf("Expected %d for ack floor but got %d", 6, ci.AckFloor.Consumer)
			}
		}
	}
}

func TestJetStreamClusterStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sc := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		MaxMsgs:  10,
		Discard:  DiscardNew,
	}

	if _, err := js.AddStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 1; i <= int(sc.MaxMsgs); i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Expect error here.
	if _, err := js.Publish("foo", []byte("fail")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Now update MaxMsgs, select non-leader
	s = c.randomNonStreamLeader("$G", "TEST")
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	sc.MaxMsgs = 20
	si, err := js.UpdateStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxMsgs != 20 {
		t.Fatalf("Expected to have config updated with max msgs of %d, got %d", 20, si.Config.MaxMsgs)
	}

	// Do one that will fail. Wait and make sure we only are getting one response.
	sc.Name = "TEST22"

	rsub, _ := nc.SubscribeSync(nats.NewInbox())
	defer rsub.Unsubscribe()
	nc.Flush()

	req, _ := json.Marshal(sc)
	if err := nc.PublishRequest(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), rsub.Subject, req); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait incase more than one reply sent.
	time.Sleep(250 * time.Millisecond)

	if nmsgs, _, _ := rsub.Pending(); err != nil || nmsgs != 1 {
		t.Fatalf("Expected only one response, got %d", nmsgs)
	}

	m, err := rsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(m.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil {
		t.Fatalf("Did not receive correct response: %+v", scResp)
	}
}

func TestJetStreamClusterStreamExtendedUpdates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updateStream := func() *nats.StreamInfo {
		si, err := js.UpdateStream(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	expectError := func() {
		if _, err := js.UpdateStream(cfg); err == nil {
			t.Fatalf("Expected error and got none")
		}
	}

	// Subjects
	cfg.Subjects = []string{"bar", "baz"}
	if si := updateStream(); !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	// Make sure these error for now.
	// R factor changes
	cfg.Replicas = 1
	expectError()
	// Mirror changes
	cfg.Replicas = 3
	cfg.Mirror = &nats.StreamSource{Name: "ORDERS"}
	expectError()
}

func TestJetStreamClusterDoubleAdd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check double add fails.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Do Consumers too.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check double add fails.
	if _, err := js.AddConsumer("TEST", cfg); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}
}

func TestJetStreamClusterDefaultMaxAckPending(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Do Consumers too.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	ci, err := js.AddConsumer("TEST", cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that we have a default set now for the max ack pending.
	if ci.Config.MaxAckPending != JsDefaultMaxAckPending {
		t.Fatalf("Expected a default for max ack pending of %d, got %d", JsDefaultMaxAckPending, ci.Config.MaxAckPending)
	}
}

func TestJetStreamClusterStreamNormalCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	// Send 10 more while one replica offline.
	for i := toSend; i <= toSend*2; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Delete the first from the second batch.
	dreq := JSApiMsgDeleteRequest{Seq: uint64(toSend)}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ := nc.Request(fmt.Sprintf(JSApiMsgDeleteT, "TEST"), dreqj, time.Second)
	var delMsgResp JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pseq := uint64(1)
	sendBatch := func(n int) {
		t.Helper()
		// Send a batch.
		for i := 0; i < n; i++ {
			msg := []byte(fmt.Sprintf("HELLO JSC-%d", pseq))
			if _, err = js.Publish("foo", msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
			pseq++
		}
	}

	sendBatch(2)

	sl := c.streamLeader("$G", "TEST")

	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	sendBatch(100)

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Delete the first from the second batch.
	deleteMsg(pseq / 2)
	// Delete the next one too.
	deleteMsg(pseq/2 + 1)

	nsl := c.streamLeader("$G", "TEST")

	nsl.JetStreamSnapshotStream("$G", "TEST")

	// Do some activity post snapshot as well.
	// Delete next to last.
	deleteMsg(pseq - 2)
	// Send another batch.
	sendBatch(100)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")
}

func TestJetStreamClusterDeleteMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	// Also make sure purge of R=1 works too.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterDeleteMsgAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchupWithPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "TEST")

	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nsl := c.streamLeader("$G", "TEST")
	if err := nsl.JetStreamSnapshotStream("$G", "TEST"); err != nil {
		t.Fatalf("Error snapshotting stream: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	// Now purge the stream while we are recovering.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	nsl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterExtendedStreamInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}

	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				if si, err = js2.StreamInfo("TEST"); err != nil {
					t.Fatalf("Could not retrieve stream info")
				}
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Shutdown the leader.
	oldLeader := c.streamLeader("$G", "TEST")
	oldLeader.Shutdown()

	c.waitOnStreamLeader("$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}
	for _, peer := range si.Cluster.Replicas {
		if peer.Name == oldLeader.Name() {
			if peer.Current {
				t.Fatalf("Expected old leader to be reported as not current: %+v", peer)
			}
		} else if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
	}

	// Now send a few more messages then restart the oldLeader.
	for i := 0; i < 10; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	oldLeader = c.restartServer(oldLeader)
	c.checkClusterFormed()

	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamCurrent(oldLeader, "$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				if si, err = js2.StreamInfo("TEST"); err != nil {
					t.Fatalf("Could not retrieve stream info")
				}
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	fetchMsgs(t, sub, 10, 5*time.Second)

	leader = c.consumerLeader("$G", "TEST", "dlc").Name()
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	if ci.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, ci.Cluster.Leader)
	}
	if len(ci.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(ci.Cluster.Replicas))
	}
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterExtendedStreamInfoSingleReplica(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 0 {
		t.Fatalf("Expected no replicas but got %d", len(si.Cluster.Replicas))
	}

	// Make sure we can grab consumer lists from any
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 0 {
		t.Fatalf("ConsumerInfo expected no paged results, got %d", len(infos))
	}

	// Now add in a consumer.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	infos = infos[:0]
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("ConsumerInfo expected 1 result, got %d", len(infos))
	}

	// Now do direct names list as well.
	var names []string
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterInterestRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "foo")
	cl := c.consumerLeader("$G", "foo", "dlc")
	if sl == cl {
		_, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "foo"), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c.waitOnStreamLeader("$G", "foo")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error getting msg: %v", err)
	}
	m.Ack()

	waitForZero := func() {
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 0 {
				return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
			}
			return nil
		})
	}

	waitForZero()

	// Add in 50 messages.
	for i := 0; i < 50; i++ {
		if _, err = js.Publish("foo", []byte("more")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 50)

	// Now delete the consumer and make sure the stream goes to zero.
	if err := js.DeleteConsumer("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForZero()
}

// https://github.com/nats-io/nats-server/issues/2243
func TestJetStreamClusterWorkQueueRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "FOO",
		Subjects:  []string{"foo.*"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo.test", "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo.test", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	si, err := js.StreamInfo("FOO")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got state: %+v", si.State)
	}

	// Fetch from our pull consumer and ack.
	for _, m := range fetchMsgs(t, sub, 1, 5*time.Second) {
		m.Ack()
	}

	// Make sure the messages are removed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("FOO")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceWorkQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "WQ22",
		Subjects:  []string{"foo"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "WQ22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "WQ22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow sync consumers to connect.
	time.Sleep(500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js.StreamInfo("WQ22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "WQ22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceInterestPolicyStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "IP22",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "IP22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "IP22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow sync consumers to connect.
	time.Sleep(500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

	// Now create other interest on IP22.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// Allow consumer state to propagate.
	time.Sleep(500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterInterestRetentionWithFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"*"}, Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fsub, err := js.SubscribeSync("foo", nats.Durable("d1"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fsub.Unsubscribe()

	bsub, err := js.SubscribeSync("bar", nats.Durable("d2"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer bsub.Unsubscribe()

	msg := []byte("FILTERED")
	sendMsg := func(subj string) {
		t.Helper()
		if _, err = js.Publish(subj, msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getAndAck := func(sub *nats.Subscription) {
		t.Helper()
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg: %v", err)
		}
		m.Ack()
	}

	jsq, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkState := func(expected uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := jsq.StreamInfo("TEST")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %d msgs, got %d", expected, si.State.Msgs)
			}
			return nil
		})
	}

	sendMsg("foo")
	checkState(1)
	getAndAck(fsub)
	checkState(0)
	sendMsg("bar")
	sendMsg("foo")
	checkState(2)
	getAndAck(bsub)
	checkState(1)
	getAndAck(fsub)
	checkState(0)

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(20)

	if err := js.DeleteConsumer("TEST", "d1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", "d2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)

	// Now make sure pull based consumers work same.
	if _, err := js.PullSubscribe("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(10)

	if err := js.DeleteConsumer("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)
}

func TestJetStreamClusterEphemeralConsumerNoImmediateInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want to relax the strict interest requirement.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: "r"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cl := c.consumerLeader("$G", "TEST", ci.Name)
	mset, err := cl.GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "TEST")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(500 * time.Millisecond)

	// Make sure the consumer goes away though eventually.
	// Should be 5 seconds wait.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if _, err := js.ConsumerInfo("TEST", ci.Name); err != nil {
			return nil
		}
		return fmt.Errorf("Consumer still present")
	})
}

func TestJetStreamClusterEphemeralConsumerCleanup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	// We will look up by hand this consumer to set inactive threshold lower for this test.
	cl := c.consumerLeader("$G", "foo", ci.Name)
	if cl == nil {
		t.Fatalf("Could not find consumer leader")
	}
	mset, err := cl.GlobalAccount().lookupStream("foo")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "foo")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(10 * time.Millisecond)

	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getConsumers := func() []string {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var names []string
		for name := range js.ConsumerNames("foo", nats.Context(ctx)) {
			names = append(names, name)
		}
		return names
	}

	checkConsumer := func(expected int) {
		consumers := getConsumers()
		if len(consumers) != expected {
			t.Fatalf("Expected %d consumers but got %d", expected, len(consumers))
		}
	}

	checkConsumer(1)

	// Now Unsubscribe, since this is ephemeral this will make this go away.
	sub.Unsubscribe()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if consumers := getConsumers(); len(consumers) == 0 {
			return nil
		} else {
			return fmt.Errorf("Still %d consumers remaining", len(consumers))
		}
	})
}

func TestJetStreamClusterEphemeralConsumersNotReplicated(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	sub.NextMsg(0)

	if ci.Cluster == nil || len(ci.Cluster.Replicas) != 0 {
		t.Fatalf("Expected ephemeral to be R=1, got %+v", ci.Cluster)
	}
	scl := c.serverByName(ci.Cluster.Leader)
	if scl == nil {
		t.Fatalf("Could not select server where ephemeral consumer is running")
	}

	// Test migrations. If we are also metadata leader will not work so skip.
	if scl == c.leader() {
		return
	}

	scl.Shutdown()
	c.waitOnStreamLeader("$G", "foo")

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
		t.Logf("Expected to see another message, but behavior is optimistic so can fail")
	}
}

func TestJetStreamClusterUserSnapshotAndRestore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 200

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Create consumer with no state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "rip", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Second})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create another consumer as well and give it a non-simplistic state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Second})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	jsub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ack first 50.
	for _, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		m.Ack()
	}
	// Now ack every third message for next 50.
	for i, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		if i%3 == 0 {
			m.Ack()
		}
	}
	nc.Flush()
	time.Sleep(500 * time.Millisecond)

	// Snapshot consumer info.
	ci, err := jsub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}

	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Grab state for comparison.
	state := *resp.State
	config := *resp.Config

	var snapshot []byte
	done := make(chan bool)

	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- true
			return
		}
		// Could be writing to a file here too.
		snapshot = append(snapshot, m.Data...)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure a restore to an existing stream fails.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistErr) {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure a restore will work.
	// Delete our stream first.
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.StreamInfo("TEST"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected not found error: %v", err)
	}

	// This should work properly.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	// Send our snapshot back in to restore the stream.
	// Can be any size message.
	var chunk [1024]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" || si.State.Msgs != uint64(toSend) {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Make sure the replicas become current eventually. They will be doing catchup.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, _ := js.StreamInfo("TEST")
		if si == nil || si.Cluster == nil {
			t.Fatalf("Did not get stream info")
		}
		for _, pi := range si.Cluster.Replicas {
			if !pi.Current {
				return fmt.Errorf("Peer not current: %+v", pi)
			}
		}
		return nil
	})

	// Wait on the system to elect a leader for the restored consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	// Now check for the consumer being recreated.
	nci, err := js.ConsumerInfo("TEST", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if nci.Delivered != ci.Delivered {
		t.Fatalf("Delivered states do not match %+v vs %+v", nci.Delivered, ci.Delivered)
	}
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Ack floors did not match %+v vs %+v", nci.AckFloor, ci.AckFloor)
	}

	// Make sure consumer works.
	// It should pick up with the next delivery spot, so check for that as first message.
	// We should have all the messages for first delivery delivered.
	wantSeq := 101
	for _, m := range fetchMsgs(t, jsub, 100, 5*time.Second) {
		meta, err := m.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if meta.Sequence.Stream != uint64(wantSeq) {
			t.Fatalf("Expected stream sequence of %d, but got %d", wantSeq, meta.Sequence.Stream)
		}
		m.Ack()
		wantSeq++
	}

	// Check that redelivered come in now..
	redelivered := 50/3 + 1
	fetchMsgs(t, jsub, redelivered, 5*time.Second)

	// Now make sure the other server was properly caughtup.
	// Need to call this by hand for now.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(rmsg.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Unexpected stream info: %+v", si)
	}

	// Check idle consumer
	c.waitOnConsumerLeader("$G", "TEST", "rip")

	// Now check for the consumer being recreated.
	if _, err := js.ConsumerInfo("TEST", "rip"); err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
}

func TestJetStreamClusterUserSnapshotAndRestoreConfigChanges(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// FIXME(dlc) - Do case with R=1
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getSnapshot := func() ([]byte, *StreamState) {
		t.Helper()
		sreq := &JSApiStreamSnapshotRequest{
			DeliverSubject: nats.NewInbox(),
			ChunkSize:      1024,
		}

		req, _ := json.Marshal(sreq)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on snapshot request: %v", err)
		}

		var resp JSApiStreamSnapshotResponse
		json.Unmarshal(rmsg.Data, &resp)
		if resp.Error != nil {
			t.Fatalf("Did not get correct error response: %+v", resp.Error)
		}

		var snapshot []byte
		done := make(chan bool)

		sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
			// EOF
			if len(m.Data) == 0 {
				done <- true
				return
			}
			// Could be writing to a file here too.
			snapshot = append(snapshot, m.Data...)
			// Flow ack
			m.Respond(nil)
		})
		defer sub.Unsubscribe()

		// Wait to receive the snapshot.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive our snapshot in time")
		}
		return snapshot, resp.State
	}

	restore := func(cfg *StreamConfig, state *StreamState, snap []byte) *nats.StreamInfo {
		rreq := &JSApiStreamRestoreRequest{
			Config: *cfg,
			State:  *state,
		}
		req, err := json.Marshal(rreq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, cfg.Name), req, 5*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var rresp JSApiStreamRestoreResponse
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		// Send our snapshot back in to restore the stream.
		// Can be any size message.
		var chunk [1024]byte
		for r := bytes.NewReader(snap); ; {
			n, err := r.Read(chunk[:])
			if err != nil {
				break
			}
			nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
		}
		rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rresp.Error = nil
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		si, err := js.StreamInfo(cfg.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	snap, state := getSnapshot()

	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Change name.
	ncfg := &StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"foo"},
		Storage:  FileStorage,
		Replicas: 2,
	}
	if si := restore(ncfg, state, snap); si.Config.Name != "TEST2" {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now change subjects.
	ncfg.Subjects = []string{"bar", "baz"}
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Storage
	ncfg.Storage = MemoryStorage
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now replicas
	ncfg.Replicas = 3
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
}

func TestJetStreamClusterAccountInfoAndLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Adjust our limits.
	c.updateLimits("$G", &JetStreamAccountLimits{
		MaxMemory:    1024,
		MaxStore:     8000,
		MaxStreams:   3,
		MaxConsumers: 1,
	})

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "baz", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 25)
	sendBatch("bar", 75)
	sendBatch("baz", 10)

	accountStats := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	// If subject is not 3 letters or payload not 2 this needs to change.
	const msgSize = uint64(22 + 3 + 6 + 8)

	stats := accountStats()
	if stats.Streams != 3 {
		t.Fatalf("Should have been tracking 3 streams, found %d", stats.Streams)
	}
	expectedSize := 25*msgSize + 75*msgSize*2 + 10*msgSize*3
	// This may lag.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if stats.Store != expectedSize {
			err := fmt.Errorf("Expected store size to be %d, got %+v\n", expectedSize, stats)
			stats = accountStats()
			return err

		}
		return nil
	})

	// Check limit enforcement.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "fail", Replicas: 3}); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	// We should be at 7995 at the moment with a limit of 8000, so any message will go over.
	if _, err := js.Publish("baz", []byte("JSC-NOT-OK")); err == nil {
		t.Fatalf("Expected publish error but got none")
	}

	// Check consumers
	_, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// This should fail.
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc22", AckPolicy: nats.AckExplicitPolicy})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}

func TestJetStreamClusterStreamLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Check that large R will fail.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 5}); err == nil {
		t.Fatalf("Expected error but got none")
	}

	maxMsgs := 5

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "foo",
		Replicas:   3,
		Retention:  nats.LimitsPolicy,
		Discard:    DiscardNew,
		MaxMsgSize: 11,
		MaxMsgs:    int64(maxMsgs),
		MaxAge:     250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Large message should fail.
	if _, err := js.Publish("foo", []byte("0123456789ZZZ")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	for i := 0; i < maxMsgs; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// These should fail.
	if _, err := js.Publish("foo", []byte("JSC-OK")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Make sure when space frees up we can send more.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

	if _, err := js.Publish("foo", []byte("ROUND2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterStreamInterestOnlyPolicy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "foo",
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10

	// With no interest these should be no-ops.
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no messages with no interest, got %d", si.State.Msgs)
	}

	// Now create a consumer.
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, toSend)

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages with interest, got %d", toSend, si.State.Msgs)
	}
	if si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected first sequence of %d, got %d", toSend+1, si.State.FirstSeq)
	}

	// Now delete the consumer.
	sub.Unsubscribe()
	// That should make it go away.
	if _, err := js.ConsumerInfo("foo", "dlc"); err == nil {
		t.Fatalf("Expected not found error, got none")
	}

	// Wait for the messages to be purged.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Wanted 0 messages, got %d", si.State.Msgs)
	})
}

// These are disabled for now.
func TestJetStreamClusterStreamTemplates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// List API
	var tListResp JSApiStreamTemplateNamesResponse
	resp, err := nc.Request(JSApiTemplates, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if tListResp.Error == nil {
		t.Fatalf("Expected an unsupported error, got none")
	}
	if !strings.Contains(tListResp.Error.Description, "not currently supported in clustered mode") {
		t.Fatalf("Did not get correct error response: %+v", tListResp.Error)
	}

	// Create
	// Now do templates.
	mcfg := &StreamConfig{
		Subjects: []string{"kv.*"},
		Storage:  MemoryStorage,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}
	req, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var stResp JSApiStreamTemplateCreateResponse
	resp, err = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.Error == nil {
		t.Fatalf("Expected an unsupported error, got none")
	}
	if !strings.Contains(stResp.Error.Description, "not currently supported in clustered mode") {
		t.Fatalf("Did not get correct error response: %+v", stResp.Error)
	}
}

func TestJetStreamClusterExtendedAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	// Go client will lag so use direct for now.
	getAccountInfo := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	// Wait to accumulate.
	time.Sleep(500 * time.Millisecond)

	ai := getAccountInfo()
	if ai.Streams != 3 || ai.Consumers != 3 {
		t.Fatalf("AccountInfo not correct: %+v", ai)
	}
	if ai.API.Total < 8 {
		t.Fatalf("Expected at least 8 total API calls, got %d", ai.API.Total)
	}

	// Now do a failure to make sure we track API errors.
	js.StreamInfo("NO-STREAM")
	js.ConsumerInfo("TEST-1", "NO-CONSUMER")
	js.ConsumerInfo("TEST-2", "NO-CONSUMER")
	js.ConsumerInfo("TEST-3", "NO-CONSUMER")

	ai = getAccountInfo()
	if ai.API.Errors != 4 {
		t.Fatalf("Expected 4 API calls to be errors, got %d", ai.API.Errors)
	}
}

func TestJetStreamClusterPeerRemovalAPI(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Expect error if unknown peer
	req := &JSApiMetaServerRemoveRequest{Server: "S-9"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}

	sub, err := nc.SubscribeSync(JSAdvisoryServerRemoved)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rs := c.randomNonLeader()
	req = &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Error = nil
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	c.waitOnLeader()
	ml = c.leader()

	checkSubsPending(t, sub, 1)
	madv, _ := sub.NextMsg(0)
	var adv JSServerRemovedAdvisory
	if err := json.Unmarshal(madv.Data, &adv); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if adv.Server != rs.Name() {
		t.Fatalf("Expected advisory about %s being removed, got %+v", rs.Name(), adv)
	}

	for _, s := range ml.JetStreamClusterPeers() {
		if s == rs.Name() {
			t.Fatalf("Still in the peer list")
		}
	}
}

func TestJetStreamClusterPeerOffline(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	ml := c.leader()
	rs := c.randomNonLeader()

	checkPeer := func(ml, rs *Server, shouldBeOffline bool) {
		t.Helper()

		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			var found bool
			for _, s := range ml.JetStreamClusterPeers() {
				if s == rs.Name() {
					found = true
					break
				}
			}
			if !shouldBeOffline && !found {
				return fmt.Errorf("Server %q not in the peers list", rs.Name())
			} else if shouldBeOffline && found {
				return fmt.Errorf("Server %q should not be in the peers list", rs.Name())
			}

			var ok bool
			ml.nodeToInfo.Range(func(k, v interface{}) bool {
				if si := v.(nodeInfo); si.name == rs.Name() {
					if shouldBeOffline && si.offline || !shouldBeOffline && !si.offline {
						ok = true
						return false
					}
				}
				return true
			})
			if !ok {
				if shouldBeOffline {
					return fmt.Errorf("Server %q should be marked as online", rs.Name())
				}
				return fmt.Errorf("Server %q is still marked as online", rs.Name())
			}
			return nil
		})
	}

	// Shutdown the server and make sure that it is now showing as offline.
	rs.Shutdown()
	checkPeer(ml, rs, true)

	// Now restart that server and check that is no longer offline.
	oldrs := rs
	rs, _ = RunServerWithConfig(rs.getOpts().ConfigFile)
	defer rs.Shutdown()

	// Replaced old with new server
	for i := 0; i < len(c.servers); i++ {
		if c.servers[i] == oldrs {
			c.servers[i] = rs
		}
	}

	// Wait for cluster to be formed
	checkClusterFormed(t, c.servers...)

	// Make sure that we have a leader (there can always be a re-election)
	c.waitOnLeader()
	ml = c.leader()

	// Now check that rs is not offline
	checkPeer(ml, rs, false)
}

func TestJetStreamClusterNoQuorumStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup subscription for leader elected.
	lesub, err := nc.SubscribeSync(JSAdvisoryStreamLeaderElectedPre + ".*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our leader elected advisory.
	leadv, _ := lesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a leader elected advisory")
	}
	var le JSStreamLeaderElectedAdvisory
	if err := json.Unmarshal(leadv.Data, &le); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ln := c.streamLeader("$G", "NO-Q").Name(); le.Leader != ln {
		t.Fatalf("Expected to have leader %q in elect advisory, got %q", ln, le.Leader)
	}

	payload := []byte("Hello JSC")
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("NO-Q", payload); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Setup subscription for leader elected.
	clesub, err := nc.SubscribeSync(JSAdvisoryConsumerLeaderElectedPre + ".*.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make durable to have R match Stream.
	sub, err := js.SubscribeSync("NO-Q", nats.Durable("rr"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := sub.ConsumerInfo()
	if err != nil || ci == nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our consumer leader elected advisory.
	leadv, _ = clesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a consumer leader elected advisory")
	}

	// Shutdown the non-leader.
	c.randomNonStreamLeader("$G", "NO-Q").Shutdown()

	// This should eventually have us stepdown as leader since we would have lost quorum with R=2.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if sl := c.streamLeader("$G", "NO-Q"); sl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for stream")
	})

	notAvailableErr := func(err error) bool {
		return err != nil && (strings.Contains(err.Error(), "unavailable") || err == context.DeadlineExceeded)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if cl := c.consumerLeader("$G", "NO-Q", ci.Name); cl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for consumer")
	})

	if _, err = js.ConsumerInfo("NO-Q", ci.Name); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := sub.ConsumerInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}

	// Now let's take out the other non meta-leader
	// We should get same error for general API calls.
	c.randomNonLeader().Shutdown()
	c.expectNoLeader()

	// Now make sure the general JS API responds with system unavailable.
	if _, err = js.AccountInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.UpdateStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.PurgeStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteMsg("NO-Q", 1); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Consumer
	if _, err := js.AddConsumer("NO-Q", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteConsumer("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.ConsumerInfo("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Listers
	for info := range js.StreamsInfo() {
		t.Fatalf("Unexpected stream info, got %v", info)
	}
	for info := range js.ConsumersInfo("NO-Q") {
		t.Fatalf("Unexpected consumer info, got %v", info)
	}
}

func TestJetStreamClusterCreateResponseAdvisoriesHaveSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST", nats.Durable("DLC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 6)

	for m, err := sub.NextMsg(0); err == nil; m, err = sub.NextMsg(0) {
		var audit JSAPIAudit
		if err := json.Unmarshal(m.Data, &audit); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if audit.Subject == "" {
			t.Fatalf("Expected subject, got nothing")
		}
	}
}

func TestJetStreamClusterRestartAndRemoveAdvisories(t *testing.T) {
	// FIXME(dlc) - Flaky on Travis, skip for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	csub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.CREATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer csub.Unsubscribe()
	nc.Flush()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	drainSub := func(sub *nats.Subscription) {
		for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
		}
	}

	// Wait for the advisories for all streams and consumers.
	checkSubsPending(t, sub, 12) // 3 streams, 3*2 consumers, 3 stream names lookups for creating consumers.
	drainSub(sub)

	// Created audit events.
	checkSubsPending(t, csub, 6)
	drainSub(csub)

	usub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.UPDATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer usub.Unsubscribe()
	nc.Flush()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	// Now restart the other two servers we are not connected to.
	for _, cs := range c.servers {
		if cs != s {
			cs.Shutdown()
			c.restartServer(cs)
		}
	}
	c.waitOnAllCurrent()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	dsub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.DELETED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer dsub.Unsubscribe()
	nc.Flush()

	c.waitOnConsumerLeader("$G", "TEST-1", "DC")
	c.waitOnLeader()

	// Now check delete advisories as well.
	if err := js.DeleteConsumer("TEST-1", "DC"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, dsub, 1)
	checkSubsPending(t, sub, 1)
	checkSubsPending(t, usub, 0)
	drainSub(dsub)

	if err := js.DeleteStream("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, dsub, 2) // Stream and the consumer underneath.
	checkSubsPending(t, sub, 2)
}

func TestJetStreamClusterNoDuplicateOnNodeRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "ND", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("msg1"))
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.Ack()
	}

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")

	// Send second msg
	js.Publish("foo", []byte("msg2"))
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if string(msg.Data) != "msg2" {
		t.Fatalf("Unexpected message: %s", msg.Data)
	}
	msg.Ack()

	// Make sure we don't get a duplicate.
	msg, err = sub.NextMsg(250 * time.Millisecond)
	if err == nil {
		t.Fatalf("Should have gotten an error, got %s", msg.Data)
	}
}

func TestJetStreamClusterNoDupePeerSelection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NDP", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create 10 streams. Make sure none of them have a replica
	// that is the same as the leader.
	for i := 1; i <= 10; i++ {
		si, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", i),
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			t.Fatalf("Unexpected cluster state for stream info: %+v\n", si.Cluster)
		}
		// Make sure that the replicas are not same as the leader.
		for _, pi := range si.Cluster.Replicas {
			if pi.Name == si.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
		// Now do a consumer and check same thing.
		sub, err := js.SubscribeSync(si.Config.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error getting consumer info: %v", err)
		}
		for _, pi := range ci.Cluster.Replicas {
			if pi.Name == ci.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
	}
}

func TestJetStreamClusterRemovePeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)

	// Grab stream info.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	peers := []string{si.Cluster.Leader}
	for _, p := range si.Cluster.Replicas {
		peers = append(peers, p.Name)
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	toRemove := peers[0]

	// First test bad peer.
	req := &JSApiStreamRemovePeerRequest{Peer: "NOT VALID"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var rpResp JSApiStreamRemovePeerResponse
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error == nil || !strings.Contains(rpResp.Error.Description, "peer not a member") {
		t.Fatalf("Expected error for bad peer, got %+v", rpResp.Error)
	}
	rpResp.Error = nil

	req = &JSApiStreamRemovePeerRequest{Peer: toRemove}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", rpResp.Error)
	}

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if si.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range si.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})

	// Now check consumer info as well.
	checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if ci.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range ci.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})
}

func TestJetStreamClusterStreamLeaderStepDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	oldLeader := c.streamLeader("$G", "TEST").Name()

	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	// Grab shorter timeout jetstream context.
	js, err = nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if si.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	oldLeader = c.consumerLeader("$G", "TEST", "cat").Name()

	// Need to call this by hand for now.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "cat"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var cdResp JSApiConsumerLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat")
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if ci.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterRemoveServer(t *testing.T) {
	skip(t)

	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cname := ci.Name

	sl := c.streamLeader("$G", "TEST")
	c.removeJetStream(sl)

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	// Faster timeout since we loop below checking for condition.
	js, err = nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check the stream info is eventually correct.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	c.waitOnConsumerLeader("$G", "TEST", cname)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", cname)
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterPurgeReplayAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "P3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish("TEST", []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch(10)
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	sendBatch(10)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 10 {
		t.Fatalf("Expected 10 msgs after restart, got %d", si.State.Msgs)
	}
}

func TestJetStreamClusterStreamGetMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	mreq := &JSApiMsgGetRequest{Seq: 1}
	req, err := json.Marshal(mreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}

	var resp JSApiMsgGetResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	if err != nil {
		t.Fatalf("Could not parse stream message: %v", err)
	}
	if resp.Message == nil || resp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", resp.Error)
	}
}

func TestJetStreamClusterSuperClusterMetaPlacement(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// We want to influence where the meta leader will place itself when we ask the
	// current leader to stepdown.
	ml := sc.leader()
	cn := ml.ClusterName()
	var pcn string
	for _, c := range sc.clusters {
		if c.name != cn {
			pcn = c.name
			break
		}
	}

	// Client based API
	s := sc.randomServer()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	stepdown := func(cn string) *JSApiLeaderStepDownResponse {
		req := &JSApiLeaderStepdownRequest{Placement: &Placement{Cluster: cn}}
		jreq, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resp, err := nc.Request(JSApiLeaderStepDown, jreq, time.Second)
		if err != nil {
			t.Fatalf("Error on stepdown request: %v", err)
		}
		var sdr JSApiLeaderStepDownResponse
		if err := json.Unmarshal(resp.Data, &sdr); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return &sdr
	}

	// Make sure we get correct errors for tags and bad or unavailable cluster placement.
	sdr := stepdown("C22")
	if sdr.Error == nil || !strings.Contains(sdr.Error.Description, "no suitable peers") {
		t.Fatalf("Got incorrect error result: %+v", sdr.Error)
	}
	// Should work.
	sdr = stepdown(pcn)
	if sdr.Error != nil {
		t.Fatalf("Got an error on stepdown: %+v", sdr.Error)
	}

	sc.waitOnLeader()
	ml = sc.leader()
	cn = ml.ClusterName()

	if cn != pcn {
		t.Fatalf("Expected new metaleader to be in cluster %q, got %q", pcn, cn)
	}
}

func TestJetStreamClusterSuperClusterBasics(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Check request origin placement.
	if si.Cluster.Name != s.ClusterName() {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Check consumers.
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Delivered.Consumer != uint64(toSend) || ci.NumAckPending != toSend {
		t.Fatalf("ConsumerInfo is not correct: %+v", ci)
	}

	// Now check we can place a stream.
	pcn := "C3"
	scResp, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Placement: &nats.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if scResp.Cluster.Name != pcn {
		t.Fatalf("Expected the stream to be placed in %q, got %q", pcn, scResp.Cluster.Name)
	}
}

// Test that consumer interest across gateways and superclusters is properly identitifed in a remote cluster.
func TestJetStreamClusterSuperClusterCrossClusterConsumerInterest(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Since we need all of the peers accounted for to add the stream wait for all to be present.
	sc.waitOnPeerCount(9)

	// Client based API - Connect to Cluster C1. Stream and consumer will live in C2.
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	pcn := "C2"
	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3, Placement: &nats.Placement{Cluster: pcn}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull based first.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send a message.
	if _, err = js.Publish("foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	fetchMsgs(t, sub, 1, 5*time.Second)

	// Now check push based delivery.
	sub, err = js.SubscribeSync("foo", nats.Durable("rip"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)

	// Send another message.
	if _, err = js.Publish("foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkSubsPending(t, sub, 2)
}

func TestJetStreamNextReqFromMsg(t *testing.T) {
	bef := time.Now()
	expires, _, _, err := nextReqFromMsg([]byte(`{"expires":5000000000}`)) // nanoseconds
	require_NoError(t, err)
	now := time.Now()
	if expires.Before(bef.Add(5*time.Second)) || expires.After(now.Add(5*time.Second)) {
		t.Fatal("Expires out of expected range")
	}
}

func TestJetStreamClusterSuperClusterPeerReassign(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	pcn := "C2"

	// Create a stream in C2 that sources TEST
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Placement: &nats.Placement{Cluster: pcn},
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Check request origin placement.
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Now remove a peer that is assigned to the stream.
	rc := sc.clusterForName(pcn)
	rs := rc.randomNonStreamLeader("$G", "TEST")
	rc.removeJetStream(rs)

	// Check the stream info is eventually correct.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
			if !strings.HasPrefix(peer.Name, pcn) {
				t.Fatalf("Stream peer reassigned to wrong cluster: %q", peer.Name)
			}
		}
		return nil
	})
}

func TestJetStreamClusterStreamPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	numConnections := 4
	var conns []nats.JetStream
	for i := 0; i < numConnections; i++ {
		s := c.randomServer()
		_, js := jsClientConnect(t, s)
		conns = append(conns, js)
	}

	toSend := 100000
	numProducers := 8

	payload := []byte("Hello JSC")

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < numProducers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			js := conns[rand.Intn(numConnections)]
			<-startCh
			for i := 0; i < int(toSend)/numProducers; i++ {
				if _, err = js.Publish("foo", payload); err != nil {
					t.Errorf("Unexpected publish error: %v", err)
				}
			}
		}()
	}

	// Wait for Go routines.
	time.Sleep(250 * time.Millisecond)

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs with %d producers and R=3!\n", tt, toSend, numProducers)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamClusterConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 500000
	msg := make([]byte, 64)
	rand.Read(msg)

	for i := 0; i < toSend; i++ {
		nc.Publish("TEST", msg)
	}
	nc.Flush()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected to have %d messages, got %d", toSend, si.State.Msgs)
		}
		return nil
	})

	received := int32(0)
	deliverTo := "r"
	done := make(chan bool)
	total := int32(toSend)
	var start time.Time

	nc.Subscribe(deliverTo, func(m *nats.Msg) {
		if r := atomic.AddInt32(&received, 1); r >= total {
			done <- true
		} else if r == 1 {
			start = time.Now()
		}
	})

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: deliverTo, Durable: "gf"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out?")
	}
	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs\n", tt, toSend)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

// This test creates a queue consumer for the delivery subject,
// and make sure it connects to the server that is not the leader
// of the stream. A bug was not stripping the $JS.ACK reply subject
// correctly, which means that ack sent on the reply subject was
// droped by the routed
func TestJetStreamClusterQueueSubConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	inbox := nats.NewInbox()
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "ivan",
		DeliverSubject: inbox,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a client that does NOT connect to the stream leader.
	// Start with url from first server in the cluster.
	u := c.servers[0].ClientURL()
	// If leader is "S-1", then use S-2 to connect to, which is at servers[1].
	if ci.Cluster.Leader == "S-1" {
		u = c.servers[1].ClientURL()
	}
	qsubnc, err := nats.Connect(u)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer qsubnc.Close()

	ch := make(chan struct{}, 2)
	if _, err := qsubnc.QueueSubscribe(inbox, "queue", func(m *nats.Msg) {
		m.Respond(nil)
		ch <- struct{}{}
	}); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}

	// Use the other connection to publish a message
	if _, err := js.Publish("foo.bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	// Wait that we receive the message first.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Did not receive message")
	}

	// Message should be ack'ed and not redelivered.
	select {
	case <-ch:
		t.Fatal("Message redelivered!!!")
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestJetStreamClusterLeaderStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	c.waitOnLeader()
	cl := c.leader()
	// Now ask the system account to have the leader stepdown.
	s := c.randomNonLeader()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	resp, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
	if err != nil {
		t.Fatalf("Error on stepdown request: %v", err)
	}
	var sdr JSApiLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdr); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdr.Error != nil || !sdr.Success {
		t.Fatalf("Unexpected error for leader stepdown: %+v", sdr.Error)
	}

	c.waitOnLeader()
	if cl == c.leader() {
		t.Fatalf("Expected a new metaleader, got same")
	}
}

func TestJetStreamClusterMirrorAndSourcesClusterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSR", 5)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Mirror:   &nats.StreamSource{Name: "TEST"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func() {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			tsi, err := js.StreamInfo("TEST")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msi, err := js.StreamInfo("M")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if tsi.State.Msgs != msi.State.Msgs {
				return fmt.Errorf("Total messages not the same: TEST %d vs M %d", tsi.State.Msgs, msi.State.Msgs)
			}
			return nil
		})
	}

	// Send 100 msgs.
	sendBatch("foo", 100)
	checkSync()

	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch("bar", 100)
	checkSync()
}

func TestJetStreamClusterMirrorAndSourcesFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "MS5", 5)
	defer c.shutdown()

	// Client for API requests.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expectFail := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")
	expectFail("M", "baz")
	expectFail("M", "baz.1.2")
	expectFail("M", "apple")

	// Make sure wider scoped subjects work as well.
	createConsumer("M", "*")
	createConsumer("M", ">")

	// Now do some sources.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O1", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O2", Subjects: []string{"bar.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create downstream now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "O1"}, {Name: "O2"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")
	expectFail("S", "baz")
	expectFail("S", "baz.1")
	expectFail("S", "apple")

	// Now cross account stuff.
	nc2, js2 := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	if _, err := js2.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cfg := StreamConfig{
		Name:    "SCA",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "ORIGIN",
			External: &ExternalStream{
				ApiPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
			},
		}},
	}
	req, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Externals skip the checks for now.
	createConsumer("SCA", "foo.1")
	createConsumer("SCA", "bar.1")
	createConsumer("SCA", "baz")
}

func TestJetStreamCrossAccountMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()

	// Have to do this direct until we get Go client support.
	// Need to match jsClusterMirrorSourceImportsTempl imports.
	_, err := js2.AddStream(&nats.StreamConfig{
		Name: "MY_MIRROR_TEST",
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_MIRROR_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info: %s", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

	// Now do sources as well.
	_, err = js2.AddStream(&nats.StreamConfig{
		Name: "MY_SOURCE_TEST",
		Sources: []*nats.StreamSource{
			{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.API",
					DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_SOURCE_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info")
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

}

func TestJetStreamFailMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2, Subjects: []string{"test.>"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nc2, _ := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	testPrefix := func(testName string, id ErrorIdentifier, cfg StreamConfig) {
		t.Run(testName, func(t *testing.T) {
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err := nc2.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var scResp JSApiStreamCreateResponse
			if err := json.Unmarshal(resp.Data, &scResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if scResp.Error == nil {
				t.Fatalf("Did expect an error but got none")
			} else if !IsNatsErr(scResp.Error, id) {
				t.Fatalf("Expected different error: %s", scResp.Error.Description)
			}
		})
	}

	testPrefix("mirror-bad-deliverprefix", JSStreamExternalDelPrefixOverlapsErrF, StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix: "RI.JS.API",
				// this will result in test.test.> which test.> would match
				DeliverPrefix: "test",
			},
		},
	})
	testPrefix("mirror-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
	})
	testPrefix("source-bad-deliverprefix", JSStreamExternalDelPrefixOverlapsErrF, StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "RI.JS.API",
				DeliverPrefix: "test",
			},
		},
		},
	})
	testPrefix("source-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
		},
	})
}

func TestJetStreamClusterJSAPIImport(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterImportsTempl, "C1", 3)
	defer c.shutdown()

	// Client based API - This will connect to the non-js account which imports JS.
	// Connect below does an AccountInfo call.
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Note if this was ephemeral we would need to setup export/import for that subject.
	sub, err := js.SubscribeSync("TEST", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we can look up both.
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := sub.ConsumerInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Names list..
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now send to stream.
	if _, err := js.Publish("TEST", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err = js.PullSubscribe("TEST", "tr")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msgs := fetchMsgs(t, sub, 1, 5*time.Second)

	m := msgs[0]
	if m.Subject != "TEST" {
		t.Fatalf("Expected subject of %q, got %q", "TEST", m.Subject)
	}
	if m.Header != nil {
		t.Fatalf("Expected no header on the message, got: %v", m.Header)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 || meta.NumPending != 0 {
		t.Fatalf("Bad meta: %+v", meta)
	}

	js.Publish("TEST", []byte("Second"))
	js.Publish("TEST", []byte("Third"))

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "tr")
		if err != nil {
			return fmt.Errorf("Error getting consumer info: %v", err)
		}
		if ci.NumPending != 2 {
			return fmt.Errorf("NumPending still not 1: %v", ci.NumPending)
		}
		return nil
	})

	// Ack across accounts.
	m, err = nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.tr", []byte("+NXT"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, err = m.Metadata()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if meta.Sequence.Consumer != 2 || meta.Sequence.Stream != 2 || meta.NumDelivered != 1 || meta.NumPending != 1 {
		t.Fatalf("Bad meta: %+v", meta)
	}

	// AckNext
	_, err = nc.Request(m.Reply, []byte("+NXT"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterSuperClusterInterestOnlyMode(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}
		accounts {
			one {
				jetstream: enabled
				users [{user: one, password: password}]
			}
			two {
				%s
				users [{user: two, password: password}]
			}
		}
		cluster {
			listen: 127.0.0.1:%d
			name: %s
			routes = ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: %s
			listen: 127.0.0.1:%d
			gateways = [{name: %s, urls: ["nats://127.0.0.1:%d"]}]
		}
	`
	storeDir1 := createDir(t, JetStreamStoreDir)
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "", 33222, "A", 33222, "A", 11222, "B", 11223)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	storeDir2 := createDir(t, JetStreamStoreDir)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "", 33223, "B", 33223, "B", 11223, "A", 11222)))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	nc1 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o1.Port))
	defer nc1.Close()
	nc1.Publish("foo", []byte("some message"))
	nc1.Flush()

	nc2 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o2.Port))
	defer nc2.Close()
	nc2.Publish("bar", []byte("some message"))
	nc2.Flush()

	checkMode := func(accName string, expectedMode GatewayInterestMode) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			servers := []*Server{s1, s2}
			for _, s := range servers {
				var gws []*client
				s.getInboundGatewayConnections(&gws)
				for _, gw := range gws {
					var mode GatewayInterestMode
					gw.mu.Lock()
					ie := gw.gw.insim[accName]
					if ie != nil {
						mode = ie.mode
					}
					gw.mu.Unlock()
					if ie == nil {
						return fmt.Errorf("Account %q not in map", accName)
					}
					if mode != expectedMode {
						return fmt.Errorf("Expected account %q mode to be %v, got: %v", accName, expectedMode, mode)
					}
				}
			}
			return nil
		})
	}

	checkMode("one", InterestOnly)
	checkMode("two", Optimistic)

	// Now change account "two" to enable JS
	changeCurrentConfigContentWithNewContent(t, conf1, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "jetstream: enabled", 33222, "A", 33222, "A", 11222, "B", 11223)))
	changeCurrentConfigContentWithNewContent(t, conf2, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "jetstream: enabled", 33223, "B", 33223, "B", 11223, "A", 11222)))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on s1 reload: %v", err)
	}
	if err := s2.Reload(); err != nil {
		t.Fatalf("Error on s2 reload: %v", err)
	}

	checkMode("one", InterestOnly)
	checkMode("two", InterestOnly)
}

func TestJetStreamClusterSuperClusterEphemeralCleanup(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Create a stream in cluster 0
	s := sc.clusters[0].randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, test := range []struct {
		name            string
		sourceInCluster int
		streamName      string
		sourceName      string
	}{
		{"local", 0, "TEST1", "S1"},
		{"remote", 1, "TEST2", "S2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := js.AddStream(&nats.StreamConfig{Name: test.streamName, Replicas: 3}); err != nil {
				t.Fatalf("Error adding %q stream: %v", test.streamName, err)
			}
			if _, err := js.Publish(test.streamName, []byte("hello")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}

			// Now create a source for that stream, either in same or remote cluster.
			s2 := sc.clusters[test.sourceInCluster].randomServer()
			nc2, js2 := jsClientConnect(t, s2)
			defer nc2.Close()

			if _, err := js2.AddStream(&nats.StreamConfig{
				Name:     test.sourceName,
				Storage:  nats.FileStorage,
				Sources:  []*nats.StreamSource{{Name: test.streamName}},
				Replicas: 1,
			}); err != nil {
				t.Fatalf("Error adding source stream: %v", err)
			}

			// Check that TEST(n) has 1 consumer and that S(n) is created and has 1 message.
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				si, err := js2.StreamInfo(test.sourceName)
				if err != nil {
					return fmt.Errorf("Could not get stream info: %v", err)
				}
				if si.State.Msgs != 1 {
					return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
				}
				return nil
			})

			// Get the consumer because we will want to artificially reduce
			// the delete threshold.
			leader := sc.clusters[0].streamLeader("$G", test.streamName)
			mset, err := leader.GlobalAccount().lookupStream(test.streamName)
			if err != nil {
				t.Fatalf("Expected to find a stream for %q, got %v", test.streamName, err)
			}
			cons := mset.getConsumers()[0]
			cons.mu.Lock()
			cons.dthresh = 1250 * time.Millisecond
			active := cons.active
			dtimerSet := cons.dtmr != nil
			deliver := cons.cfg.DeliverSubject
			cons.mu.Unlock()

			if !active || dtimerSet {
				t.Fatalf("Invalid values for active=%v dtimerSet=%v", active, dtimerSet)
			}
			// To add to the mix, let's create a local interest on the delivery subject
			// and stop it. This is to ensure that this does not stop timers that should
			// still be running and monitor the GW interest.
			sub := natsSubSync(t, nc, deliver)
			natsFlush(t, nc)
			natsUnsub(t, sub)
			natsFlush(t, nc)

			// Now remove the "S(n)" stream...
			if err := js2.DeleteStream(test.sourceName); err != nil {
				t.Fatalf("Error deleting stream: %v", err)
			}

			// Now check that the stream S(n) is really removed and that
			// the consumer is gone for stream TEST(n).
			checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
				// First, make sure that stream S(n) has disappeared.
				if _, err := js2.StreamInfo(test.sourceName); err == nil {
					return fmt.Errorf("Stream %q should no longer exist", test.sourceName)
				}
				if ndc := mset.numDirectConsumers(); ndc != 0 {
					return fmt.Errorf("Expected %q stream to have 0 consumers, got %v", test.streamName, ndc)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSuperClusterDirectConsumersBrokenGateways(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 1, 2)
	defer sc.shutdown()

	// Client based API
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// This will be in C1.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a stream in C2 that sources TEST
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "S",
		Placement: &nats.Placement{Cluster: "C2"},
		Sources:   []*nats.StreamSource{{Name: "TEST"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait for direct consumer to get registered and detect interest across GW.
	time.Sleep(time.Second)

	// Send 100 msgs over 100ms in separate Go routine.
	msg, toSend, done := []byte("Hello"), 100, make(chan bool)
	go func() {
		// Send in 10 messages.
		for i := 0; i < toSend; i++ {
			if _, err = js.Publish("TEST", msg); err != nil {
				t.Errorf("Unexpected publish error: %v", err)
			}
			time.Sleep(500 * time.Microsecond)
		}
		done <- true
	}()

	breakGW := func() {
		s.gateway.Lock()
		gw := s.gateway.out["C2"]
		s.gateway.Unlock()
		if gw != nil {
			gw.closeConnection(ClientClosed)
		}
	}

	// Wait til about half way through.
	time.Sleep(20 * time.Millisecond)
	// Now break GW connection.
	breakGW()

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not complete sending first batch of messages")
	}

	// Make sure we can deal with data loss at the end.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected to have %d messages, got %d", 100, si.State.Msgs)
		}
		return nil
	})

	// Now send 100 more. Will aos break here in the middle.
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
		if i == 50 {
			breakGW()
		}
	}

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 200 {
		t.Fatalf("Expected to have %d messages, got %d", 200, si.State.Msgs)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 200 {
			return fmt.Errorf("Expected to have %d messages, got %d", 200, si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterMultiRestartBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10000 messages.
	msg, toSend := make([]byte, 4*1024), 10000
	rand.Read(msg)

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected to have %d messages, got %d", toSend, si.State.Msgs)
		}
		return nil
	})

	// For this bug, we will stop and remove the complete state from one server.
	s := c.randomServer()
	opts := s.getOpts()
	s.Shutdown()
	removeDir(t, opts.StoreDir)

	// Then restart it.
	c.restartAll()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader("$G", "TEST")

	s = c.serverByName(s.Name())
	opts = s.getOpts()

	c.waitOnStreamCurrent(s, "$G", "TEST")

	snaps, err := ioutil.ReadDir(path.Join(opts.StoreDir, JetStreamStoreDir, "$SYS", "_js_", "_meta_", "snapshots"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(snaps) == 0 {
		t.Fatalf("Expected a meta snapshot for the restarted server")
	}

	// Now restart them all..
	c.stopAll()
	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	// Create new client.
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Make sure the replicas are current.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, _ := js.StreamInfo("TEST")
		if si == nil || si.Cluster == nil {
			t.Fatalf("Did not get stream info")
		}
		for _, pi := range si.Cluster.Replicas {
			if !pi.Current {
				return fmt.Errorf("Peer not current: %+v", pi)
			}
		}
		return nil
	})
}

func TestJetStreamClusterServerLimits(t *testing.T) {
	// 2MB memory, 8MB disk
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	msg, toSend := make([]byte, 4*1024), 5000
	rand.Read(msg)

	// Memory first.
	max_mem := uint64(2*1024*1024) + uint64(len(msg))

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TM",
		Replicas: 3,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TM", msg); err != nil {
			break
		}
	}
	if err == nil || !strings.HasPrefix(err.Error(), "nats: insufficient resources") {
		t.Fatalf("Expected a ErrJetStreamResourcesExceeded error, got %v", err)
	}

	si, err := js.StreamInfo("TM")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Bytes > max_mem {
		t.Fatalf("Expected bytes of %v to not be greater then %v",
			friendlyBytes(int64(si.State.Bytes)),
			friendlyBytes(int64(max_mem)),
		)
	}

	c.waitOnLeader()

	// Now disk.
	max_disk := uint64(8*1024*1024) + uint64(len(msg))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TF",
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TF", msg); err != nil {
			break
		}
	}
	if err == nil || !strings.HasPrefix(err.Error(), "nats: insufficient resources") {
		t.Fatalf("Expected a ErrJetStreamResourcesExceeded error, got %v", err)
	}

	si, err = js.StreamInfo("TF")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Bytes > max_disk {
		t.Fatalf("Expected bytes of %v to not be greater then %v",
			friendlyBytes(int64(si.State.Bytes)),
			friendlyBytes(int64(max_disk)),
		)
	}
}

func TestJetStreamClusterAccountLoadFailure(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	// Remove the "ONE" account from non-leader
	s := c.randomNonLeader()
	s.mu.Lock()
	s.accounts.Delete("ONE")
	s.mu.Unlock()

	_, err := js.AddStream(&nats.StreamConfig{Name: "F", Replicas: 3})
	if err == nil || !strings.Contains(err.Error(), "account not found") {
		t.Fatalf("Expected an 'account not found' error but got %v", err)
	}
}

func TestJetStreamClusterAckPendingWithExpired(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		MaxAge:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := make([]byte, 256), 100
	rand.Read(msg)

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumAckPending != toSend {
		t.Fatalf("Expected %d to be pending, got %d", toSend, ci.NumAckPending)
	}

	// Wait for messages to expire.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Once expired these messages can not be redelivered so should not be considered ack pending at this point.
	// Now ack..
	ci, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumAckPending != 0 {
		t.Fatalf("Expected nothing to be ack pending, got %d", ci.NumAckPending)
	}
}

func TestJetStreamClusterAckPendingWithMaxRedelivered(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := []byte("HELLO"), 100

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("foo",
		nats.MaxDeliver(2),
		nats.Durable("dlc"),
		nats.AckWait(10*time.Millisecond),
		nats.MaxAckPending(50),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend*2)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := sub.ConsumerInfo()
		if err != nil {
			return err
		}
		if ci.NumAckPending != 0 {
			return fmt.Errorf("Expected nothing to be ack pending, got %d", ci.NumAckPending)
		}
		return nil
	})
}

func TestJetStreamClusterMixedMode(t *testing.T) {
	c := createMixedModeCluster(t, jsClusterLimitsTempl, "MM5", _EMPTY_, 3, 2, false)
	defer c.shutdown()

	// Client based API - Non-JS server.
	nc, js := jsClientConnect(t, c.serverByName("S-5"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ml := c.leader()
	if ml == nil {
		t.Fatalf("No metaleader")
	}

	// Make sure we are tracking only the JS peers.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		peers := ml.JetStreamClusterPeers()
		if len(peers) == 3 {
			return nil
		}
		return fmt.Errorf("Not correct number of peers, expected %d, got %d", 3, len(peers))
	})

	// Grab the underlying raft structure and make sure the system adjusts its cluster set size.
	meta := ml.getJetStream().getMetaGroup().(*raft)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ps := meta.currentPeerState()
		if len(ps.knownPeers) != 3 {
			return fmt.Errorf("Expected known peers to be 3, but got %+v", ps.knownPeers)
		}
		if ps.clusterSize < 3 {
			return fmt.Errorf("Expected cluster size to be 3, but got %+v", ps)
		}
		return nil
	})
}

func TestJetStreamClusterLeafnodeSpokes(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, 3, 22020, false)
	defer c.shutdown()

	lnc1 := c.createLeafNodesWithStartPort("R1", 3, 22110)
	defer lnc1.shutdown()

	lnc2 := c.createLeafNodesWithStartPort("R2", 3, 22120)
	defer lnc2.shutdown()

	lnc3 := c.createLeafNodesWithStartPort("R3", 3, 22130)
	defer lnc3.shutdown()

	// Wait on all peers.
	c.waitOnPeerCount(12)

	// Make sure shrinking works.
	lnc3.shutdown()
	c.waitOnPeerCount(9)

	lnc3 = c.createLeafNodesWithStartPort("LNC3", 3, 22130)
	defer lnc3.shutdown()

	c.waitOnPeerCount(12)
}

func TestJetStreamClusterSuperClusterAndLeafNodesWithSharedSystemAccount(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	lnc := sc.createLeafNodes("LNC", 2)
	defer lnc.shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	if ml := lnc.leader(); ml != nil {
		t.Fatalf("Detected a meta-leader in the leafnode cluster: %s", ml)
	}

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(8)

	// Check here that we auto detect sharing system account as well and auto place the correct
	// deny imports and exports.
	ls := lnc.randomServer()
	if ls == nil {
		t.Fatalf("Expected a leafnode server, got none")
	}
	gacc := ls.globalAccount().GetName()

	ls.mu.Lock()
	var hasDE, hasDI bool
	for _, ln := range ls.leafs {
		ln.mu.Lock()
		if ln.leaf.remote.RemoteLeafOpts.LocalAccount == gacc {
			// Make sure we have the $JS.API denied in both.
			for _, dsubj := range ln.leaf.remote.RemoteLeafOpts.DenyExports {
				if dsubj == jsAllAPI {
					hasDE = true
					break
				}
			}
			for _, dsubj := range ln.leaf.remote.RemoteLeafOpts.DenyImports {
				if dsubj == jsAllAPI {
					hasDI = true
					break
				}
			}
		}
		ln.mu.Unlock()
	}
	ls.mu.Unlock()

	if !hasDE {
		t.Fatalf("No deny export on system account")
	}
	if !hasDI {
		t.Fatalf("No deny import on system account")
	}

	// Make a stream by connecting to the leafnode cluster. Make sure placement is correct.
	// Client based API
	nc, js := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNC" {
		t.Fatalf("Expected default placement to be %q, got %q", "LNC", si.Cluster.Name)
	}

	// Now make sure placement also works if we want to place in a cluster in the supercluster.
	pcn := "C2"
	si, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"baz"},
		Replicas:  2,
		Placement: &nats.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected default placement to be %q, got %q", pcn, si.Cluster.Name)
	}
}

func TestJetStreamClusterSuperClusterAndSingleLeafNodeWithSharedSystemAccount(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	ln := sc.createSingleLeafNode()
	defer ln.Shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(7)

	// Now make sure we can place a stream in the leaf node.
	// First connect to the leafnode server itself.
	nc, js := jsClientConnect(t, ln)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
	// Now check we can place on here as well but connect to the hub.
	nc, js = jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	si, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"bar"},
		Placement: &nats.Placement{Cluster: "LNS"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
}

// Multiple JS domains.
func TestJetStreamClusterSingleLeafNodeWithoutSharedSystemAccount(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterAccountsTempl, "HUB", _EMPTY_, 3, 14333, true)
	defer c.shutdown()

	ln := c.createSingleLeafNodeNoSystemAccount()
	defer ln.Shutdown()

	// The setup here has a single leafnode server with two accounts. One has JS, the other does not.
	// We want to to test the following.
	// 1. For the account without JS, we simply will pass through to the HUB. Meaning since our local account
	//    does not have it, we simply inherit the hub's by default.
	// 2. For the JS enabled account, we are isolated and use our local one only.

	// Check behavior of the account without JS.
	// Normally this should fail since our local account is not enabled. However, since we are bridging
	// via the leafnode we expect this to work here.
	nc, js := jsClientConnect(t, ln, nats.UserInfo("n", "p"))
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "HUB" {
		t.Fatalf("Expected stream to be placed in %q", "HUB")
	}
	// Do some other API calls.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	seen := 0
	for name := range js.StreamNames() {
		seen++
		if name != "TEST" {
			t.Fatalf("Expected only %q but got %q", "TEST", name)
		}
	}
	if seen != 1 {
		t.Fatalf("Expected only 1 stream, got %d", seen)
	}
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", "C1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"bar"}, Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now check the enabled account.
	// Check the enabled account only talks to its local JS domain by default.
	nc, js = jsClientConnect(t, ln, nats.UserInfo("y", "p"))
	defer nc.Close()

	sub, err := nc.SubscribeSync(JSAdvisoryStreamCreatedPre + ".>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster != nil {
		t.Fatalf("Expected no cluster designation for stream since created on single LN server")
	}

	// Wait for a bit and make sure we only get one of these.
	// The HUB domain should be cut off by default.
	time.Sleep(250 * time.Millisecond)
	checkSubsPending(t, sub, 1)
	// Drain.
	for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
	}

	// Now try to talk to the HUB JS domain through a new context that uses a different mapped subject.
	// This is similar to how we let users cross JS domains between accounts as well.
	js, err = nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	// This should fail here with jetstream not enabled.
	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now add in a mapping to the connected account in the HUB.
	// This aligns with the APIPrefix context above and works across leafnodes.
	// TODO(dlc) - Should we have a mapping section for leafnode solicit?
	c.addSubjectMapping("ONE", "$JS.HUB.API.>", "$JS.API.>")

	// Now it should work.
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can add a stream, etc.
	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST22",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "HUB" {
		t.Fatalf("Expected stream to be placed in %q", "HUB")
	}

	jsLocal, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Create a mirror on the local leafnode for stream TEST22.
	_, err = jsLocal.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST22",
			External: &nats.ExternalStream{APIPrefix: "$JS.HUB.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to the HUB's TEST22 stream.
	if _, err := js.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Make sure the message arrives in our mirror.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// Now do the reverse and create a sourced stream in the HUB from our local stream on leafnode.
	// Inside the HUB we need to be able to find our local leafnode JetStream assets, so we need
	// a mapping in the LN server to allow this to work. Normally this will just be in server config.
	acc, err := ln.LookupAccount("JSY")
	if err != nil {
		c.t.Fatalf("Unexpected error on %v: %v", ln, err)
	}
	if err := acc.AddMapping("$JS.LN.API.>", "$JS.API.>"); err != nil {
		c.t.Fatalf("Error adding mapping: %v", err)
	}

	// js is the HUB JetStream context here.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name:     "M",
			External: &nats.ExternalStream{APIPrefix: "$JS.LN.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})
}

// JetStream Domains
func TestJetStreamClusterDomains(t *testing.T) {
	// This adds in domain config option to template.
	// jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: CORE, store_dir: "%s"}
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	// This leafnode is a single server with no domain but sharing the system account.
	// This extends the CORE domain through this leafnode.
	ln := c.createLeafNodeWithTemplate("LN-SYS", jsClusterTemplWithSingleLeafNode)
	defer ln.Shutdown()

	// This shows we have extended this system.
	c.waitOnPeerCount(4)
	if ml := c.leader(); ml == ln {
		t.Fatalf("Detected a meta-leader in the leafnode: %s", ml)
	}

	// Now create another LN but with a domain defined.
	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	spoke := c.createLeafNodeWithTemplate("LN-SPOKE", tmpl)
	defer spoke.Shutdown()

	// Should be the same, should not extend the CORE domain.
	c.waitOnPeerCount(4)

	// The domain signals to the system that we are our own JetStream domain and should not extend CORE.
	// We want to check to make sure we have all the deny properly setup.
	spoke.mu.Lock()
	// var hasDE, hasDI bool
	for _, ln := range spoke.leafs {
		ln.mu.Lock()
		remote := ln.leaf.remote
		ln.mu.Unlock()
		remote.RLock()
		if remote.RemoteLeafOpts.LocalAccount == "$SYS" {
			if len(remote.RemoteLeafOpts.DenyExports) != 3 {
				t.Fatalf("Expected to have deny exports, got %+v", remote.RemoteLeafOpts.DenyExports)
			}
			if len(remote.RemoteLeafOpts.DenyImports) != 3 {
				t.Fatalf("Expected to have deny imports, got %+v", remote.RemoteLeafOpts.DenyImports)
			}
		}
		remote.RUnlock()
	}
	spoke.mu.Unlock()

	// Now do some operations.
	// Check the enabled account only talks to its local JS domain by default.
	nc, js := jsClientConnect(t, spoke)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster != nil {
		t.Fatalf("Expected no cluster designation for stream since created on single LN server")
	}

	// Now try to talk to the CORE JS domain through a new context that uses a different mapped subject.
	jsCore, err := nc.JetStream(nats.APIPrefix("$JS.CORE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	if _, err := jsCore.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we can add a stream, etc.
	si, err = jsCore.AddStream(&nats.StreamConfig{
		Name:     "TEST22",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "CORE" {
		t.Fatalf("Expected stream to be placed in %q, got %q", "CORE", si.Cluster.Name)
	}

	jsLocal, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Create a mirror on our local leafnode for stream TEST22.
	_, err = jsLocal.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST22",
			External: &nats.ExternalStream{APIPrefix: "$JS.CORE.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to the CORE's TEST22 stream.
	if _, err := jsCore.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Make sure the message arrives in our mirror.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// jsCore is the CORE JetStream domain.
	// Create a sourced stream in the CORE that is sourced from our mirror stream in our leafnode.
	_, err = jsCore.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name:     "M",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsCore.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// Now connect directly to the CORE cluster and make sure we can operate there.
	nc, jsLocal = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create the js contexts again.
	jsSpoke, err := nc.JetStream(nats.APIPrefix("$JS.SPOKE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Publish a message to the CORE's TEST22 stream.
	if _, err := jsLocal.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Make sure the message arrives in our mirror.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsSpoke.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// We are connected to the CORE domain/system. Create a JetStream context referencing ourselves.
	jsCore, err = nc.JetStream(nats.APIPrefix("$JS.CORE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	si, err = jsCore.StreamInfo("S")
	if err != nil {
		t.Fatalf("Could not get stream info: %v", err)
	}
	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs, got state: %+v", si.State)
	}
}

// Issue #2205
func TestJetStreamClusterDomainsAndAPIResponses(t *testing.T) {
	// This adds in domain config option to template.
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	// Now create spoke LN cluster.
	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 5, 33913)
	defer lnc.shutdown()

	lnc.waitOnClusterReady()

	// Make the physical connection to the CORE.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create JS domain context and try to do same in LN cluster.
	// The issue referenced above details a bug where we can not receive a positive response.
	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	jsSpoke, err := nc.JetStream(nats.APIPrefix("$JS.SPOKE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	si, err := jsSpoke.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "SPOKE" {
		t.Fatalf("Expected %q as the cluster, got %q", "SPOKE", si.Cluster.Name)
	}
}

// Issue #2202
func TestJetStreamClusterDomainsAndSameNameSources(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 9323, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE-1, store_dir:", 1)
	spoke1 := c.createLeafNodeWithTemplate("LN-SPOKE-1", tmpl)
	defer spoke1.Shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE-2, store_dir:", 1)
	spoke2 := c.createLeafNodeWithTemplate("LN-SPOKE-2", tmpl)
	defer spoke2.Shutdown()

	subjFor := func(s *Server) string {
		switch s {
		case spoke1:
			return "foo"
		case spoke2:
			return "bar"
		}
		return "TEST"
	}

	// Create the same name stream in both spoke domains.
	for _, s := range []*Server{spoke1, spoke2} {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{subjFor(s)},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		nc.Close()
	}

	// Now connect to the hub and create a sourced stream from both leafnode streams.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{
			{
				Name:     "TEST",
				External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-1.API"},
			},
			{
				Name:     "TEST",
				External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-2.API"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to each spoke stream and we will check that our sourced stream gets both.
	for _, s := range []*Server{spoke1, spoke2} {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		js.Publish(subjFor(s), []byte("DOUBLE TROUBLE"))
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 1 {
			t.Fatalf("Expected 1 msg, got %d", si.State.Msgs)
		}
		nc.Close()
	}

	// Now make sure we have 2 msgs in our sourced stream.
	si, err := js.StreamInfo("S")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs, got %d", si.State.Msgs)
	}

	// Make sure we can see our external information.
	// This not in the Go client yet so manual for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "S"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ssi StreamInfo
	if err = json.Unmarshal(resp.Data, &ssi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(ssi.Sources) != 2 {
		t.Fatalf("Expected 2 source streams, got %d", len(ssi.Sources))
	}
	if ssi.Sources[0].External == nil {
		t.Fatalf("Expected a non-nil external designation")
	}
	pre := ssi.Sources[0].External.ApiPrefix
	if pre != "$JS.SPOKE-1.API" && pre != "$JS.SPOKE-2.API" {
		t.Fatalf("Expected external api of %q, got %q", "$JS.SPOKE-[1|2].API", ssi.Sources[0].External.ApiPrefix)
	}

	// Also create a mirror.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-1.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "M"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &ssi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ssi.Mirror == nil || ssi.Mirror.External == nil {
		t.Fatalf("Expected a non-nil external designation for our mirror")
	}
	if ssi.Mirror.External.ApiPrefix != "$JS.SPOKE-1.API" {
		t.Fatalf("Expected external api of %q, got %q", "$JS.SPOKE-1.API", ssi.Sources[0].External.ApiPrefix)
	}
}

// When a leafnode enables JS on an account that is not enabled on the remote cluster account this should
// still work. Early NGS beta testers etc.
func TestJetStreamClusterSingleLeafNodeEnablingJetStream(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11322, true)
	defer c.shutdown()

	ln := c.createSingleLeafNodeNoSystemAccountAndEnablesJetStream()
	defer ln.Shutdown()

	// Check that we have JS in the $G account on the leafnode.
	nc, js := jsClientConnect(t, ln)
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect our client to the "nojs" account in the cluster but make sure JS works since its enabled via the leafnode.
	s := c.randomServer()
	nc, js = jsClientConnect(t, s, nats.UserInfo("nojs", "p"))
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterLeafNodesWithoutJS(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11233, true)
	defer c.shutdown()

	testJS := func(s *Server, domain string, doDomainAPI bool) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		if doDomainAPI {
			var err error
			apiPre := fmt.Sprintf("$JS.%s.API", domain)
			if js, err = nc.JetStream(nats.APIPrefix(apiPre)); err != nil {
				t.Fatalf("Unexpected error getting JetStream context: %v", err)
			}
		}
		ai, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ai.Domain != domain {
			t.Fatalf("Expected domain of %q, got %q", domain, ai.Domain)
		}
	}

	ln := c.createLeafNodeWithTemplate("LN-SYS-S-NOJS", jsClusterTemplWithSingleLeafNodeNoJS)
	defer ln.Shutdown()

	// Check that we can access JS in the $G account on the cluster through the leafnode.
	testJS(ln, "HUB", false)
	ln.Shutdown()

	// Now create a leafnode cluster with No JS and make sure that works.
	lnc := c.createLeafNodesNoJS("LN-SYS-C-NOJS", 3)
	defer lnc.shutdown()

	testJS(lnc.randomServer(), "HUB", false)
	lnc.shutdown()

	// Do mixed mode but with a JS config block that specifies domain and just sets it to disabled.
	// This is the preferred method for mixed mode, always define JS server config block just disable
	// in those you do not want it running.
	// e.g. jetstream: {domain: "SPOKE", enabled: false}
	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lncm := c.createLeafNodesWithTemplateMixedMode(tmpl, "SPOKE", 3, 2, true)
	defer lncm.shutdown()

	// Now grab a non-JS server, last two are non-JS.
	sl := lncm.servers[0]
	testJS(sl, "SPOKE", false)

	// Test that mappings work as well and we can access the hub.
	testJS(sl, "HUB", true)
}

func TestJetStreamClusterLeafNodesWithSameDomainNames(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11233, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: HUB, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 3, 11311)
	defer lnc.shutdown()

	c.waitOnPeerCount(6)
}

// Issue reported with superclusters and leafnodes where first few get next requests for pull susbcribers
// have the wrong subject.
func TestJetStreamClusterSuperClusterGetNextRewrite(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 2, 2)
	defer sc.shutdown()

	// Will connect the leafnode to cluster C1. We will then connect the "client" to cluster C2 to cross gateways.
	ln := sc.clusterForName("C1").createSingleLeafNodeNoSystemAccountAndEnablesJetStream()
	defer ln.Shutdown()

	c2 := sc.clusterForName("C2")
	nc, js := jsClientConnect(t, c2.randomServer(), nats.UserInfo("nojs", "p"))
	defer nc.Close()

	// Create a stream and add messages.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("foo", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Pull messages and make sure subject rewrite works.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, m := range fetchMsgs(t, sub, 5, time.Second) {
		if m.Subject != "foo" {
			t.Fatalf("Expected %q as subject but got %q", "foo", m.Subject)
		}
	}
}

func TestJetStreamClusterSuperClusterPullConsumerAndHeaders(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnect(t, c1.randomServer())
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("ORIGIN", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnect(t, c2.randomServer())
	defer nc2.Close()

	_, err := js2.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "ORIGIN"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Wait for them to be in the sourced stream.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js2.StreamInfo("S"); si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs for %q, got %d", toSend, "S", si.State.Msgs)
		}
		return nil
	})

	// Now create a pull consumers for the sourced stream.
	_, err = js2.AddConsumer("S", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now we will connect and request the next message from each server in C1 cluster and check that headers remain in place.
	for _, s := range c1.servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		m, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.S.dlc", nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(m.Header) != 1 {
			t.Fatalf("Expected 1 header element, got %+v", m.Header)
		}
	}
}

func TestJetStreamClusterLeafDifferentAccounts(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterAccountsTempl, "HUB", _EMPTY_, 2, 33133, false)
	defer c.shutdown()

	ln := c.createLeafNodesWithStartPort("LN", 2, 22110)
	defer ln.shutdown()

	// Wait on all peers.
	c.waitOnPeerCount(4)

	nc, js := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	// Make sure we can properly indentify the right account when the leader received the request.
	// We need to map the client info header to the new account once received by the hub.
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterStreamInfoDeletedDetails(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2", 2)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("HELLO"), 10

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now remove some messages.
	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	deleteMsg(2)
	deleteMsg(4)
	deleteMsg(6)

	// Need to do these via direct server request for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var si StreamInfo
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.NumDeleted != 3 {
		t.Fatalf("Expected %d deleted, got %d", 3, si.State.NumDeleted)
	}
	if len(si.State.Deleted) != 0 {
		t.Fatalf("Expected not deleted details, but got %+v", si.State.Deleted)
	}

	// Now request deleted details.
	req := JSApiStreamInfoRequest{DeletedDetails: true}
	b, _ := json.Marshal(req)

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), b, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if si.State.NumDeleted != 3 {
		t.Fatalf("Expected %d deleted, got %d", 3, si.State.NumDeleted)
	}
	if len(si.State.Deleted) != 3 {
		t.Fatalf("Expected deleted details, but got %+v", si.State.Deleted)
	}
}

func TestJetStreamClusterMirrorAndSourceExpiration(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSE", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Origin
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	bi := 1
	sendBatch := func(n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			msg := fmt.Sprintf("ID: %d", bi)
			bi++
			if _, err := js.PublishAsync("TEST", []byte(msg)); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkStream := func(stream string, num uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != num {
				return fmt.Errorf("Expected %d msgs, got %d", num, si.State.Msgs)
			}
			return nil
		})
	}

	checkSource := func(num uint64) { t.Helper(); checkStream("S", num) }
	checkMirror := func(num uint64) { t.Helper(); checkStream("M", num) }
	checkTest := func(num uint64) { t.Helper(); checkStream("TEST", num) }

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Mirror:   &nats.StreamSource{Name: "TEST"},
		Replicas: 2,
		MaxAge:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want this to not be same as TEST leader for this test.
	sl := c.streamLeader("$G", "TEST")
	for ss := sl; ss == sl; {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "S",
			Sources:  []*nats.StreamSource{{Name: "TEST"}},
			Replicas: 2,
			MaxAge:   500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ss = c.streamLeader("$G", "S"); ss == sl {
			// Delete and retry.
			js.DeleteStream("S")
		}
	}

	sendBatch(100)
	checkTest(100)
	checkMirror(100)
	checkSource(100)

	// Make sure they expire.
	checkMirror(0)
	checkSource(0)

	// Now stop the server housing the leader of the source stream.
	sl.Shutdown()
	c.restartServer(sl)
	checkClusterFormed(t, c.servers...)
	c.waitOnStreamLeader("$G", "S")
	c.waitOnStreamLeader("$G", "M")

	// Make sure can process correctly after we have expired all of the messages.
	sendBatch(100)
	// Need to check both in parallel.
	scheck, mcheck := uint64(0), uint64(0)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if scheck != 100 {
			if si, _ := js.StreamInfo("S"); si != nil {
				scheck = si.State.Msgs
			}
		}
		if mcheck != 100 {
			if si, _ := js.StreamInfo("M"); si != nil {
				mcheck = si.State.Msgs
			}
		}
		if scheck == 100 && mcheck == 100 {
			return nil
		}
		return fmt.Errorf("Both not at 100 yet, S=%d, M=%d", scheck, mcheck)
	})

	checkTest(200)
}

func TestJetStreamClusterMirrorAndSourceSubLeaks(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	startSubs := c.stableTotalSubs()

	var ss []*nats.StreamSource

	// Create 10 origin streams
	for i := 0; i < 10; i++ {
		sn := fmt.Sprintf("ORDERS-%d", i+1)
		ss = append(ss, &nats.StreamSource{Name: sn})
		if _, err := js.AddStream(&nats.StreamConfig{Name: sn}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Create mux'd stream that sources all of the origin streams.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "MUX",
		Replicas: 2,
		Sources:  ss,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a mirror of the mux stream.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "MIRROR",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "MUX"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Get stable subs count.
	afterSubs := c.stableTotalSubs()

	js.DeleteStream("MIRROR")
	js.DeleteStream("MUX")

	for _, si := range ss {
		js.DeleteStream(si.Name)
	}

	// Some subs take longer to settle out so we give ourselves a small buffer.
	// There will be 1 sub for client on each server (such as _INBOX.IvVJ2DOXUotn4RUSZZCFvp.*)
	// and 2 or 3 subs such as `_R_.xxxxx.>` on each server, so a total of 12 subs.
	if deleteSubs := c.stableTotalSubs(); deleteSubs > startSubs+12 {
		t.Fatalf("Expected subs to return to %d from a high of %d, but got %d", startSubs, afterSubs, deleteSubs)
	}
}

func TestJetStreamClusterCreateConcurrentDurableConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create origin stream, must be R > 1
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.QueueSubscribeSync("ORDERS", "wq", nats.Durable("shared")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now try to create durables concurrently.
	start := make(chan struct{})
	var wg sync.WaitGroup
	created := uint32(0)
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start
			_, err := js.QueueSubscribeSync("ORDERS", "wq", nats.Durable("shared"))
			if err == nil {
				atomic.AddUint32(&created, 1)
			} else if !strings.Contains(err.Error(), "consumer name already") {
				errs <- err
			}
		}()
	}

	close(start)
	wg.Wait()

	if lc := atomic.LoadUint32(&created); lc != 10 {
		t.Fatalf("Expected all 10 to be created, got %d", lc)
	}
	if len(errs) > 0 {
		t.Fatalf("Failed to create some sub: %v", <-errs)
	}
}

// https://github.com/nats-io/nats-server/issues/2144
func TestJetStreamClusterUpdateStreamToExisting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS1",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS2",
		Replicas: 3,
		Subjects: []string{"bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "ORDERS2",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	if err == nil {
		t.Fatalf("Expected an error but got none")
	}
}

func TestJetStreamClusterCrossAccountInterop(t *testing.T) {
	template := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: HUB, store_dir: "%s"}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.INFO.>" }
				{ service: "$JS.HUB.API.CONSUMER.>", response: stream }
				{ stream: "M.SYNC.>" } # For the mirror
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.INFO.TEST.DLC"}, to: "FROM.DLC" }
				{ service: { account: JS, subject: "$JS.HUB.API.CONSUMER.>"}, to: "js.xacc.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "M.SYNC.>"} }
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`

	c := createJetStreamClusterWithTemplate(t, template, "HUB", 3)
	defer c.shutdown()

	// Create the stream and the consumer under the JS/rip user.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DLC", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Also create a stream via the domain qualified API.
	js, err = nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now we want to access the consumer info from IA/dlc.
	nc2, js2 := jsClientConnect(t, c.randomServer(), nats.UserInfo("dlc", "pass"))
	defer nc2.Close()

	if _, err := nc2.Request("FROM.DLC", nil, time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure domain mappings etc work across accounts.
	// Setup a mirror.
	_, err = js2.AddStream(&nats.StreamConfig{
		Name: "MIRROR",
		Mirror: &nats.StreamSource{
			Name: "ORDERS",
			External: &nats.ExternalStream{
				APIPrefix:     "js.xacc.API",
				DeliverPrefix: "M.SYNC",
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send 10 messages..
	msg, toSend := []byte("Hello mapped domains"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("ORDERS", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MIRROR")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 10 {
			return fmt.Errorf("Expected 10 msgs, got state: %+v", si.State)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/2242
func TestJetStreamClusterMsgIdDuplicateBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendMsgID := func(id string) (*nats.PubAck, error) {
		t.Helper()
		m := nats.NewMsg("foo")
		m.Header.Add(JSMsgId, id)
		m.Data = []byte("HELLO WORLD")
		return js.PublishMsg(m)
	}

	if _, err := sendMsgID("1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// This should fail with duplicate detected.
	if pa, _ := sendMsgID("1"); pa == nil || !pa.Duplicate {
		t.Fatalf("Expected duplicate but got none: %+v", pa)
	}
	// This should be fine.
	if _, err := sendMsgID("2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterNilMsgWithHeaderThroughSourcedStream(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	spoke := c.createLeafNodeWithTemplate("SPOKE", tmpl)
	defer spoke.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, spoke)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	jsHub, err := nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	_, err = jsHub.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources: []*nats.StreamSource{{
			Name:     "TEST",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now send a message to the origin stream with nil body and a header.
	m := nats.NewMsg("foo")
	m.Header.Add("X-Request-ID", "e9a639b4-cecb-4fbe-8376-1ef511ae1f8d")
	m.Data = []byte("HELLO WORLD")

	if _, err = jsHub.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := jsHub.SubscribeSync("foo", nats.BindStream("S"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(msg.Data) != "HELLO WORLD" {
		t.Fatalf("Message corrupt? Expecting %q got %q", "HELLO WORLD", msg.Data)
	}
}

// Make sure varz reports the server usage not replicated usage etc.
func TestJetStreamClusterVarzReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// ~100k per message.
	msg := []byte(strings.Repeat("A", 99_960))
	msz := fileStoreMsgSize("TEST", nil, msg)
	total := msz * 10

	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// To show the bug we need this to allow remote usage to replicate.
	time.Sleep(2 * usageTick)

	v, err := s.Varz(nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if v.JetStream.Stats.Store > total {
		t.Fatalf("Single server varz JetStream store usage should be <= %d, got %d", total, v.JetStream.Stats.Store)
	}

	info, err := js.AccountInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Store < total*3 {
		t.Fatalf("Expected account information to show usage ~%d, got %d", total*3, info.Store)
	}
}

func TestJetStreamClusterStatszActiveServers(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 2, 2)
	defer sc.shutdown()

	checkActive := func(expected int) {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			s := sc.randomServer()
			nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			if err != nil {
				t.Fatalf("Failed to create system client: %v", err)
			}
			defer nc.Close()

			resp, err := nc.Request(serverStatsPingReqSubj, nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ssm ServerStatsMsg
			if err := json.Unmarshal(resp.Data, &ssm); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ssm.Stats.ActiveServers != expected {
				return fmt.Errorf("Wanted %d, got %d", expected, ssm.Stats.ActiveServers)
			}
			return nil
		})
	}

	checkActive(4)
	c := sc.randomCluster()
	ss := c.randomServer()
	ss.Shutdown()
	checkActive(3)
	c.restartServer(ss)
	checkActive(4)
}

func TestJetStreamClusterSourceAndMirrorConsumersLeaderChange(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnect(t, c1.randomServer())
	defer nc.Close()

	var sources []*nats.StreamSource
	numStreams := 10

	for i := 1; i <= numStreams; i++ {
		name := fmt.Sprintf("O%d", i)
		sources = append(sources, &nats.StreamSource{Name: name})
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Place our new stream that will source all the others in different cluster.
	nc, js = jsClientConnect(t, c2.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  sources,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")

	// Now make sure we only have a single direct consumer on our origin streams.
	// Pick one at random.
	name := fmt.Sprintf("O%d", rand.Intn(numStreams-1)+1)
	c1.waitOnStreamLeader("$G", name)
	s := c1.streamLeader("$G", name)
	a, err := s.lookupAccount("$G")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	mset, err := a.lookupStream(name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 1 {
			return fmt.Errorf("Stream %q wanted 1 direct consumer, got %d", name, ndc)
		}
		return nil
	})

	// Now create a mirror of selected from above. Will test same scenario.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: name},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 2 {
			return fmt.Errorf("Stream %q wanted 2 direct consumers, got %d", name, ndc)
		}
		return nil
	})
}

func TestJetStreamClusterExtendedStreamPurge(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   2,
				MaxMsgsPer: 100,
			}
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			for i := 0; i < 1000; i++ {
				js.PublishAsync("kv.foo", []byte("OK")) // 1 * i
				js.PublishAsync("kv.bar", []byte("OK")) // 2 * i
				js.PublishAsync("kv.baz", []byte("OK")) // 3 * i, so after first is 2700, last is 3000
			}
			for i := 0; i < 700; i++ {
				js.PublishAsync(fmt.Sprintf("kv.%d", i+1), []byte("OK"))
			}

			checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
				si, err := js.StreamInfo("KV")
				if err != nil {
					return err
				}
				if si.State.Msgs != 1000 {
					return fmt.Errorf("Expected %d msgs, got %d", 300, si.State.Msgs)
				}
				return nil
			})

			shouldFail := func(preq *JSApiStreamPurgeRequest) {
				req, _ := json.Marshal(preq)
				resp, err := nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "KV"), req, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				var pResp JSApiStreamPurgeResponse
				if err = json.Unmarshal(resp.Data, &pResp); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if pResp.Success || pResp.Error == nil {
					t.Fatalf("Expected an error response but got none")
				}
			}

			// Sequence and Keep should be mutually exclusive.
			shouldFail(&JSApiStreamPurgeRequest{Sequence: 10, Keep: 10})

			purge := func(preq *JSApiStreamPurgeRequest, newTotal uint64) {
				t.Helper()
				req, _ := json.Marshal(preq)
				resp, err := nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "KV"), req, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				var pResp JSApiStreamPurgeResponse
				if err = json.Unmarshal(resp.Data, &pResp); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !pResp.Success || pResp.Error != nil {
					t.Fatalf("Got a bad response %+v", pResp)
				}
				si, err = js.StreamInfo("KV")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != newTotal {
					t.Fatalf("Expected total after purge to be %d but got %d", newTotal, si.State.Msgs)
				}
			}
			expectLeft := func(subject string, expected uint64) {
				t.Helper()
				ci, err := js.AddConsumer("KV", &nats.ConsumerConfig{Durable: "dlc", FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				defer js.DeleteConsumer("KV", "dlc")
				if ci.NumPending != expected {
					t.Fatalf("Expected %d remaining but got %d", expected, ci.NumPending)
				}
			}

			purge(&JSApiStreamPurgeRequest{Subject: "kv.foo"}, 900)
			expectLeft("kv.foo", 0)

			purge(&JSApiStreamPurgeRequest{Subject: "kv.bar", Keep: 1}, 801)
			expectLeft("kv.bar", 1)

			purge(&JSApiStreamPurgeRequest{Subject: "kv.baz", Sequence: 2851}, 751)
			expectLeft("kv.baz", 50)

			purge(&JSApiStreamPurgeRequest{Subject: "kv.*"}, 0)

			// RESET
			js.DeleteStream("KV")
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if _, err := js.StreamInfo("KV"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Put in 100.
			for i := 0; i < 100; i++ {
				js.Publish("kv.foo", []byte("OK"))
			}
			purge(&JSApiStreamPurgeRequest{Subject: "kv.foo", Keep: 10}, 10)
			purge(&JSApiStreamPurgeRequest{Subject: "kv.foo", Keep: 10}, 10)
			expectLeft("kv.foo", 10)

			// RESET AGAIN
			js.DeleteStream("KV")
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if _, err := js.StreamInfo("KV"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Put in 100.
			for i := 0; i < 100; i++ {
				js.Publish("kv.foo", []byte("OK"))
			}
			purge(&JSApiStreamPurgeRequest{Keep: 10}, 10)
			expectLeft(">", 10)

			// RESET AGAIN
			js.DeleteStream("KV")
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if _, err := js.StreamInfo("KV"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Put in 100.
			for i := 0; i < 100; i++ {
				js.Publish("kv.foo", []byte("OK"))
			}
			purge(&JSApiStreamPurgeRequest{Sequence: 90}, 11) // Up to 90 so we keep that, hence the 11.
			expectLeft(">", 11)
		})
	}
}

func TestPurgeBySequence(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {

			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.*.*"},
				Storage:    st,
				Replicas:   2,
				MaxMsgsPer: 5,
			}
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			for i := 0; i < 20; i++ {
				if _, err = js.Publish("kv.myapp.username", []byte(fmt.Sprintf("value %d", i))); err != nil {
					t.Fatalf("request failed: %s", err)
				}
			}
			for i := 0; i < 20; i++ {
				if _, err = js.Publish("kv.myapp.password", []byte(fmt.Sprintf("value %d", i))); err != nil {
					t.Fatalf("request failed: %s", err)
				}
			}
			expectSequences := func(t *testing.T, subject string, seq ...int) {
				sub, err := js.SubscribeSync(subject)
				if err != nil {
					t.Fatalf("sub failed: %s", err)
				}
				defer sub.Unsubscribe()
				for _, i := range seq {
					msg, err := sub.NextMsg(time.Second)
					if err != nil {
						t.Fatalf("didn't get message: %s", err)
					}
					meta, err := msg.Metadata()
					if err != nil {
						t.Fatalf("didn't get metadata: %s", err)
					}
					if meta.Sequence.Stream != uint64(i) {
						t.Fatalf("expected sequence %d got %d", i, meta.Sequence.Stream)
					}
				}
			}
			expectSequences(t, "kv.myapp.username", 16, 17, 18, 19, 20)
			expectSequences(t, "kv.myapp.password", 36, 37, 38, 39, 40)

			// delete up to but not including 18 of username...
			jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "kv.myapp.username", Sequence: 18})
			_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "KV"), jr, time.Second)
			if err != nil {
				t.Fatalf("request failed: %s", err)
			}
			// 18 should still be there
			expectSequences(t, "kv.myapp.username", 18, 19, 20)
		})
	}
}

func TestJetStreamClusterMaxConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 1,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXC")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}
	// Make sure we get the right error.
	// This should succeed.
	if _, err := js.SubscribeSync("in.maxc.foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("in.maxc.bar"); err == nil {
		t.Fatalf("Eexpected error but got none")
	}
}

func TestJetStreamClusterMaxConsumersMultipleConcurrentRequests(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXCC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxcc.>"},
		MaxConsumers: 1,
		Replicas:     3,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXCC")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()
			<-startCh
			js.SubscribeSync("in.maxcc.foo")
		}()
	}
	// Wait for Go routines.
	time.Sleep(250 * time.Millisecond)

	close(startCh)
	wg.Wait()

	var names []string
	for n := range js.ConsumerNames("MAXCC") {
		names = append(names, n)
	}
	if nc := len(names); nc > 1 {
		t.Fatalf("Expected only 1 consumer, got %d", nc)
	}
}

func TestJetStreamPanicDecodingConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	rch := make(chan struct{}, 1)
	nc, js := jsClientConnect(t, c.randomServer(),
		nats.ReconnectWait(50*time.Millisecond),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			rch <- struct{}{}
		}),
	)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"ORDERS.*"},
		Storage:   nats.FileStorage,
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
		Discard:   nats.DiscardNew,
		MaxMsgs:   -1,
		MaxAge:    time.Hour * 24 * 365,
	}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	sub, err := js.PullSubscribe("ORDERS.created", "durable", nats.MaxAckPending(1000))

	if err != nil {
		t.Fatalf("Error creating pull subscriber: %v", err)
	}

	sendMsg := func(subject string) {
		t.Helper()
		if _, err := js.Publish(subject, []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		sendMsg("ORDERS.something")
		sendMsg("ORDERS.created")
	}

	for total := 0; total != 100; {
		msgs, err := sub.Fetch(100-total, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Failed to fetch message: %v", err)
		}
		for _, m := range msgs {
			m.Ack()
			total++
		}
	}

	c.stopAll()
	c.restartAllSamePorts()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "durable")

	select {
	case <-rch:
	case <-time.After(2 * time.Second):
		t.Fatal("Did not reconnect")
	}

	for i := 0; i < 100; i++ {
		sendMsg("ORDERS.something")
		sendMsg("ORDERS.created")
	}

	for total := 0; total != 100; {
		msgs, err := sub.Fetch(100-total, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Error on fetch: %v", err)
		}
		for _, m := range msgs {
			m.Ack()
			total++
		}
	}
}

// Support functions

// Used to setup superclusters for tests.
type supercluster struct {
	t        *testing.T
	clusters []*cluster
}

func (sc *supercluster) shutdown() {
	if sc == nil {
		return
	}
	for _, c := range sc.clusters {
		shutdownCluster(c)
	}
}

func (sc *supercluster) randomServer() *Server {
	return sc.randomCluster().randomServer()
}

var jsClusterAccountsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: one

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		TWO { users = [ { user: "two", pass: "p" } ]; jetstream: enabled }
		NOJS { users = [ { user: "nojs", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsSuperClusterTempl = `
	%s
	gateway {
		name: %s
		listen: 127.0.0.1:%d
		gateways = [%s
		]
	}

	system_account: "$SYS"
`

var jsClusterLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: "%s"}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		ONE {
			users = [ { user: "u", pass: "s3cr3t!" } ]
			jetstream: enabled
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsGWTempl = `%s{name: %s, urls: [%s]}`

func createJetStreamSuperCluster(t *testing.T, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplate(t, jsClusterTempl, numServersPer, numClusters)
}

func createJetStreamSuperClusterWithTemplate(t *testing.T, tmpl string, numServersPer, numClusters int) *supercluster {
	t.Helper()
	if numServersPer < 1 {
		t.Fatalf("Number of servers must be >= 1")
	}
	if numClusters <= 1 {
		t.Fatalf("Number of clusters must be > 1")
	}

	startClusterPorts := []int{20_022, 22_022, 24_022}
	startGatewayPorts := []int{20_122, 22_122, 24_122}
	startClusterPort := startClusterPorts[rand.Intn(len(startClusterPorts))]
	startGWPort := startGatewayPorts[rand.Intn(len(startGatewayPorts))]

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	cp, gp := startClusterPort, startGWPort
	var clusters []*cluster

	var gws []string
	// Build GWs first, will be same for all servers.
	for i, port := 1, gp; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		var urls []string
		for n := 0; n < numServersPer; n++ {
			urls = append(urls, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
			port++
		}
		gws = append(gws, fmt.Sprintf(jsGWTempl, "\n\t\t\t", cn, strings.Join(urls, ",")))
	}
	gwconf := strings.Join(gws, "")

	for i := 1; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		// Go ahead and build configurations.
		c := &cluster{servers: make([]*Server, 0, numServersPer), opts: make([]*Options, 0, numServersPer), name: cn}

		// Build out the routes that will be shared with all configs.
		var routes []string
		for port := cp; port < cp+numServersPer; port++ {
			routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
		}
		routeConfig := strings.Join(routes, ",")

		for si := 0; si < numServersPer; si++ {
			storeDir := createDir(t, JetStreamStoreDir)
			sn := fmt.Sprintf("%s-S%d", cn, si+1)
			bconf := fmt.Sprintf(tmpl, sn, storeDir, cn, cp+si, routeConfig)
			conf := fmt.Sprintf(jsSuperClusterTempl, bconf, cn, gp, gwconf)
			gp++
			s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
			c.servers = append(c.servers, s)
			c.opts = append(c.opts, o)
		}
		checkClusterFormed(t, c.servers...)
		clusters = append(clusters, c)
		cp += numServersPer
		c.t = t
	}

	// Wait for the supercluster to be formed.
	egws := numClusters - 1
	for _, c := range clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, egws, 2*time.Second)
		}
	}

	sc := &supercluster{t, clusters}
	sc.waitOnLeader()
	sc.waitOnAllCurrent()

	// Wait for all the peer nodes to be registered.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		var peers []string
		if ml := sc.leader(); ml != nil {
			peers = ml.ActivePeers()
			if len(peers) == numClusters*numServersPer {
				return nil
			}
		}
		return fmt.Errorf("Not correct number of peers, expected %d, got %d", numClusters*numServersPer, len(peers))
	})

	if sc.leader() == nil {
		sc.t.Fatalf("Expected a cluster leader, got none")
	}

	return sc
}

func (sc *supercluster) createLeafNodes(clusterName string, numServers int) *cluster {
	// Create our leafnode cluster template first.
	return sc.randomCluster().createLeafNodes(clusterName, numServers)
}

func (sc *supercluster) createSingleLeafNode() *Server {
	return sc.randomCluster().createLeafNode()
}

func (sc *supercluster) leader() *Server {
	for _, c := range sc.clusters {
		if leader := c.leader(); leader != nil {
			return leader
		}
	}
	return nil
}

func (sc *supercluster) waitOnLeader() {
	sc.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.leader(); leader != nil {
				time.Sleep(200 * time.Millisecond)
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a cluster leader, got none")
}

func (sc *supercluster) waitOnAllCurrent() {
	for _, c := range sc.clusters {
		c.waitOnAllCurrent()
	}
}

func (sc *supercluster) clusterForName(name string) *cluster {
	for _, c := range sc.clusters {
		if c.name == name {
			return c
		}
	}
	return nil
}

func (sc *supercluster) randomCluster() *cluster {
	clusters := append(sc.clusters[:0:0], sc.clusters...)
	rand.Shuffle(len(clusters), func(i, j int) { clusters[i], clusters[j] = clusters[j], clusters[i] })
	return clusters[0]
}

func (sc *supercluster) waitOnPeerCount(n int) {
	sc.t.Helper()
	sc.waitOnLeader()
	leader := sc.leader()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		peers := leader.JetStreamClusterPeers()
		if len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a super cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

var jsClusterMirrorSourceImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
				{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.>", response: stream }
				{ service: "TEST" } # For publishing to the stream.
				{ service: "$JS.ACK.TEST.*.>" }
			]
		}
		IA {
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { subject: "$JS.API.>", account: JS }}
				{ service: { subject: "TEST", account: JS }}
				{ service: { subject: "$JS.ACK.TEST.*.>", account: JS }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func createMixedModeCluster(t *testing.T, tmpl string, clusterName, snPre string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	t.Helper()

	if clusterName == _EMPTY_ || numJsServers < 1 || numNonServers < 1 {
		t.Fatalf("Bad params")
	}

	numServers := numJsServers + numNonServers
	const startClusterPort = 23232

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)

		sn := fmt.Sprintf("%sS-%d", snPre, cp-startClusterPort+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)

		// Disable JS here.
		if cp-startClusterPort >= numJsServers {
			// We can disable by commmenting it out, meaning no JS config, or can set the config up and just set disabled.
			// e.g. jetstream: {domain: "SPOKE", enabled: false}
			if doJSConfig {
				conf = strings.Replace(conf, "jetstream: {", "jetstream: { enabled: false, ", 1)
			} else {
				conf = strings.Replace(conf, "jetstream: ", "# jetstream: ", 1)
			}
		}

		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	c.waitOnPeerCount(numJsServers)

	return c
}

// This will create a cluster that is explicitly configured for the routes, etc.
// and also has a defined clustername. All configs for routes and cluster name will be the same.
func createJetStreamClusterExplicit(t *testing.T, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplate(t, jsClusterTempl, clusterName, numServers)
}

func createJetStreamClusterWithTemplate(t *testing.T, tmpl string, clusterName string, numServers int) *cluster {
	startPorts := []int{7_022, 9_022, 11_022, 15_022}
	port := startPorts[rand.Intn(len(startPorts))]
	return createJetStreamCluster(t, tmpl, clusterName, _EMPTY_, numServers, port, true)
}

func createJetStreamCluster(t *testing.T, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool) *cluster {
	t.Helper()
	if clusterName == _EMPTY_ || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := portStart; cp < portStart+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := portStart; cp < portStart+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)
		sn := fmt.Sprintf("%sS-%d", snPre, cp-portStart+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if waitOnReady {
		c.waitOnClusterReady()
	}

	return c
}

func (c *cluster) addInNewServer() *Server {
	c.t.Helper()
	sn := fmt.Sprintf("S-%d", len(c.servers)+1)
	storeDir, _ := ioutil.TempDir(tempRoot, JetStreamStoreDir)
	seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
	conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, c.name, -1, seedRoute)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	c.checkClusterFormed()
	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccount() *Server {
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln1 := fmt.Sprintf("nats://one:p@%s:%d", lno.Host, lno.Port)
	ln2 := fmt.Sprintf("nats://two:p@%s:%d", lno.Host, lno.Port)
	conf := fmt.Sprintf(jsClusterSingleLeafNodeTempl, createDir(c.t, JetStreamStoreDir), ln1, ln2)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 2)

	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStream() *Server {
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln := fmt.Sprintf("nats://nojs:p@%s:%d", lno.Host, lno.Port)
	conf := fmt.Sprintf(jsClusterSingleLeafNodeLikeNGSTempl, createDir(c.t, JetStreamStoreDir), ln)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 1)

	return s
}

var jsClusterSingleLeafNodeLikeNGSTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	leaf { remotes [ { urls: [ %s ] } ] }
`

var jsClusterSingleLeafNodeTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	leaf { remotes [
		{ urls: [ %s ], account: "JSY" }
		{ urls: [ %s ], account: "JSN" } ]
	}

	accounts {
		JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
		JSN { users = [ { user: "n", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTemplWithLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# Need to keep below since it fills in the store dir by default so just comment out.
	# jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: "%s"}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# jetstream: {store_dir: "%s"}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsLeafFrag = `
	leaf {
		remotes [
			{ urls: [ %s ] }
			{ urls: [ %s ], account: "$SYS" }
		]
	}
`

func (c *cluster) createLeafNodes(clusterName string, numServers int) *cluster {
	return c.createLeafNodesWithStartPort(clusterName, numServers, 22111)
}

func (c *cluster) createLeafNodesNoJS(clusterName string, numServers int) *cluster {
	return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNodeNoJS, clusterName, numServers, 21333)
}

func (c *cluster) createLeafNodesWithStartPort(clusterName string, numServers int, portStart int) *cluster {
	return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNode, clusterName, numServers, portStart)
}

func (c *cluster) createLeafNode() *Server {
	return c.createLeafNodeWithTemplate("LNS", jsClusterTemplWithSingleLeafNode)
}

func (c *cluster) createLeafNodeWithTemplate(name, template string) *Server {
	tmpl := c.createLeafSolicit(template)
	conf := fmt.Sprintf(tmpl, name, createDir(c.t, JetStreamStoreDir))
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	return s
}

// Helper to generate the leaf solicit configs.
func (c *cluster) createLeafSolicit(tmpl string) string {
	// Create our leafnode cluster template first.
	var lns, lnss []string
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		ln := s.getOpts().LeafNode
		lns = append(lns, fmt.Sprintf("nats://%s:%d", ln.Host, ln.Port))
		lnss = append(lnss, fmt.Sprintf("nats://admin:s3cr3t!@%s:%d", ln.Host, ln.Port))
	}
	lnc := strings.Join(lns, ", ")
	lnsc := strings.Join(lnss, ", ")
	lconf := fmt.Sprintf(jsLeafFrag, lnc, lnsc)
	return strings.Replace(tmpl, "{{leaf}}", lconf, 1)
}

func (c *cluster) createLeafNodesWithTemplateMixedMode(template, clusterName string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"

	lc := createMixedModeCluster(c.t, tmpl, clusterName, pre, numJsServers, numNonServers, doJSConfig)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	lc.waitOnClusterReadyWithNumPeers(numJsServers)

	return lc
}

func (c *cluster) createLeafNodesWithTemplateAndStartPort(template, clusterName string, numServers int, portStart int) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"
	lc := createJetStreamCluster(c.t, tmpl, clusterName, pre, numServers, portStart, false)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	return lc
}

// Will add in the mapping for the account to each server.
func (c *cluster) addSubjectMapping(account, src, dest string) {
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error on %v: %v", s, err)
		}
		if err := acc.AddMapping(src, dest); err != nil {
			c.t.Fatalf("Error adding mapping: %v", err)
		}
	}
	// Make sure interest propagates.
	time.Sleep(200 * time.Millisecond)
}

// Adjust limits for the given account.
func (c *cluster) updateLimits(account string, newLimits *JetStreamAccountLimits) {
	c.t.Helper()
	for _, s := range c.servers {
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
		if err := acc.UpdateJetStreamLimits(newLimits); err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Hack for staticcheck
var skip = func(t *testing.T) {
	t.SkipNow()
}

func jsClientConnect(t *testing.T, s *Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func fetchMsgs(t *testing.T, sub *nats.Subscription, numExpected int, wait time.Duration) []*nats.Msg {
	t.Helper()
	msgs, err := sub.Fetch(numExpected, nats.MaxWait(wait))
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != numExpected {
		t.Fatalf("Unexpected msg count, got %d, want %d", len(msgs), numExpected)
	}
	return msgs
}

func (c *cluster) restartServer(rs *Server) *Server {
	c.t.Helper()
	index := -1
	var opts *Options
	for i, s := range c.servers {
		if s == rs {
			index = i
			break
		}
	}
	if index < 0 {
		c.t.Fatalf("Could not find server %v to restart", rs)
	}
	opts = c.opts[index]
	s, o := RunServerWithConfig(opts.ConfigFile)
	c.servers[index] = s
	c.opts[index] = o
	return s
}

func (c *cluster) checkClusterFormed() {
	c.t.Helper()
	checkClusterFormed(c.t, c.servers...)
}

func (c *cluster) waitOnPeerCount(n int) {
	c.t.Helper()
	c.waitOnLeader()
	leader := c.leader()
	for leader == nil {
		c.waitOnLeader()
		leader = c.leader()
	}
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		if peers := leader.JetStreamClusterPeers(); len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

func (c *cluster) waitOnConsumerLeader(account, stream, consumer string) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.consumerLeader(account, stream, consumer); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a consumer leader for %q %q %q, got none", account, stream, consumer)
}

func (c *cluster) consumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamLeader(account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.streamLeader(account, stream); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

func (c *cluster) randomNonStreamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(account, stream) && !s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) streamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamCurrent(s *Server, account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsStreamCurrent(account, stream) {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current for stream %q", s, stream)
}

func (c *cluster) waitOnServerCurrent(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsCurrent() {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current", s)
}

func (c *cluster) waitOnAllCurrent() {
	for _, cs := range c.servers {
		c.waitOnServerCurrent(cs)
	}
}

func (c *cluster) serverByName(sname string) *Server {
	for _, s := range c.servers {
		if s.Name() == sname {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonLeader() *Server {
	// range should randomize.. but..
	for _, s := range c.servers {
		if s.Running() && !s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) leader() *Server {
	for _, s := range c.servers {
		if s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) expectNoLeader() {
	c.t.Helper()
	expires := time.Now().Add(maxElectionTimeout)
	for time.Now().Before(expires) {
		if c.leader() == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("Expected no leader but have one")
}

func (c *cluster) waitOnLeader() {
	c.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.leader(); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	c.t.Fatalf("Expected a cluster leader, got none")
}

// Helper function to check that a cluster is formed
func (c *cluster) waitOnClusterReady() {
	c.t.Helper()
	c.waitOnClusterReadyWithNumPeers(len(c.servers))
}

func (c *cluster) waitOnClusterReadyWithNumPeers(numPeersExpected int) {
	c.t.Helper()
	var leader *Server
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		if leader = c.leader(); leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Now make sure we have all peers.
	for leader != nil && time.Now().Before(expires) {
		if len(leader.JetStreamClusterPeers()) == numPeersExpected {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	peersSeen := len(leader.JetStreamClusterPeers())
	c.shutdown()
	if leader == nil {
		c.t.Fatalf("Expected a cluster leader and fully formed cluster, no leader")
	} else {
		c.t.Fatalf("Expected a fully formed cluster, only %d of %d peers seen", peersSeen, numPeersExpected)
	}
}

// Helper function to remove JetStream from a server.
func (c *cluster) removeJetStream(s *Server) {
	c.t.Helper()
	index := -1
	for i, cs := range c.servers {
		if cs == s {
			index = i
			break
		}
	}
	cf := c.opts[index].ConfigFile
	cb, _ := ioutil.ReadFile(cf)
	var sb strings.Builder
	for _, l := range strings.Split(string(cb), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "jetstream") {
			sb.WriteString(l + "\n")
		}
	}
	if err := ioutil.WriteFile(cf, []byte(sb.String()), 0644); err != nil {
		c.t.Fatalf("Error writing updated config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		c.t.Fatalf("Error on server reload: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func (c *cluster) stopAll() {
	c.t.Helper()
	for _, s := range c.servers {
		s.Shutdown()
	}
}

func (c *cluster) restartAll() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s, o := RunServerWithConfig(opts.ConfigFile)
			c.servers[i] = s
			c.opts[i] = o
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) restartAllSamePorts() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s := RunServer(opts)
			c.servers[i] = s
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) totalSubs() (total int) {
	c.t.Helper()
	for _, s := range c.servers {
		total += int(s.NumSubscriptions())
	}
	return total
}

func (c *cluster) stableTotalSubs() (total int) {
	nsubs := -1
	checkFor(c.t, 2*time.Second, 250*time.Millisecond, func() error {
		subs := c.totalSubs()
		if subs == nsubs {
			return nil
		}
		nsubs = subs
		return fmt.Errorf("Still stabilizing")
	})
	return nsubs
}
