package streams

import (
	"github.com/borderlesshq/axon/v2"
	"sync"
	"time"
)

type flow int

const (
	closeFlow flow = iota + 1
	runFlow
)

type cmd struct {
	flow flow
	str  *stream
}

type Streams struct {
	su                     sync.RWMutex
	subscribers            map[string]*stream
	streamProcessorChan    chan cmd
	staleStreamCleanupTime time.Duration
	pipe                   axon.EventStore
}

const (
	streamSuffix    = "-stream"
	heartbeatSuffix = "-heartbeat"
)

type stream struct {
	executed         bool
	streamChannel    string
	heartBeatChannel string
	closer           chan bool
	pipe             axon.EventStore
	timeCreated      time.Time
	streamHandler    StreamHandler
}

type StreamHandler func(Send, Close)

//*handler.SubscriptionHandler

type Send func(b []byte) error
type Close func()
