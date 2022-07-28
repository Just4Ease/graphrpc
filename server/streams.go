package server

import (
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/utils"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql/handler"
	"sync"
)

type streams struct {
	subscribers    map[string]*stream
	streamWatchers chan string
	sync.RWMutex
}

func MakeStreams() *streams {
	return &streams{
		subscribers:    make(map[string]*stream),
		streamWatchers: make(chan string, 1),
		RWMutex:        sync.RWMutex{},
	}
}

func (s *streams) processStreams() {

}

func (s *streams) close() {
	for streamChannel := range s.streamWatchers {
		s.RLock()
		str, ok := s.subscribers[streamChannel]
		if !ok {
			s.RUnlock()
			return
		}

		delete(s.subscribers, streamChannel)
		s.RUnlock()
		str.closeWithoutSignal()
	}
}

type stream struct {
	streamChannel       string
	heartBeatChannel    string
	closer              chan bool
	signalWatcher       chan string
	pipe                axon.EventStore
	subscriptionHandler *handler.SubscriptionHandler
}

func (s *streams) newStream(pipe axon.EventStore, subHandler *handler.SubscriptionHandler) (streamChannel, heartBeatChannel string) {
	s.RLock()

	// Cook UniqueID
kitchen:
	id := utils.GenerateRandomString()
	streamChannel = fmt.Sprintf("%s-stream", id)
	if _, ok := s.subscribers[streamChannel]; !ok {
		goto kitchen // This ensures a regeneration for unique value.
	}

	heartBeatChannel = fmt.Sprintf("%s-heartbeat", id)
	s.subscribers[streamChannel] = &stream{
		streamChannel:    streamChannel,
		heartBeatChannel: heartBeatChannel,
		//signalWatcher:
		pipe:                pipe,
		subscriptionHandler: subHandler,
	}
	s.RUnlock()
	return streamChannel, heartBeatChannel
}
