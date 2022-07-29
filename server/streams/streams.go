package streams

import (
	"errors"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/utils"
	"sync"
	"time"
)

func MakeStreams(pipe axon.EventStore) *Streams {
	return &Streams{
		subscribers:            make(map[string]*stream),
		streamProcessorChan:    make(chan cmd, 3),
		staleStreamCleanupTime: time.Second * 5,
		pipe:                   pipe,
		su:                     sync.RWMutex{},
	}
}

func (s *Streams) Run() {
	go s.streamCleaner() // Have a runner that cleans up stale streams.

	for cmd := range s.streamProcessorChan {
		s.su.RLock()
		switch cmd.flow {
		case closeFlow:
			delete(s.subscribers, cmd.str.streamChannel)
			cmd.str.close()
		case runFlow:
			cmd.str.executed = true
			go cmd.str.serve(s.streamProcessorChan)
			s.subscribers[cmd.str.streamChannel] = cmd.str // Add it back to the pool.
		}

		s.su.RUnlock()
	}
}

/**
streamCleaner cleans stale streams.
*/
func (s *Streams) streamCleaner() {
roster:
	for _, str := range s.subscribers {
		FiveMinutes := time.Now().Add(s.staleStreamCleanupTime)
		if str.timeCreated.After(FiveMinutes) {
			s.streamProcessorChan <- cmd{
				flow: closeFlow,
				str:  str,
			}
		}
	}

	time.Sleep(time.Second * 1)
	goto roster
}

func (s *Streams) RunStream(channel string) (heatBeatChannel string, err error) {
	str, ok := s.subscribers[channel]
	if !ok {
		return "", errors.New("kindly initialize stream setup")
	}

	s.streamProcessorChan <- cmd{
		flow: runFlow,
		str:  str,
	}

	return str.heartBeatChannel, nil
}

func (s *Streams) NewStream(h StreamHandler) (streamChannel, heartBeatChannel string) {
	s.su.RLock()

kitchen:
	// Cook unique id
	streamChannel = utils.GenerateRandomString() + streamSuffix
	if _, ok := s.subscribers[streamChannel]; !ok {
		goto kitchen // This ensures a regeneration for unique value.
	}

	// produces: id-stream-heartbeat
	heartBeatChannel = streamChannel + heartbeatSuffix

	s.subscribers[streamChannel] = &stream{
		executed:         false,
		streamChannel:    streamChannel,
		heartBeatChannel: heartBeatChannel,
		closer:           make(chan bool),
		pipe:             s.pipe,
		timeCreated:      time.Now(),
		streamHandler:    h,
	}
	s.su.RUnlock()

	return streamChannel, heartBeatChannel
}
