package streams

import (
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"time"
)

const (
	pingSig  = "ping"
	pongSig  = "pong"
	closeSig = "close"
)

func (s *stream) serve(closeChan chan<- cmd) {
	defer func() {
		// This is used to signal what stream should be cleaned up from the streams.subscribers[]
		closeChan <- cmd{
			flow: closeFlow,
			str:  s,
		}
	}()

	// Check heartbeat of caller if caller is alive.
	go func() {
		defer s.close()
		for {
			time.Sleep(1 * time.Second) // Check heartbeat every second or die.
			ok, err := s.pipe.Request(s.heartBeatChannel, []byte(pingSig), options.DisablePubStreaming())
			if err != nil {
				break
			}

			if string(ok.Body) != pongSig {
				break
			}
		}
	}()

	// Turn on heart beat for caller so that caller can disconnect if responder is dead.
	go func() {
		// Reply to heartbeat every second or die.
		err := s.pipe.Reply(s.heartBeatChannel, func(mg *messages.Message) (*messages.Message, error) {
			if string(mg.Body) != pingSig {
				s.close()
				return mg.WithBody([]byte(closeSig)), nil
			}

			return mg.WithBody([]byte(pongSig)), nil
		})

		if err != nil {
			s.close()
		}
	}()

	go s.streamHandler(s.send, s.close)

	<-s.wait()
	// NOTE: defer() func above is called before this function exits.
	//Thus signalling the cleaner to remove this stream from the list of streams.
}

func (s *stream) close() {
	close(s.closer)
}

func (s *stream) wait() <-chan bool {
	return s.closer
}

func (s *stream) send(payload []byte) error {
	// DisablePubStreaming ensures we don't store this stream in the eventStore.
	// They are fire and forget.
	return s.pipe.Publish(s.streamChannel, payload, options.DisablePubStreaming())
}
