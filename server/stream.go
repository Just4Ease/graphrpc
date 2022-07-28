package server

import (
	"encoding/json"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"time"
)

const (
	pingSig  = "ping"
	pongSig  = "pong"
	closeSig = "close"
)

func (s stream) serve(closeChan chan<- string) {
	s.closer = make(chan bool)
	defer func() {
		// This is used to signal what stream should be cleaned up from the streams.subscribers[]
		closeChan <- s.streamChannel
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

	go func() {
		defer func() {
			gqlerr := s.subscriptionHandler.PanicHandler()
			if gqlerr != nil {
				s.sendErr()
			}
		}()
		responses, ctx := s.subscriptionHandler.Exec()

		for {
			response := responses(ctx)
			if response == nil {
				break
			}

			s.sendResponse(response)
		}
	}()

	s.wait()
	// NOTE: defer() func above is called before this function exits.
	//Thus signalling the cleaner to remove this stream from the list of streams.
}

func (s stream) close() {
	close(s.closer)
}

func (s stream) closeWithoutSignal() {
	close(s.closer)
}

func (s stream) wait() {
	<-s.closer
}

func (s stream) sendErr(errors ...*gqlerror.Error) {
	errs := make([]error, len(errors))
	for i, err := range errors {
		errs[i] = err
	}
	b, err := json.Marshal(errs)
	if err != nil {
		panic(err)
	}

	_ = s.send(b)
}

func (s stream) sendResponse(response *graphql.Response) {
	// TODO: Ensure we can use custom encoding here.
	b, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}

	_ = s.send(b)
}

func (s stream) send(payload []byte) error {
	return s.pipe.Publish(s.streamChannel, payload)
}
