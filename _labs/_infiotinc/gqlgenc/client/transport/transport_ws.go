package transport

// Original work from https://github.com/hasura/go-graphql-client/blob/0806e5ec7/subscription.go
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"io"
	"nhooyr.io/websocket"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type OperationMessageType string

const (
	// GQL_CONNECTION_INIT the Client sends this message after plain websocket connection to start the communication with the server
	GQL_CONNECTION_INIT OperationMessageType = "connection_init"
	// GQL_CONNECTION_ERROR The server may responses with this message to the GQL_CONNECTION_INIT from client, indicates the server rejected the connection.
	GQL_CONNECTION_ERROR OperationMessageType = "conn_err"
	// GQL_START Client sends this message to execute GraphQL operation
	GQL_START OperationMessageType = "start"
	// GQL_STOP Client sends this message in order to stop a running GraphQL operation execution (for example: unsubscribe)
	GQL_STOP OperationMessageType = "stop"
	// GQL_ERROR Server sends this message upon a failing operation, before the GraphQL execution, usually due to GraphQL validation errors (resolver errors are part of GQL_DATA message, and will be added as errors array)
	GQL_ERROR OperationMessageType = "error"
	// GQL_DATA The server sends this message to transfter the GraphQL execution result from the server to the client, this message is a response for GQL_START message.
	GQL_DATA OperationMessageType = "data"
	// GQL_COMPLETE Server sends this message to indicate that a GraphQL operation is done, and no more data will arrive for the specific operation.
	GQL_COMPLETE OperationMessageType = "complete"
	// GQL_CONNECTION_KEEP_ALIVE Server message that should be sent right after each GQL_CONNECTION_ACK processed and then periodically to keep the client connection alive.
	// The client starts to consider the keep alive message only upon the first received keep alive message from the server.
	GQL_CONNECTION_KEEP_ALIVE OperationMessageType = "ka"
	// GQL_CONNECTION_ACK The server may responses with this message to the GQL_CONNECTION_INIT from client, indicates the server accepted the connection. May optionally include a payload.
	GQL_CONNECTION_ACK OperationMessageType = "connection_ack"
	// GQL_CONNECTION_TERMINATE the Client sends this message to terminate the connection.
	GQL_CONNECTION_TERMINATE OperationMessageType = "connection_terminate"

	// GQL_UNKNOWN is an Unknown operation type, for logging only
	GQL_UNKNOWN OperationMessageType = "unknown"
	// GQL_INTERNAL is the Internal status, for logging only
	GQL_INTERNAL OperationMessageType = "internal"
)

type WebsocketConn interface {
	ReadJSON(v interface{}) error
	WriteJSON(v interface{}) error
	Close() error
	// SetReadLimit sets the maximum size in bytes for a message read from the peer. If a
	// message exceeds the limit, the connection sends a close message to the peer
	// and returns ErrReadLimit to the application.
	SetReadLimit(limit int64)
}

type OperationMessage struct {
	ID      string               `json:"id,omitempty"`
	Type    OperationMessageType `json:"type"`
	Payload json.RawMessage      `json:"payload,omitempty"`
}

func (msg OperationMessage) String() string {
	return fmt.Sprintf("%v %v %s", msg.ID, msg.Type, msg.Payload)
}

type ConnOptions struct {
	Context context.Context
	URL     string
	Timeout time.Duration
}

type wsResponse struct {
	*ChanResponse
	Context          context.Context
	OperationRequest OperationRequest
	started          bool
}

type WebsocketConnProvider func(ctx context.Context, URL string) (WebsocketConn, error)

type Status int

const (
	StatusDisconnected Status = iota
	StatusConnected
	StatusReady
)

func (s Status) String() string {
	switch s {
	case StatusDisconnected:
		return "disconnected"
	case StatusConnected:
		return "connected"
	case StatusReady:
		return "ready"
	}

	panic("unknown status")
}

// Ws transports GQL queries over websocket
// Start() must be called to initiate the websocket connection
// Close() must be called to dispose of the transport
type Ws struct {
	URL string
	// WebsocketConnProvider defaults to DefaultWebsocketConnProvider(30 * time.Second)
	WebsocketConnProvider WebsocketConnProvider
	// ConnectionParams will be sent during the connection init
	ConnectionParams interface{}

	cancel  context.CancelFunc
	conn    WebsocketConn
	running bool
	status  Status
	sc      *sync.Cond

	ops  map[string]*wsResponse
	opsm sync.Mutex

	o     sync.Once
	errCh chan error
	i     uint64
	rm    sync.Mutex
	log   bool
}

func (t *Ws) sendErr(err error) {
	select {
	case t.errCh <- err: // Attempt to write err
	default:
	}
}

func (t *Ws) init() {
	t.o.Do(func() {
		t.ops = make(map[string]*wsResponse)

		t.sc = sync.NewCond(&sync.Mutex{})

		t.conn = &closedWs{}

		if t.WebsocketConnProvider == nil {
			t.WebsocketConnProvider = DefaultWebsocketConnProvider(30 * time.Second)
		}

		t.log, _ = strconv.ParseBool(os.Getenv("GQLGENC_WS_LOG"))
	})
}

func (t *Ws) WaitFor(st Status, s time.Duration) {
	t.init()

	t.printLog(GQL_INTERNAL, "WAIT FOR", st)

	for {
		t.waitFor(st)

		time.Sleep(s)

		// After timeout, is it still in the expected status ?
		if t.status == st {
			break
		}
	}

	t.printLog(GQL_INTERNAL, "DONE WAIT FOR", st)
}

func (t *Ws) waitFor(s Status) {
	t.init()

	t.sc.L.Lock()
	defer t.sc.L.Unlock()
	for t.status != s {
		t.sc.Wait()
	}
}

func (t *Ws) setRunning(v bool) {
	t.printLog(GQL_INTERNAL, "SET ISRUNNING", v)
	t.running = v
	if v == false {
		t.setStatus(StatusDisconnected)
	} else {
		t.sc.Broadcast()
	}
}

func (t *Ws) setStatus(s Status) {
	if t.status == s {
		return
	}

	t.printLog(GQL_INTERNAL, "SET STATUS", s)
	t.status = s
	t.sc.Broadcast()
}

func (t *Ws) Start(ctx context.Context) <-chan error {
	t.init()

	if t.running {
		panic("transport is already running")
	}

	t.errCh = make(chan error)

	go t.run(ctx)

	return t.errCh
}

func (t *Ws) readJson(v interface{}) error {
	return t.conn.ReadJSON(v)
}

func (t *Ws) writeJson(v interface{}) error {
	return t.conn.WriteJSON(v)
}

func (t *Ws) run(inctx context.Context) {
	defer func() {
		t.setRunning(false)
		close(t.errCh)
	}()

	t.setRunning(true)

	ctx := inctx
	for {
		//t.printLog(GQL_INTERNAL, "STATUS", t.status)

		select {
		case <-inctx.Done():
			err := inctx.Err()
			t.printLog(GQL_INTERNAL, "CTX DONE", err)
			t.sendErr(err)
			return
		default:
			// continue...
		}

		if t.status == StatusDisconnected {
			t.printLog(GQL_INTERNAL, "CANCEL PREV CTX")

			if t.cancel != nil {
				t.cancel()
			}
			ctx, t.cancel = context.WithCancel(inctx)

			t.printLog(GQL_INTERNAL, "CONNECTING")
			conn, err := t.WebsocketConnProvider(ctx, t.URL)
			if err != nil {
				t.printLog(GQL_INTERNAL, "WebsocketConnProvider ERR", err)
				t.ResetWithErr(err)
				time.Sleep(time.Second)
				continue
			}
			t.printLog(GQL_INTERNAL, "HAS CONN")
			t.conn = conn
			t.setStatus(StatusConnected)

			err = t.sendConnectionInit()
			if err != nil {
				t.printLog(GQL_INTERNAL, "sendConnectionInit ERR", err)
				t.ResetWithErr(err)
				time.Sleep(time.Second)
				continue
			}

			t.printLog(GQL_INTERNAL, "CONNECTED")
		}

		var message OperationMessage
		if err := t.readJson(&message); err != nil {
			// Is expected as part of conn.ReadJSON timeout, we have not received a message or
			// a KA, the connection is probably dead... RIP
			if errors.Is(err, context.DeadlineExceeded) {
				t.printLog(GQL_INTERNAL, "READ DEADLINE EXCEEDED")
				t.ResetWithErr(err)
				continue
			}

			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF") {
				t.printLog(GQL_INTERNAL, "EOF")
				t.ResetWithErr(err)
				continue
			}

			closeStatus := websocket.CloseStatus(err)
			if closeStatus == websocket.StatusNormalClosure {
				t.printLog(GQL_INTERNAL, "NORMAL CLOSURE")
				// close event from websocket client, exiting...
				return
			}

			t.printLog(GQL_INTERNAL, "READ JSON ERR", err)
			t.ResetWithErr(err)
			continue
		}

		switch message.Type {
		case GQL_CONNECTION_ACK:
			t.printLog(GQL_CONNECTION_ACK, message)
			t.setStatus(StatusReady)

			t.opsm.Lock()
			for id, op := range t.ops {
				if err := t.startOp(id, op); err != nil {
					t.printLog(GQL_INTERNAL, "ACK: START OP FAILED")
					_ = t.cancelOp(id)
					t.sendErr(err)
				}
			}
			t.opsm.Unlock()
		case GQL_CONNECTION_KEEP_ALIVE:
			t.printLog(GQL_CONNECTION_KEEP_ALIVE, message)
		case GQL_CONNECTION_ERROR:
			t.printLog(GQL_CONNECTION_ERROR, message)
			t.setStatus(StatusDisconnected)
			t.ResetWithErr(fmt.Errorf("gql conn error: %v", message))
		case GQL_COMPLETE:
			t.printLog(GQL_COMPLETE, message)
			_ = t.cancelOp(message.ID)
		case GQL_ERROR:
			t.printLog(GQL_ERROR, message)
			fallthrough
		case GQL_DATA:
			t.printLog(GQL_DATA, message)

			id := message.ID
			t.opsm.Lock()
			op, ok := t.ops[id]
			if !ok {
				continue
			}
			t.opsm.Unlock()

			var out OperationResponse
			err := json.Unmarshal(message.Payload, &out)
			if err != nil {
				out.Errors = append(out.Errors, gqlerror.WrapPath(nil, err))
			}
			op.Send(out)
		default:
			t.printLog(GQL_UNKNOWN, message)
		}
	}
}

func (t *Ws) sendConnectionInit() error {
	var bParams []byte = nil
	if t.ConnectionParams != nil {
		var err error
		bParams, err = json.Marshal(t.ConnectionParams)
		if err != nil {
			return err
		}
	}

	msg := OperationMessage{
		Type:    GQL_CONNECTION_INIT,
		Payload: bParams,
	}

	t.printLog(GQL_CONNECTION_INIT, msg)
	return t.writeJson(msg)
}

func (t *Ws) Reset() {
	t.init()

	t.ResetWithErr(nil)
}

func (t *Ws) ResetWithErr(err error) {
	t.init()

	t.rm.Lock()
	defer t.rm.Unlock()

	if t.status == StatusDisconnected {
		return
	}

	t.setStatus(StatusDisconnected)

	t.printLog(GQL_INTERNAL, "RESET", err)

	if err != nil {
		t.sendErr(err)
	}

	for id, op := range t.ops {
		if op.started {
			_ = t.stopOp(id)
			op.started = false
		}
	}

	_ = t.closeConn()

	atomic.StoreUint64(&t.i, 0)
}

func (t *Ws) terminate() error {
	msg := OperationMessage{
		Type: GQL_CONNECTION_TERMINATE,
	}

	t.printLog(GQL_CONNECTION_TERMINATE, msg)
	return t.writeJson(msg)
}

func (t *Ws) closeConn() error {
	_ = t.terminate()
	err := t.conn.Close()
	t.conn = &closedWs{}
	t.cancel()

	t.printLog(GQL_INTERNAL, "DONE CLOSE CONN", err)

	return err
}

func (t *Ws) Close() error {
	t.init()

	t.printLog(GQL_INTERNAL, "CLOSE")

	for id := range t.ops {
		_ = t.cancelOp(id)
	}

	return t.closeConn()
}

func (t *Ws) Request(req Request) Response {
	t.init()

	t.printLog(GQL_INTERNAL, "REQ")

	id := fmt.Sprintf("%v", atomic.AddUint64(&t.i, 1))

	res := &wsResponse{
		Context:          req.Context,
		OperationRequest: NewOperationRequestFromRequest(req),
		ChanResponse: NewChanResponse(
			func() error {
				t.printLog(GQL_INTERNAL, "CLOSE RES")
				return t.cancelOp(id)
			},
		),
	}

	t.printLog(GQL_INTERNAL, "ADD TO OPS")
	t.opsm.Lock()
	t.ops[id] = res
	t.opsm.Unlock()

	if t.status == StatusReady {
		err := t.startOp(id, res)
		if err != nil {
			return NewErrorResponse(err)
		}
	}

	return res
}

func (t *Ws) printLog(typ OperationMessageType, rest ...interface{}) {
	if t.log {
		fmt.Printf("# %-20v: ", typ)
		fmt.Println(rest...)
	}
}

func (t *Ws) registerOp(id string, op *wsResponse) {
	t.opsm.Lock()
	defer t.opsm.Unlock()

	t.ops[id] = op
}

func (t *Ws) startOp(id string, op *wsResponse) error {
	if op.started {
		return nil
	}

	t.printLog(GQL_INTERNAL, "START OP")

	payload, err := json.Marshal(op.OperationRequest)
	if err != nil {
		return err
	}

	msg := OperationMessage{
		ID:      id,
		Type:    GQL_START,
		Payload: payload,
	}

	t.printLog(GQL_START, msg)
	if err := t.writeJson(msg); err != nil {
		t.printLog(GQL_INTERNAL, "GQL_START ERR", err)
		return err
	}

	op.started = true

	return nil
}

func (t *Ws) stopOp(id string) error {
	t.printLog(GQL_INTERNAL, "STOP OP", id)

	msg := OperationMessage{
		ID:   id,
		Type: GQL_STOP,
	}

	t.printLog(GQL_STOP, msg)
	return t.writeJson(msg)
}

func (t *Ws) cancelOp(id string) error {
	t.printLog(GQL_INTERNAL, "CANCEL OP", id)

	t.opsm.Lock()
	op, ok := t.ops[id]
	if !ok {
		t.opsm.Unlock()
		return nil
	}
	delete(t.ops, id)
	t.opsm.Unlock()

	op.CloseCh()

	return t.stopOp(id)
}

func (t *Ws) GetConn() WebsocketConn {
	return t.conn
}
