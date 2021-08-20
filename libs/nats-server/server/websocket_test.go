// Copyright 2020 The NATS Authors
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
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

type testReader struct {
	buf []byte
	pos int
	max int
	err error
}

func (tr *testReader) Read(p []byte) (int, error) {
	if tr.err != nil {
		return 0, tr.err
	}
	n := len(tr.buf) - tr.pos
	if n == 0 {
		return 0, nil
	}
	if n > cap(p) {
		n = cap(p)
	}
	if tr.max > 0 && n > tr.max {
		n = tr.max
	}
	copy(p, tr.buf[tr.pos:tr.pos+n])
	tr.pos += n
	return n, nil
}

func TestWSGet(t *testing.T) {
	rb := []byte("012345")

	tr := &testReader{buf: []byte("6789")}

	for _, test := range []struct {
		name   string
		pos    int
		needed int
		newpos int
		trmax  int
		result string
		reterr bool
	}{
		{"fromrb1", 0, 3, 3, 4, "012", false},    // Partial from read buffer
		{"fromrb2", 3, 2, 5, 4, "34", false},     // Partial from read buffer
		{"fromrb3", 5, 1, 6, 4, "5", false},      // Partial from read buffer
		{"fromtr1", 4, 4, 6, 4, "4567", false},   // Partial from read buffer + some of ioReader
		{"fromtr2", 4, 6, 6, 4, "456789", false}, // Partial from read buffer + all of ioReader
		{"fromtr3", 4, 6, 6, 2, "456789", false}, // Partial from read buffer + all of ioReader with several reads
		{"fromtr4", 4, 6, 6, 2, "", true},        // ioReader returns error
	} {
		t.Run(test.name, func(t *testing.T) {
			tr.pos = 0
			tr.max = test.trmax
			if test.reterr {
				tr.err = fmt.Errorf("on purpose")
			}
			res, np, err := wsGet(tr, rb, test.pos, test.needed)
			if test.reterr {
				if err == nil {
					t.Fatalf("Expected error, got none")
				}
				if err.Error() != "on purpose" {
					t.Fatalf("Unexpected error: %v", err)
				}
				if np != 0 || res != nil {
					t.Fatalf("Unexpected returned values: res=%v n=%v", res, np)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error on get: %v", err)
			}
			if np != test.newpos {
				t.Fatalf("Expected pos=%v, got %v", test.newpos, np)
			}
			if string(res) != test.result {
				t.Fatalf("Invalid returned content: %s", res)
			}
		})
	}
}

func TestWSIsControlFrame(t *testing.T) {
	for _, test := range []struct {
		name      string
		code      wsOpCode
		isControl bool
	}{
		{"binary", wsBinaryMessage, false},
		{"text", wsTextMessage, false},
		{"ping", wsPingMessage, true},
		{"pong", wsPongMessage, true},
		{"close", wsCloseMessage, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if res := wsIsControlFrame(test.code); res != test.isControl {
				t.Fatalf("Expected %q isControl to be %v, got %v", test.name, test.isControl, res)
			}
		})
	}
}

func testWSSimpleMask(key, buf []byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] ^= key[i&3]
	}
}

func TestWSUnmask(t *testing.T) {
	key := []byte{1, 2, 3, 4}
	orgBuf := []byte("this is a clear text")

	mask := func() []byte {
		t.Helper()
		buf := append([]byte(nil), orgBuf...)
		testWSSimpleMask(key, buf)
		// First ensure that the content is masked.
		if bytes.Equal(buf, orgBuf) {
			t.Fatalf("Masking did not do anything: %q", buf)
		}
		return buf
	}

	ri := &wsReadInfo{mask: true}
	ri.init()
	copy(ri.mkey[:], key)

	buf := mask()
	// Unmask in one call
	ri.unmask(buf)
	if !bytes.Equal(buf, orgBuf) {
		t.Fatalf("Unmask error, expected %q, got %q", orgBuf, buf)
	}

	// Unmask in multiple calls
	buf = mask()
	ri.mkpos = 0
	ri.unmask(buf[:3])
	ri.unmask(buf[3:11])
	ri.unmask(buf[11:])
	if !bytes.Equal(buf, orgBuf) {
		t.Fatalf("Unmask error, expected %q, got %q", orgBuf, buf)
	}
}

func TestWSCreateCloseMessage(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		psize     int
		truncated bool
	}{
		{"fits", wsCloseStatusInternalSrvError, 10, false},
		{"truncated", wsCloseStatusProtocolError, wsMaxControlPayloadSize + 10, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := make([]byte, test.psize)
			for i := 0; i < len(payload); i++ {
				payload[i] = byte('A' + (i % 26))
			}
			res := wsCreateCloseMessage(test.status, string(payload))
			if status := binary.BigEndian.Uint16(res[:2]); int(status) != test.status {
				t.Fatalf("Expected status to be %v, got %v", test.status, status)
			}
			psize := len(res) - 2
			if !test.truncated {
				if int(psize) != test.psize {
					t.Fatalf("Expected size to be %v, got %v", test.psize, psize)
				}
				if !bytes.Equal(res[2:], payload) {
					t.Fatalf("Unexpected result: %q", res[2:])
				}
				return
			}
			// Since the payload of a close message contains a 2 byte status, the
			// actual max text size will be wsMaxControlPayloadSize-2
			if int(psize) != wsMaxControlPayloadSize-2 {
				t.Fatalf("Expected size to be capped to %v, got %v", wsMaxControlPayloadSize-2, psize)
			}
			if string(res[len(res)-3:]) != "..." {
				t.Fatalf("Expected res to have `...` at the end, got %q", res[4:])
			}
		})
	}
}

func TestWSCreateFrameHeader(t *testing.T) {
	for _, test := range []struct {
		name       string
		frameType  wsOpCode
		compressed bool
		len        int
	}{
		{"uncompressed 10", wsBinaryMessage, false, 10},
		{"uncompressed 600", wsTextMessage, false, 600},
		{"uncompressed 100000", wsTextMessage, false, 100000},
		{"compressed 10", wsBinaryMessage, true, 10},
		{"compressed 600", wsBinaryMessage, true, 600},
		{"compressed 100000", wsTextMessage, true, 100000},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, _ := wsCreateFrameHeader(false, test.compressed, test.frameType, test.len)
			// The server is always sending the message has a single frame,
			// so the "final" bit should be set.
			expected := byte(test.frameType) | wsFinalBit
			if test.compressed {
				expected |= wsRsv1Bit
			}
			if b := res[0]; b != expected {
				t.Fatalf("Expected first byte to be %v, got %v", expected, b)
			}
			switch {
			case test.len <= 125:
				if len(res) != 2 {
					t.Fatalf("Frame len should be 2, got %v", len(res))
				}
				if res[1] != byte(test.len) {
					t.Fatalf("Expected len to be in second byte and be %v, got %v", test.len, res[1])
				}
			case test.len < 65536:
				// 1+1+2
				if len(res) != 4 {
					t.Fatalf("Frame len should be 4, got %v", len(res))
				}
				if res[1] != 126 {
					t.Fatalf("Second byte value should be 126, got %v", res[1])
				}
				if rl := binary.BigEndian.Uint16(res[2:]); int(rl) != test.len {
					t.Fatalf("Expected len to be %v, got %v", test.len, rl)
				}
			default:
				// 1+1+8
				if len(res) != 10 {
					t.Fatalf("Frame len should be 10, got %v", len(res))
				}
				if res[1] != 127 {
					t.Fatalf("Second byte value should be 127, got %v", res[1])
				}
				if rl := binary.BigEndian.Uint64(res[2:]); int(rl) != test.len {
					t.Fatalf("Expected len to be %v, got %v", test.len, rl)
				}
			}
		})
	}
}

func testWSCreateClientMsg(frameType wsOpCode, frameNum int, final, compressed bool, payload []byte) []byte {
	if compressed {
		buf := &bytes.Buffer{}
		compressor, _ := flate.NewWriter(buf, 1)
		compressor.Write(payload)
		compressor.Flush()
		payload = buf.Bytes()
		// The last 4 bytes are dropped
		payload = payload[:len(payload)-4]
	}
	frame := make([]byte, 14+len(payload))
	if frameNum == 1 {
		frame[0] = byte(frameType)
	}
	if final {
		frame[0] |= wsFinalBit
	}
	if compressed {
		frame[0] |= wsRsv1Bit
	}
	pos := 1
	lenPayload := len(payload)
	switch {
	case lenPayload <= 125:
		frame[pos] = byte(lenPayload) | wsMaskBit
		pos++
	case lenPayload < 65536:
		frame[pos] = 126 | wsMaskBit
		binary.BigEndian.PutUint16(frame[2:], uint16(lenPayload))
		pos += 3
	default:
		frame[1] = 127 | wsMaskBit
		binary.BigEndian.PutUint64(frame[2:], uint64(lenPayload))
		pos += 9
	}
	key := []byte{1, 2, 3, 4}
	copy(frame[pos:], key)
	pos += 4
	copy(frame[pos:], payload)
	testWSSimpleMask(key, frame[pos:])
	pos += lenPayload
	return frame[:pos]
}

func testWSSetupForRead() (*client, *wsReadInfo, *testReader) {
	ri := &wsReadInfo{mask: true}
	ri.init()
	tr := &testReader{}
	opts := DefaultOptions()
	opts.MaxPending = MAX_PENDING_SIZE
	s := &Server{opts: opts}
	c := &client{srv: s, ws: &websocket{}}
	c.initClient()
	return c, ri, tr
}

func TestWSReadUncompressedFrames(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	// Create 2 WS messages
	pl1 := []byte("first message")
	wsmsg1 := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, pl1)
	pl2 := []byte("second message")
	wsmsg2 := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, pl2)
	// Add both in single buffer
	orgrb := append([]byte(nil), wsmsg1...)
	orgrb = append(orgrb, wsmsg2...)

	rb := append([]byte(nil), orgrb...)
	bufs, err := c.wsRead(ri, tr, rb)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 2 {
		t.Fatalf("Expected 2 buffers, got %v", n)
	}
	if !bytes.Equal(bufs[0], pl1) {
		t.Fatalf("Unexpected content for buffer 1: %s", bufs[0])
	}
	if !bytes.Equal(bufs[1], pl2) {
		t.Fatalf("Unexpected content for buffer 2: %s", bufs[1])
	}

	// Now reset and try with the read buffer not containing full ws frame
	c, ri, tr = testWSSetupForRead()
	rb = append([]byte(nil), orgrb...)
	// Frame is 1+1+4+'first message'. So say we pass with rb of 11 bytes,
	// then we should get "first"
	bufs, err = c.wsRead(ri, tr, rb[:11])
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 1 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if string(bufs[0]) != "first" {
		t.Fatalf("Unexpected content: %q", bufs[0])
	}
	// Call again with more data..
	bufs, err = c.wsRead(ri, tr, rb[11:32])
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 2 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if string(bufs[0]) != " message" {
		t.Fatalf("Unexpected content: %q", bufs[0])
	}
	if string(bufs[1]) != "second " {
		t.Fatalf("Unexpected content: %q", bufs[1])
	}
	// Call with the rest
	bufs, err = c.wsRead(ri, tr, rb[32:])
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 1 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if string(bufs[0]) != "message" {
		t.Fatalf("Unexpected content: %q", bufs[0])
	}
}

func TestWSReadCompressedFrames(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	uncompressed := []byte("this is the uncompress data")
	wsmsg1 := testWSCreateClientMsg(wsBinaryMessage, 1, true, true, uncompressed)
	rb := append([]byte(nil), wsmsg1...)
	// Call with some but not all of the payload
	bufs, err := c.wsRead(ri, tr, rb[:10])
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 0 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	// Call with the rest, only then should we get the uncompressed data.
	bufs, err = c.wsRead(ri, tr, rb[10:])
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 1 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if !bytes.Equal(bufs[0], uncompressed) {
		t.Fatalf("Unexpected content: %s", bufs[0])
	}
	// Stress the fact that we use a pool and want to make sure
	// that if we get a decompressor from the pool, it is properly reset
	// with the buffer to decompress.
	// Since we unmask the read buffer, reset it now and fill it
	// with 10 compressed frames.
	rb = nil
	for i := 0; i < 10; i++ {
		rb = append(rb, wsmsg1...)
	}
	bufs, err = c.wsRead(ri, tr, rb)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 10 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}

	// Compress a message and send it in several frames.
	buf := &bytes.Buffer{}
	compressor, _ := flate.NewWriter(buf, 1)
	compressor.Write(uncompressed)
	compressor.Flush()
	compressed := buf.Bytes()
	// The last 4 bytes are dropped
	compressed = compressed[:len(compressed)-4]
	ncomp := 10
	frag1 := testWSCreateClientMsg(wsBinaryMessage, 1, false, false, compressed[:ncomp])
	frag1[0] |= wsRsv1Bit
	frag2 := testWSCreateClientMsg(wsBinaryMessage, 2, true, false, compressed[ncomp:])
	rb = append([]byte(nil), frag1...)
	rb = append(rb, frag2...)
	bufs, err = c.wsRead(ri, tr, rb)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 1 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if !bytes.Equal(bufs[0], uncompressed) {
		t.Fatalf("Unexpected content: %s", bufs[0])
	}
}

func TestWSReadCompressedFrameCorrupted(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	uncompressed := []byte("this is the uncompress data")
	wsmsg1 := testWSCreateClientMsg(wsBinaryMessage, 1, true, true, uncompressed)
	copy(wsmsg1[10:], []byte{1, 2, 3, 4})
	rb := append([]byte(nil), wsmsg1...)
	bufs, err := c.wsRead(ri, tr, rb)
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("Expected error about corrupted data, got %v", err)
	}
	if n := len(bufs); n != 0 {
		t.Fatalf("Expected no buffer, got %v", n)
	}
}

func TestWSReadVariousFrameSizes(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
	}{
		{"tiny", 100},
		{"medium", 1000},
		{"large", 70000},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, ri, tr := testWSSetupForRead()
			uncompressed := make([]byte, test.size)
			for i := 0; i < len(uncompressed); i++ {
				uncompressed[i] = 'A' + byte(i%26)
			}
			wsmsg1 := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, uncompressed)
			rb := append([]byte(nil), wsmsg1...)
			bufs, err := c.wsRead(ri, tr, rb)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if n := len(bufs); n != 1 {
				t.Fatalf("Unexpected buffer returned: %v", n)
			}
			if !bytes.Equal(bufs[0], uncompressed) {
				t.Fatalf("Unexpected content: %s", bufs[0])
			}
		})
	}
}

func TestWSReadFragmentedFrames(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	payloads := []string{"first", "second", "third"}
	var rb []byte
	for i := 0; i < len(payloads); i++ {
		final := i == len(payloads)-1
		frag := testWSCreateClientMsg(wsBinaryMessage, i+1, final, false, []byte(payloads[i]))
		rb = append(rb, frag...)
	}
	bufs, err := c.wsRead(ri, tr, rb)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 3 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	for i, expected := range payloads {
		if string(bufs[i]) != expected {
			t.Fatalf("Unexpected content for buf=%v: %s", i, bufs[i])
		}
	}
}

func TestWSReadPartialFrameHeaderAtEndOfReadBuffer(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	msg1 := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("msg1"))
	msg2 := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("msg2"))
	rb := append([]byte(nil), msg1...)
	rb = append(rb, msg2...)
	// We will pass the first frame + the first byte of the next frame.
	rbl := rb[:len(msg1)+1]
	// Make the io reader return the rest of the frame
	tr.buf = rb[len(msg1)+1:]
	bufs, err := c.wsRead(ri, tr, rbl)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 1 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	// We should not have asked to the io reader more than what is needed for reading
	// the frame header. Since we had already the first byte in the read buffer,
	// tr.pos should be 1(size)+4(key)=5
	if tr.pos != 5 {
		t.Fatalf("Expected reader pos to be 5, got %v", tr.pos)
	}
}

func TestWSReadPingFrame(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{"without payload", nil},
		{"with payload", []byte("optional payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, ri, tr := testWSSetupForRead()
			ping := testWSCreateClientMsg(wsPingMessage, 1, true, false, test.payload)
			rb := append([]byte(nil), ping...)
			bufs, err := c.wsRead(ri, tr, rb)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if n := len(bufs); n != 0 {
				t.Fatalf("Unexpected buffer returned: %v", n)
			}
			// A PONG should have been queued with the payload of the ping
			c.mu.Lock()
			nb, _ := c.collapsePtoNB()
			c.mu.Unlock()
			if n := len(nb); n == 0 {
				t.Fatalf("Expected buffers, got %v", n)
			}
			if expected := 2 + len(test.payload); expected != len(nb[0]) {
				t.Fatalf("Expected buffer to be %v bytes long, got %v", expected, len(nb[0]))
			}
			b := nb[0][0]
			if b&wsFinalBit == 0 {
				t.Fatalf("Control frame should have been the final flag, it was not set: %v", b)
			}
			if b&byte(wsPongMessage) == 0 {
				t.Fatalf("Should have been a PONG, it wasn't: %v", b)
			}
			if len(test.payload) > 0 {
				if !bytes.Equal(nb[0][2:], test.payload) {
					t.Fatalf("Unexpected content: %s", nb[0][2:])
				}
			}
		})
	}
}

func TestWSReadPongFrame(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{"without payload", nil},
		{"with payload", []byte("optional payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, ri, tr := testWSSetupForRead()
			pong := testWSCreateClientMsg(wsPongMessage, 1, true, false, test.payload)
			rb := append([]byte(nil), pong...)
			bufs, err := c.wsRead(ri, tr, rb)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if n := len(bufs); n != 0 {
				t.Fatalf("Unexpected buffer returned: %v", n)
			}
			// Nothing should be sent...
			c.mu.Lock()
			nb, _ := c.collapsePtoNB()
			c.mu.Unlock()
			if n := len(nb); n != 0 {
				t.Fatalf("Expected no buffer, got %v", n)
			}
		})
	}
}

func TestWSReadCloseFrame(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{"without payload", nil},
		{"with payload", []byte("optional payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, ri, tr := testWSSetupForRead()
			// a close message has a status in 2 bytes + optional payload
			payload := make([]byte, 2+len(test.payload))
			binary.BigEndian.PutUint16(payload[:2], wsCloseStatusNormalClosure)
			if len(test.payload) > 0 {
				copy(payload[2:], test.payload)
			}
			close := testWSCreateClientMsg(wsCloseMessage, 1, true, false, payload)
			// Have a normal frame prior to close to make sure that wsRead returns
			// the normal frame along with io.EOF to indicate that wsCloseMessage was received.
			msg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("msg"))
			rb := append([]byte(nil), msg...)
			rb = append(rb, close...)
			bufs, err := c.wsRead(ri, tr, rb)
			// It is expected that wsRead returns io.EOF on processing a close.
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			if n := len(bufs); n != 1 {
				t.Fatalf("Unexpected buffer returned: %v", n)
			}
			if string(bufs[0]) != "msg" {
				t.Fatalf("Unexpected content: %s", bufs[0])
			}
			// A CLOSE should have been queued with the payload of the original close message.
			c.mu.Lock()
			nb, _ := c.collapsePtoNB()
			c.mu.Unlock()
			if n := len(nb); n == 0 {
				t.Fatalf("Expected buffers, got %v", n)
			}
			if expected := 2 + 2 + len(test.payload); expected != len(nb[0]) {
				t.Fatalf("Expected buffer to be %v bytes long, got %v", expected, len(nb[0]))
			}
			b := nb[0][0]
			if b&wsFinalBit == 0 {
				t.Fatalf("Control frame should have been the final flag, it was not set: %v", b)
			}
			if b&byte(wsCloseMessage) == 0 {
				t.Fatalf("Should have been a CLOSE, it wasn't: %v", b)
			}
			if status := binary.BigEndian.Uint16(nb[0][2:4]); status != wsCloseStatusNormalClosure {
				t.Fatalf("Expected status to be %v, got %v", wsCloseStatusNormalClosure, status)
			}
			if len(test.payload) > 0 {
				if !bytes.Equal(nb[0][4:], test.payload) {
					t.Fatalf("Unexpected content: %s", nb[0][4:])
				}
			}
		})
	}
}

func TestWSReadControlFrameBetweebFragmentedFrames(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	frag1 := testWSCreateClientMsg(wsBinaryMessage, 1, false, false, []byte("first"))
	frag2 := testWSCreateClientMsg(wsBinaryMessage, 2, true, false, []byte("second"))
	ctrl := testWSCreateClientMsg(wsPongMessage, 1, true, false, nil)
	rb := append([]byte(nil), frag1...)
	rb = append(rb, ctrl...)
	rb = append(rb, frag2...)
	bufs, err := c.wsRead(ri, tr, rb)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 2 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	if string(bufs[0]) != "first" {
		t.Fatalf("Unexpected content: %s", bufs[0])
	}
	if string(bufs[1]) != "second" {
		t.Fatalf("Unexpected content: %s", bufs[1])
	}
}

func TestWSReadGetErrors(t *testing.T) {
	tr := &testReader{err: fmt.Errorf("on purpose")}
	for _, test := range []struct {
		lenPayload int
		rbextra    int
	}{
		{10, 1},
		{10, 3},
		{200, 1},
		{200, 2},
		{200, 5},
		{70000, 1},
		{70000, 5},
		{70000, 13},
	} {
		t.Run("", func(t *testing.T) {
			c, ri, _ := testWSSetupForRead()
			msg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("msg"))
			frame := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, make([]byte, test.lenPayload))
			rb := append([]byte(nil), msg...)
			rb = append(rb, frame...)
			bufs, err := c.wsRead(ri, tr, rb[:len(msg)+test.rbextra])
			if err == nil || err.Error() != "on purpose" {
				t.Fatalf("Expected 'on purpose' error, got %v", err)
			}
			if n := len(bufs); n != 1 {
				t.Fatalf("Unexpected buffer returned: %v", n)
			}
			if string(bufs[0]) != "msg" {
				t.Fatalf("Unexpected content: %s", bufs[0])
			}
		})
	}
}

func TestWSHandleControlFrameErrors(t *testing.T) {
	c, ri, tr := testWSSetupForRead()
	tr.err = fmt.Errorf("on purpose")

	// a close message has a status in 2 bytes + optional payload
	text := []byte("this is a close message")
	payload := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(payload[:2], wsCloseStatusNormalClosure)
	copy(payload[2:], text)
	ctrl := testWSCreateClientMsg(wsCloseMessage, 1, true, false, payload)

	bufs, err := c.wsRead(ri, tr, ctrl[:len(ctrl)-4])
	if err == nil || err.Error() != "on purpose" {
		t.Fatalf("Expected 'on purpose' error, got %v", err)
	}
	if n := len(bufs); n != 0 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}

	// Alter the content of close message. It is supposed to be valid utf-8.
	c, ri, tr = testWSSetupForRead()
	cp := append([]byte(nil), payload...)
	cp[10] = 0xF1
	ctrl = testWSCreateClientMsg(wsCloseMessage, 1, true, false, cp)
	bufs, err = c.wsRead(ri, tr, ctrl)
	// We should still receive an EOF but the message enqueued to the client
	// should contain wsCloseStatusInvalidPayloadData and the error about invalid utf8
	if err != io.EOF {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n := len(bufs); n != 0 {
		t.Fatalf("Unexpected buffer returned: %v", n)
	}
	c.mu.Lock()
	nb, _ := c.collapsePtoNB()
	c.mu.Unlock()
	if n := len(nb); n == 0 {
		t.Fatalf("Expected buffers, got %v", n)
	}
	b := nb[0][0]
	if b&wsFinalBit == 0 {
		t.Fatalf("Control frame should have been the final flag, it was not set: %v", b)
	}
	if b&byte(wsCloseMessage) == 0 {
		t.Fatalf("Should have been a CLOSE, it wasn't: %v", b)
	}
	if status := binary.BigEndian.Uint16(nb[0][2:4]); status != wsCloseStatusInvalidPayloadData {
		t.Fatalf("Expected status to be %v, got %v", wsCloseStatusInvalidPayloadData, status)
	}
	if !bytes.Contains(nb[0][4:], []byte("utf8")) {
		t.Fatalf("Unexpected content: %s", nb[0][4:])
	}
}

func TestWSReadErrors(t *testing.T) {
	for _, test := range []struct {
		cframe func() []byte
		err    string
		nbufs  int
	}{
		{
			func() []byte {
				msg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("hello"))
				msg[1] &= ^byte(wsMaskBit)
				return msg
			},
			"mask bit missing", 1,
		},
		{
			func() []byte {
				return testWSCreateClientMsg(wsPingMessage, 1, true, false, make([]byte, 200))
			},
			"control frame length bigger than maximum allowed", 1,
		},
		{
			func() []byte {
				return testWSCreateClientMsg(wsPingMessage, 1, false, false, []byte("hello"))
			},
			"control frame does not have final bit set", 1,
		},
		{
			func() []byte {
				frag1 := testWSCreateClientMsg(wsBinaryMessage, 1, false, false, []byte("frag1"))
				newMsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("new message"))
				all := append([]byte(nil), frag1...)
				all = append(all, newMsg...)
				return all
			},
			"new message started before final frame for previous message was received", 2,
		},
		{
			func() []byte {
				frame := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("frame"))
				frag := testWSCreateClientMsg(wsBinaryMessage, 2, false, false, []byte("continuation"))
				all := append([]byte(nil), frame...)
				all = append(all, frag...)
				return all
			},
			"invalid continuation frame", 2,
		},
		{
			func() []byte {
				return testWSCreateClientMsg(wsBinaryMessage, 2, false, true, []byte("frame"))
			},
			"invalid continuation frame", 1,
		},
		{
			func() []byte {
				return testWSCreateClientMsg(99, 1, false, false, []byte("hello"))
			},
			"unknown opcode", 1,
		},
	} {
		t.Run(test.err, func(t *testing.T) {
			c, ri, tr := testWSSetupForRead()
			// Add a valid message first
			msg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("hello"))
			// Then add the bad frame
			bad := test.cframe()
			// Add them both to a read buffer
			rb := append([]byte(nil), msg...)
			rb = append(rb, bad...)
			bufs, err := c.wsRead(ri, tr, rb)
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error to contain %q, got %q", test.err, err.Error())
			}
			if n := len(bufs); n != test.nbufs {
				t.Fatalf("Unexpected number of buffers: %v", n)
			}
			if string(bufs[0]) != "hello" {
				t.Fatalf("Unexpected content: %s", bufs[0])
			}
		})
	}
}

func TestWSEnqueueCloseMsg(t *testing.T) {
	for _, test := range []struct {
		reason ClosedState
		status int
	}{
		{ClientClosed, wsCloseStatusNormalClosure},
		{AuthenticationTimeout, wsCloseStatusPolicyViolation},
		{AuthenticationViolation, wsCloseStatusPolicyViolation},
		{SlowConsumerPendingBytes, wsCloseStatusPolicyViolation},
		{SlowConsumerWriteDeadline, wsCloseStatusPolicyViolation},
		{MaxAccountConnectionsExceeded, wsCloseStatusPolicyViolation},
		{MaxConnectionsExceeded, wsCloseStatusPolicyViolation},
		{MaxControlLineExceeded, wsCloseStatusPolicyViolation},
		{MaxSubscriptionsExceeded, wsCloseStatusPolicyViolation},
		{MissingAccount, wsCloseStatusPolicyViolation},
		{AuthenticationExpired, wsCloseStatusPolicyViolation},
		{Revocation, wsCloseStatusPolicyViolation},
		{TLSHandshakeError, wsCloseStatusTLSHandshake},
		{ParseError, wsCloseStatusProtocolError},
		{ProtocolViolation, wsCloseStatusProtocolError},
		{BadClientProtocolVersion, wsCloseStatusProtocolError},
		{MaxPayloadExceeded, wsCloseStatusMessageTooBig},
		{ServerShutdown, wsCloseStatusGoingAway},
		{WriteError, wsCloseStatusAbnormalClosure},
		{ReadError, wsCloseStatusAbnormalClosure},
		{StaleConnection, wsCloseStatusAbnormalClosure},
		{ClosedState(254), wsCloseStatusInternalSrvError},
	} {
		t.Run(test.reason.String(), func(t *testing.T) {
			c, _, _ := testWSSetupForRead()
			c.wsEnqueueCloseMessage(test.reason)
			c.mu.Lock()
			nb, _ := c.collapsePtoNB()
			c.mu.Unlock()
			if n := len(nb); n != 1 {
				t.Fatalf("Expected 1 buffer, got %v", n)
			}
			b := nb[0][0]
			if b&wsFinalBit == 0 {
				t.Fatalf("Control frame should have been the final flag, it was not set: %v", b)
			}
			if b&byte(wsCloseMessage) == 0 {
				t.Fatalf("Should have been a CLOSE, it wasn't: %v", b)
			}
			if status := binary.BigEndian.Uint16(nb[0][2:4]); int(status) != test.status {
				t.Fatalf("Expected status to be %v, got %v", test.status, status)
			}
			if string(nb[0][4:]) != test.reason.String() {
				t.Fatalf("Unexpected content: %s", nb[0][4:])
			}
		})
	}
}

type testResponseWriter struct {
	http.ResponseWriter
	buf     bytes.Buffer
	headers http.Header
	err     error
	brw     *bufio.ReadWriter
	conn    *testWSFakeNetConn
}

func (trw *testResponseWriter) Write(p []byte) (int, error) {
	return trw.buf.Write(p)
}

func (trw *testResponseWriter) WriteHeader(status int) {
	trw.buf.WriteString(fmt.Sprintf("%v", status))
}

func (trw *testResponseWriter) Header() http.Header {
	if trw.headers == nil {
		trw.headers = make(http.Header)
	}
	return trw.headers
}

type testWSFakeNetConn struct {
	net.Conn
	wbuf            bytes.Buffer
	err             error
	wsOpened        bool
	isClosed        bool
	deadlineCleared bool
}

func (c *testWSFakeNetConn) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	return c.wbuf.Write(p)
}

func (c *testWSFakeNetConn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		c.deadlineCleared = true
	}
	return nil
}

func (c *testWSFakeNetConn) Close() error {
	c.isClosed = true
	return nil
}

func (trw *testResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if trw.conn == nil {
		trw.conn = &testWSFakeNetConn{}
	}
	trw.conn.wsOpened = true
	if trw.brw == nil {
		trw.brw = bufio.NewReadWriter(bufio.NewReader(trw.conn), bufio.NewWriter(trw.conn))
	}
	return trw.conn, trw.brw, trw.err
}

func testWSOptions() *Options {
	opts := DefaultOptions()
	opts.DisableShortFirstPing = true
	opts.Websocket.Host = "127.0.0.1"
	opts.Websocket.Port = -1
	opts.NoSystemAccount = true
	var err error
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
	}
	opts.Websocket.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		panic(err)
	}
	return opts
}

func testWSCreateValidReq() *http.Request {
	req := &http.Request{
		Method: "GET",
		Host:   "localhost",
		Proto:  "HTTP/1.1",
	}
	req.Header = make(http.Header)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-Websocket-Version", "13")
	return req
}

func TestWSCheckOrigin(t *testing.T) {
	notSameOrigin := false
	sameOrigin := true
	allowedListEmpty := []string{}
	someList := []string{"http://host1.com", "http://host2.com:1234"}

	for _, test := range []struct {
		name       string
		sameOrigin bool
		origins    []string
		reqHost    string
		reqTLS     bool
		origin     string
		err        string
	}{
		{"any", notSameOrigin, allowedListEmpty, "", false, "http://any.host.com", ""},
		{"same origin ok", sameOrigin, allowedListEmpty, "host.com", false, "http://host.com:80", ""},
		{"same origin bad host", sameOrigin, allowedListEmpty, "host.com", false, "http://other.host.com", "not same origin"},
		{"same origin bad port", sameOrigin, allowedListEmpty, "host.com", false, "http://host.com:81", "not same origin"},
		{"same origin bad scheme", sameOrigin, allowedListEmpty, "host.com", true, "http://host.com", "not same origin"},
		{"same origin bad uri", sameOrigin, allowedListEmpty, "host.com", false, "@@@://invalid:url:1234", "invalid URI"},
		{"same origin bad url", sameOrigin, allowedListEmpty, "host.com", false, "http://invalid:url:1234", "too many colons"},
		{"same origin bad req host", sameOrigin, allowedListEmpty, "invalid:url:1234", false, "http://host.com", "too many colons"},
		{"no origin same origin ignored", sameOrigin, allowedListEmpty, "", false, "", ""},
		{"no origin list ignored", sameOrigin, someList, "", false, "", ""},
		{"no origin same origin and list ignored", sameOrigin, someList, "", false, "", ""},
		{"allowed from list", notSameOrigin, someList, "", false, "http://host2.com:1234", ""},
		{"allowed with different path", notSameOrigin, someList, "", false, "http://host1.com/some/path", ""},
		{"list bad port", notSameOrigin, someList, "", false, "http://host1.com:1234", "not in the allowed list"},
		{"list bad scheme", notSameOrigin, someList, "", false, "https://host2.com:1234", "not in the allowed list"},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.Websocket.SameOrigin = test.sameOrigin
			opts.Websocket.AllowedOrigins = test.origins
			s := &Server{opts: opts}
			s.wsSetOriginOptions(&opts.Websocket)

			req := testWSCreateValidReq()
			req.Host = test.reqHost
			if test.reqTLS {
				req.TLS = &tls.ConnectionState{}
			}
			if test.origin != "" {
				req.Header.Set("Origin", test.origin)
			}
			err := s.websocket.checkOrigin(req)
			if test.err == "" && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if test.err != "" && (err == nil || !strings.Contains(err.Error(), test.err)) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestWSUpgradeValidationErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		setup  func() (*Options, *testResponseWriter, *http.Request)
		err    string
		status int
	}{
		{
			"bad method",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Method = "POST"
				return opts, nil, req
			},
			"must be GET",
			http.StatusMethodNotAllowed,
		},
		{
			"no host",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Host = ""
				return opts, nil, req
			},
			"'Host' missing in request",
			http.StatusBadRequest,
		},
		{
			"invalid upgrade header",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Del("Upgrade")
				return opts, nil, req
			},
			"invalid value for header 'Upgrade'",
			http.StatusBadRequest,
		},
		{
			"invalid connection header",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Del("Connection")
				return opts, nil, req
			},
			"invalid value for header 'Connection'",
			http.StatusBadRequest,
		},
		{
			"no key",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Del("Sec-Websocket-Key")
				return opts, nil, req
			},
			"key missing",
			http.StatusBadRequest,
		},
		{
			"empty key",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Set("Sec-Websocket-Key", "")
				return opts, nil, req
			},
			"key missing",
			http.StatusBadRequest,
		},
		{
			"missing version",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Del("Sec-Websocket-Version")
				return opts, nil, req
			},
			"invalid version",
			http.StatusBadRequest,
		},
		{
			"wrong version",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				req := testWSCreateValidReq()
				req.Header.Set("Sec-Websocket-Version", "99")
				return opts, nil, req
			},
			"invalid version",
			http.StatusBadRequest,
		},
		{
			"origin",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				opts.Websocket.SameOrigin = true
				req := testWSCreateValidReq()
				req.Header.Set("Origin", "http://bad.host.com")
				return opts, nil, req
			},
			"origin not allowed",
			http.StatusForbidden,
		},
		{
			"hijack error",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				rw := &testResponseWriter{err: fmt.Errorf("on purpose")}
				req := testWSCreateValidReq()
				return opts, rw, req
			},
			"on purpose",
			http.StatusInternalServerError,
		},
		{
			"hijack buffered data",
			func() (*Options, *testResponseWriter, *http.Request) {
				opts := testWSOptions()
				buf := &bytes.Buffer{}
				buf.WriteString("some data")
				rw := &testResponseWriter{
					conn: &testWSFakeNetConn{},
					brw:  bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(nil)),
				}
				tmp := [1]byte{}
				io.ReadAtLeast(rw.brw, tmp[:1], 1)
				req := testWSCreateValidReq()
				return opts, rw, req
			},
			"client sent data before handshake is complete",
			http.StatusBadRequest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts, rw, req := test.setup()
			if rw == nil {
				rw = &testResponseWriter{}
			}
			s := &Server{opts: opts}
			s.wsSetOriginOptions(&opts.Websocket)
			res, err := s.wsUpgrade(rw, req)
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Should get error %q, got %v", test.err, err)
			}
			if res != nil {
				t.Fatalf("Should not have returned a result, got %v", res)
			}
			expected := fmt.Sprintf("%v%s\n", test.status, http.StatusText(test.status))
			if got := rw.buf.String(); got != expected {
				t.Fatalf("Expected %q got %q", expected, got)
			}
			// Check that if the connection was opened, it is now closed.
			if rw.conn != nil && rw.conn.wsOpened && !rw.conn.isClosed {
				t.Fatal("Connection was opened, but has not been closed")
			}
		})
	}
}

func TestWSUpgradeResponseWriteError(t *testing.T) {
	opts := testWSOptions()
	s := &Server{opts: opts}
	expectedErr := errors.New("on purpose")
	rw := &testResponseWriter{
		conn: &testWSFakeNetConn{err: expectedErr},
	}
	req := testWSCreateValidReq()
	res, err := s.wsUpgrade(rw, req)
	if err != expectedErr {
		t.Fatalf("Should get error %q, got %v", expectedErr.Error(), err)
	}
	if res != nil {
		t.Fatalf("Should not have returned a result, got %v", res)
	}
	if !rw.conn.isClosed {
		t.Fatal("Connection should have been closed")
	}
}

func TestWSUpgradeConnDeadline(t *testing.T) {
	opts := testWSOptions()
	opts.Websocket.HandshakeTimeout = time.Second
	s := &Server{opts: opts}
	rw := &testResponseWriter{}
	req := testWSCreateValidReq()
	res, err := s.wsUpgrade(rw, req)
	if res == nil || err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rw.conn.isClosed {
		t.Fatal("Connection should NOT have been closed")
	}
	if !rw.conn.deadlineCleared {
		t.Fatal("Connection deadline should have been cleared after handshake")
	}
}

func TestWSCompressNegotiation(t *testing.T) {
	// No compression on the server, but client asks
	opts := testWSOptions()
	s := &Server{opts: opts}
	rw := &testResponseWriter{}
	req := testWSCreateValidReq()
	req.Header.Set("Sec-Websocket-Extensions", "permessage-deflate")
	res, err := s.wsUpgrade(rw, req)
	if res == nil || err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// The http response should not contain "permessage-deflate"
	output := rw.conn.wbuf.String()
	if strings.Contains(output, "permessage-deflate") {
		t.Fatalf("Compression disabled in server so response to client should not contain extension, got %s", output)
	}

	// Option in the server and client, so compression should be negotiated.
	s.opts.Websocket.Compression = true
	rw = &testResponseWriter{}
	res, err = s.wsUpgrade(rw, req)
	if res == nil || err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// The http response should not contain "permessage-deflate"
	output = rw.conn.wbuf.String()
	if !strings.Contains(output, "permessage-deflate") {
		t.Fatalf("Compression in server and client request, so response should contain extension, got %s", output)
	}

	// Option in server but not asked by the client, so response should not contain "permessage-deflate"
	rw = &testResponseWriter{}
	req.Header.Del("Sec-Websocket-Extensions")
	res, err = s.wsUpgrade(rw, req)
	if res == nil || err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// The http response should not contain "permessage-deflate"
	output = rw.conn.wbuf.String()
	if strings.Contains(output, "permessage-deflate") {
		t.Fatalf("Compression in server but not in client, so response to client should not contain extension, got %s", output)
	}
}

func TestWSParseOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		content  string
		checkOpt func(*WebsocketOpts) error
		err      string
	}{
		// Negative tests
		{"bad type", "websocket: []", nil, "to be a map"},
		{"bad listen", "websocket: { listen: [] }", nil, "port or host:port"},
		{"bad port", `websocket: { port: "abc" }`, nil, "not int64"},
		{"bad host", `websocket: { host: 123 }`, nil, "not string"},
		{"bad advertise type", `websocket: { advertise: 123 }`, nil, "not string"},
		{"bad tls", `websocket: { tls: 123 }`, nil, "not map[string]interface {}"},
		{"bad same origin", `websocket: { same_origin: "abc" }`, nil, "not bool"},
		{"bad allowed origins type", `websocket: { allowed_origins: {} }`, nil, "unsupported type"},
		{"bad allowed origins values", `websocket: { allowed_origins: [ {} ] }`, nil, "unsupported type in array"},
		{"bad handshake timeout type", `websocket: { handshake_timeout: [] }`, nil, "unsupported type"},
		{"bad handshake timeout duration", `websocket: { handshake_timeout: "abc" }`, nil, "invalid duration"},
		{"unknown field", `websocket: { this_does_not_exist: 123 }`, nil, "unknown"},
		// Positive tests
		{"listen port only", `websocket { listen: 1234 }`, func(wo *WebsocketOpts) error {
			if wo.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", wo.Port)
			}
			return nil
		}, ""},
		{"listen host and port", `websocket { listen: "localhost:1234" }`, func(wo *WebsocketOpts) error {
			if wo.Host != "localhost" || wo.Port != 1234 {
				return fmt.Errorf("expected localhost:1234, got %v:%v", wo.Host, wo.Port)
			}
			return nil
		}, ""},
		{"host", `websocket { host: "localhost" }`, func(wo *WebsocketOpts) error {
			if wo.Host != "localhost" {
				return fmt.Errorf("expected localhost, got %v", wo.Host)
			}
			return nil
		}, ""},
		{"port", `websocket { port: 1234 }`, func(wo *WebsocketOpts) error {
			if wo.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", wo.Port)
			}
			return nil
		}, ""},
		{"advertise", `websocket { advertise: "host:1234" }`, func(wo *WebsocketOpts) error {
			if wo.Advertise != "host:1234" {
				return fmt.Errorf("expected %q, got %q", "host:1234", wo.Advertise)
			}
			return nil
		}, ""},
		{"same origin", `websocket { same_origin: true }`, func(wo *WebsocketOpts) error {
			if !wo.SameOrigin {
				return fmt.Errorf("expected same_origin==true, got %v", wo.SameOrigin)
			}
			return nil
		}, ""},
		{"allowed origins one only", `websocket { allowed_origins: "https://host.com/" }`, func(wo *WebsocketOpts) error {
			expected := []string{"https://host.com/"}
			if !reflect.DeepEqual(wo.AllowedOrigins, expected) {
				return fmt.Errorf("expected allowed origins to be %q, got %q", expected, wo.AllowedOrigins)
			}
			return nil
		}, ""},
		{"allowed origins array",
			`
			websocket {
				allowed_origins: [
					"https://host1.com/"
					"https://host2.com/"
				]
			}
			`, func(wo *WebsocketOpts) error {
				expected := []string{"https://host1.com/", "https://host2.com/"}
				if !reflect.DeepEqual(wo.AllowedOrigins, expected) {
					return fmt.Errorf("expected allowed origins to be %q, got %q", expected, wo.AllowedOrigins)
				}
				return nil
			}, ""},
		{"handshake timeout in whole seconds", `websocket { handshake_timeout: 3 }`, func(wo *WebsocketOpts) error {
			if wo.HandshakeTimeout != 3*time.Second {
				return fmt.Errorf("expected handshake to be 3s, got %v", wo.HandshakeTimeout)
			}
			return nil
		}, ""},
		{"handshake timeout n duration", `websocket { handshake_timeout: "4s" }`, func(wo *WebsocketOpts) error {
			if wo.HandshakeTimeout != 4*time.Second {
				return fmt.Errorf("expected handshake to be 4s, got %v", wo.HandshakeTimeout)
			}
			return nil
		}, ""},
		{"tls config",
			`
			websocket {
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/key.pem"
				}
			}
			`, func(wo *WebsocketOpts) error {
				if wo.TLSConfig == nil {
					return fmt.Errorf("TLSConfig should have been set")
				}
				return nil
			}, ""},
		{"compression",
			`
			websocket {
				compression: true
			}
			`, func(wo *WebsocketOpts) error {
				if !wo.Compression {
					return fmt.Errorf("Compression should have been set")
				}
				return nil
			}, ""},
		{"jwt cookie",
			`
			websocket {
				jwt_cookie: "jwtcookie"
			}
			`, func(wo *WebsocketOpts) error {
				if wo.JWTCookie != "jwtcookie" {
					return fmt.Errorf("Invalid JWTCookie value: %q", wo.JWTCookie)
				}
				return nil
			}, ""},
		{"no auth user",
			`
			websocket {
				no_auth_user: "noauthuser"
			}
			`, func(wo *WebsocketOpts) error {
				if wo.NoAuthUser != "noauthuser" {
					return fmt.Errorf("Invalid NoAuthUser value: %q", wo.NoAuthUser)
				}
				return nil
			}, ""},
		{"auth block",
			`
			websocket {
				authorization {
					user: "webuser"
					password: "pwd"
					token: "token"
					timeout: 2.0
				}
			}
			`, func(wo *WebsocketOpts) error {
				if wo.Username != "webuser" || wo.Password != "pwd" || wo.Token != "token" || wo.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth block: %+v", wo)
				}
				return nil
			}, ""},
		{"auth timeout as int",
			`
			websocket {
				authorization {
					timeout: 2
				}
			}
			`, func(wo *WebsocketOpts) error {
				if wo.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth timeout: %v", wo.AuthTimeout)
				}
				return nil
			}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.content))
			defer removeFile(t, conf)
			o, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("For content: %q, expected error about %q, got %v", test.content, test.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error for content %q: %v", test.content, err)
			}
			if err := test.checkOpt(&o.Websocket); err != nil {
				t.Fatalf("Incorrect option for content %q: %v", test.content, err.Error())
			}
		})
	}
}

func TestWSValidateOptions(t *testing.T) {
	nwso := DefaultOptions()
	wso := testWSOptions()
	for _, test := range []struct {
		name    string
		getOpts func() *Options
		err     string
	}{
		{"websocket disabled", func() *Options { return nwso.Clone() }, ""},
		{"no tls", func() *Options { o := wso.Clone(); o.Websocket.TLSConfig = nil; return o }, "requires TLS configuration"},
		{"bad url in allowed list", func() *Options {
			o := wso.Clone()
			o.Websocket.AllowedOrigins = []string{"http://this:is:bad:url"}
			return o
		}, "unable to parse"},
		{"missing trusted configuration", func() *Options {
			o := wso.Clone()
			o.Websocket.JWTCookie = "jwt"
			return o
		}, "keys configuration is required"},
		{"websocket username not allowed if users specified", func() *Options {
			o := wso.Clone()
			o.Nkeys = []*NkeyUser{{Nkey: "abc"}}
			o.Websocket.Username = "b"
			o.Websocket.Password = "pwd"
			return o
		}, "websocket authentication username not compatible with presence of users/nkeys"},
		{"websocket token not allowed if users specified", func() *Options {
			o := wso.Clone()
			o.Nkeys = []*NkeyUser{{Nkey: "abc"}}
			o.Websocket.Token = "mytoken"
			return o
		}, "websocket authentication token not compatible with presence of users/nkeys"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateWebsocketOptions(test.getOpts())
			if test.err == "" && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if test.err != "" && (err == nil || !strings.Contains(err.Error(), test.err)) {
				t.Fatalf("Expected error to contain %q, got %v", test.err, err)
			}
		})
	}
}

func TestWSSetOriginOptions(t *testing.T) {
	o := testWSOptions()
	for _, test := range []struct {
		content string
		err     string
	}{
		{"@@@://host.com/", "invalid URI"},
		{"http://this:is:bad:url/", "invalid port"},
	} {
		t.Run(test.err, func(t *testing.T) {
			o.Websocket.AllowedOrigins = []string{test.content}
			s := &Server{}
			l := &captureErrorLogger{errCh: make(chan string, 1)}
			s.SetLogger(l, false, false)
			s.wsSetOriginOptions(&o.Websocket)
			select {
			case e := <-l.errCh:
				if !strings.Contains(e, test.err) {
					t.Fatalf("Unexpected error: %v", e)
				}
			case <-time.After(50 * time.Millisecond):
				t.Fatalf("Did not get the error")
			}

		})
	}
}

type captureFatalLogger struct {
	DummyLogger
	fatalCh chan string
}

func (l *captureFatalLogger) Fatalf(format string, v ...interface{}) {
	select {
	case l.fatalCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func TestWSFailureToStartServer(t *testing.T) {
	// Create a listener to use a port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error listening: %v", err)
	}
	defer l.Close()

	o := testWSOptions()
	// Make sure we don't have unnecessary listen ports opened.
	o.HTTPPort = 0
	o.Cluster.Port = 0
	o.Gateway.Name = ""
	o.Gateway.Port = 0
	o.LeafNode.Port = 0
	o.Websocket.Port = l.Addr().(*net.TCPAddr).Port
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s.Shutdown()
	logger := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s.SetLogger(logger, false, false)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s.Start()
		wg.Done()
	}()

	select {
	case e := <-logger.fatalCh:
		if !strings.Contains(e, "Unable to listen") {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Should have reported a fatal error")
	}
	// Since this is a test and the process does not actually
	// exit on Fatal error, wait for the client port to be
	// ready so when we shutdown we don't leave the accept
	// loop hanging.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		s.mu.Lock()
		ready := s.listener != nil
		s.mu.Unlock()
		if !ready {
			return fmt.Errorf("client accept loop not started yet")
		}
		return nil
	})
	s.Shutdown()
	wg.Wait()
}

func TestWSAbnormalFailureOfWebServer(t *testing.T) {
	o := testWSOptions()
	s := RunServer(o)
	defer s.Shutdown()
	logger := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s.SetLogger(logger, false, false)

	// Now close the WS listener to cause a WebServer error
	s.mu.Lock()
	s.websocket.listener.Close()
	s.mu.Unlock()

	select {
	case e := <-logger.fatalCh:
		if !strings.Contains(e, "websocket listener error") {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Should have reported a fatal error")
	}
}

type testWSClientOptions struct {
	compress, web bool
	host          string
	port          int
	extraHeaders  map[string]string
	noTLS         bool
}

func testNewWSClient(t testing.TB, o testWSClientOptions) (net.Conn, *bufio.Reader, []byte) {
	t.Helper()
	addr := fmt.Sprintf("%s:%d", o.host, o.port)
	wsc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	if !o.noTLS {
		wsc = tls.Client(wsc, &tls.Config{InsecureSkipVerify: true})
		if err := wsc.(*tls.Conn).Handshake(); err != nil {
			t.Fatalf("Error during handshake: %v", err)
		}
	}
	req := testWSCreateValidReq()
	if o.compress {
		req.Header.Set("Sec-Websocket-Extensions", "permessage-deflate")
	}
	if o.web {
		req.Header.Set("User-Agent", "Mozilla/5.0")
	}
	if o.extraHeaders != nil {
		for hdr, val := range o.extraHeaders {
			req.Header.Add(hdr, val)
		}
	}
	req.URL, _ = url.Parse("wss://" + addr)
	if err := req.Write(wsc); err != nil {
		t.Fatalf("Error sending request: %v", err)
	}
	br := bufio.NewReader(wsc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected response status %v, got %v", http.StatusSwitchingProtocols, resp.StatusCode)
	}
	// Wait for the INFO
	info := testWSReadFrame(t, br)
	if !bytes.HasPrefix(info, []byte("INFO {")) {
		t.Fatalf("Expected INFO, got %s", info)
	}
	return wsc, br, info
}

type testClaimsOptions struct {
	nac            *jwt.AccountClaims
	nuc            *jwt.UserClaims
	connectRequest interface{}
	dontSign       bool
	expectAnswer   string
}

func testWSWithClaims(t *testing.T, s *Server, o testWSClientOptions, tclm testClaimsOptions) (kp nkeys.KeyPair, conn net.Conn, rdr *bufio.Reader, auth_was_required bool) {
	t.Helper()

	okp, _ := nkeys.FromSeed(oSeed)

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	if tclm.nac == nil {
		tclm.nac = jwt.NewAccountClaims(apub)
	} else {
		tclm.nac.Subject = apub
	}
	ajwt, err := tclm.nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	if tclm.nuc == nil {
		tclm.nuc = jwt.NewUserClaims(pub)
	} else {
		tclm.nuc.Subject = pub
	}
	jwt, err := tclm.nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	addAccountToMemResolver(s, apub, ajwt)

	c, cr, l := testNewWSClient(t, o)

	var info struct {
		Nonce        string `json:"nonce,omitempty"`
		AuthRequired bool   `json:"auth_required,omitempty"`
	}

	if err := json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatal(err)
	}
	if info.AuthRequired {
		cs := ""
		if tclm.connectRequest != nil {
			customReq, err := json.Marshal(tclm.connectRequest)
			if err != nil {
				t.Fatal(err)
			}
			// PING needed to flush the +OK/-ERR to us.
			cs = fmt.Sprintf("CONNECT %v\r\nPING\r\n", string(customReq))
		} else if !tclm.dontSign {
			// Sign Nonce
			sigraw, _ := nkp.Sign([]byte(info.Nonce))
			sig := base64.RawURLEncoding.EncodeToString(sigraw)
			cs = fmt.Sprintf("CONNECT {\"jwt\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt, sig)
		} else {
			cs = fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt)
		}
		wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(cs))
		c.Write(wsmsg)
		l = testWSReadFrame(t, cr)
		if !strings.HasPrefix(string(l), tclm.expectAnswer) {
			t.Fatalf("Expected %q, got %q", tclm.expectAnswer, l)
		}
	}
	return akp, c, cr, info.AuthRequired
}

func setupAddTrusted(o *Options) {
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	o.TrustedKeys = []string{pub}
}

func setupAddCookie(o *Options) {
	o.Websocket.JWTCookie = "jwt"
}

func testWSCreateClientGetInfo(t testing.TB, compress, web bool, host string, port int) (net.Conn, *bufio.Reader, []byte) {
	t.Helper()
	return testNewWSClient(t, testWSClientOptions{
		compress: compress,
		web:      web,
		host:     host,
		port:     port,
	})
}

func testWSCreateClient(t testing.TB, compress, web bool, host string, port int) (net.Conn, *bufio.Reader) {
	wsc, br, _ := testWSCreateClientGetInfo(t, compress, web, host, port)
	// Send CONNECT and PING
	wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, compress, []byte("CONNECT {\"verbose\":false,\"protocol\":1}\r\nPING\r\n"))
	if _, err := wsc.Write(wsmsg); err != nil {
		t.Fatalf("Error sending message: %v", err)
	}
	// Wait for the PONG
	if msg := testWSReadFrame(t, br); !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
		t.Fatalf("Expected PONG, got %s", msg)
	}
	return wsc, br
}

func testWSReadFrame(t testing.TB, br *bufio.Reader) []byte {
	t.Helper()
	fh := [2]byte{}
	if _, err := io.ReadAtLeast(br, fh[:2], 2); err != nil {
		t.Fatalf("Error reading frame: %v", err)
	}
	fc := fh[0]&wsRsv1Bit != 0
	sb := fh[1]
	size := 0
	switch {
	case sb <= 125:
		size = int(sb)
	case sb == 126:
		tmp := [2]byte{}
		if _, err := io.ReadAtLeast(br, tmp[:2], 2); err != nil {
			t.Fatalf("Error reading frame: %v", err)
		}
		size = int(binary.BigEndian.Uint16(tmp[:2]))
	case sb == 127:
		tmp := [8]byte{}
		if _, err := io.ReadAtLeast(br, tmp[:8], 8); err != nil {
			t.Fatalf("Error reading frame: %v", err)
		}
		size = int(binary.BigEndian.Uint64(tmp[:8]))
	}
	buf := make([]byte, size)
	if _, err := io.ReadAtLeast(br, buf, size); err != nil {
		t.Fatalf("Error reading frame: %v", err)
	}
	if !fc {
		return buf
	}
	buf = append(buf, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff)
	dbr := bytes.NewBuffer(buf)
	d := flate.NewReader(dbr)
	uncompressed, err := ioutil.ReadAll(d)
	if err != nil {
		t.Fatalf("Error reading frame: %v", err)
	}
	return uncompressed
}

func TestWSPubSub(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression bool
	}{
		{"no compression", false},
		{"compression", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testWSOptions()
			if test.compression {
				o.Websocket.Compression = true
			}
			s := RunServer(o)
			defer s.Shutdown()

			// Create a regular client to subscribe
			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()
			nsub := natsSubSync(t, nc, "foo")
			checkExpectedSubs(t, 1, s)

			// Now create a WS client and send a message on "foo"
			wsc, br := testWSCreateClient(t, test.compression, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			// Send a WS message for "PUB foo 2\r\nok\r\n"
			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("PUB foo 7\r\nfrom ws\r\n"))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}

			// Now check that message is received
			msg := natsNexMsg(t, nsub, time.Second)
			if string(msg.Data) != "from ws" {
				t.Fatalf("Expected message to be %q, got %q", "ok", string(msg.Data))
			}

			// Now do reverse, create a subscription on WS client on bar
			wsmsg = testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("SUB bar 1\r\n"))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending subscription: %v", err)
			}
			// Wait for it to be registered on server
			checkExpectedSubs(t, 2, s)
			// Now publish from NATS connection and verify received on WS client
			natsPub(t, nc, "bar", []byte("from nats"))
			natsFlush(t, nc)

			// Check for the "from nats" message...
			// Set some deadline so we are not stuck forever on failure
			wsc.SetReadDeadline(time.Now().Add(10 * time.Second))
			ok := 0
			for {
				line, _, err := br.ReadLine()
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}
				// Note that this works even in compression test because those
				// texts are likely not to be compressed, but compression code is
				// still executed.
				if ok == 0 && bytes.Contains(line, []byte("MSG bar 1 9")) {
					ok = 1
					continue
				} else if ok == 1 && bytes.Contains(line, []byte("from nats")) {
					ok = 2
					break
				}
			}
		})
	}
}

func TestWSTLSConnection(t *testing.T) {
	o := testWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("%s:%d", o.Websocket.Host, o.Websocket.Port)

	for _, test := range []struct {
		name   string
		useTLS bool
		status int
	}{
		{"client uses TLS", true, http.StatusSwitchingProtocols},
		{"client does not use TLS", false, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			wsc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Error creating ws connection: %v", err)
			}
			defer wsc.Close()
			if test.useTLS {
				wsc = tls.Client(wsc, &tls.Config{InsecureSkipVerify: true})
				if err := wsc.(*tls.Conn).Handshake(); err != nil {
					t.Fatalf("Error during handshake: %v", err)
				}
			}
			req := testWSCreateValidReq()
			var scheme string
			if test.useTLS {
				scheme = "s"
			}
			req.URL, _ = url.Parse("ws" + scheme + "://" + addr)
			if err := req.Write(wsc); err != nil {
				t.Fatalf("Error sending request: %v", err)
			}
			br := bufio.NewReader(wsc)
			resp, err := http.ReadResponse(br, req)
			if err != nil {
				t.Fatalf("Error reading response: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != test.status {
				t.Fatalf("Expected status %v, got %v", test.status, resp.StatusCode)
			}
		})
	}
}

func TestWSTLSVerifyClientCert(t *testing.T) {
	o := testWSOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Verify:   true,
	}
	tlsc, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error creating tls config: %v", err)
	}
	o.Websocket.TLSConfig = tlsc
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("%s:%d", o.Websocket.Host, o.Websocket.Port)

	for _, test := range []struct {
		name        string
		provideCert bool
	}{
		{"client provides cert", true},
		{"client does not provide cert", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			wsc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Error creating ws connection: %v", err)
			}
			defer wsc.Close()
			tlsc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/client-cert.pem",
					KeyFile:  "../test/configs/certs/client-key.pem",
				}
				var err error
				tlsc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlsc.InsecureSkipVerify = true
			wsc = tls.Client(wsc, tlsc)
			if err := wsc.(*tls.Conn).Handshake(); err != nil {
				t.Fatalf("Error during handshake: %v", err)
			}
			req := testWSCreateValidReq()
			req.URL, _ = url.Parse("wss://" + addr)
			if err := req.Write(wsc); err != nil {
				t.Fatalf("Error sending request: %v", err)
			}
			br := bufio.NewReader(wsc)
			resp, err := http.ReadResponse(br, req)
			if resp != nil {
				resp.Body.Close()
			}
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("Expected status %v, got %v", http.StatusSwitchingProtocols, resp.StatusCode)
			}
		})
	}
}

func testCreateAllowedConnectionTypes(list []string) map[string]struct{} {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(list))
	for _, l := range list {
		m[l] = struct{}{}
	}
	return m
}

func TestWSTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		filtering   bool
		provideCert bool
	}{
		{"no filtering, client provides cert", false, true},
		{"no filtering, client does not provide cert", false, false},
		{"filtering, client provides cert", true, true},
		{"filtering, client does not provide cert", true, false},
		{"no users override, client provides cert", false, true},
		{"no users override, client does not provide cert", false, false},
		{"users override, client provides cert", true, true},
		{"users override, client does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testWSOptions()
			o.Accounts = []*Account{acc}
			o.Users = users
			if test.filtering {
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket})
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.Websocket.TLSConfig = tlsc
			o.Websocket.TLSMap = true
			s := RunServer(o)
			defer s.Shutdown()

			addr := fmt.Sprintf("%s:%d", o.Websocket.Host, o.Websocket.Port)
			wsc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Error creating ws connection: %v", err)
			}
			defer wsc.Close()
			tlscc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				tlscc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlscc.InsecureSkipVerify = true
			wsc = tls.Client(wsc, tlscc)
			if err := wsc.(*tls.Conn).Handshake(); err != nil {
				t.Fatalf("Error during handshake: %v", err)
			}
			req := testWSCreateValidReq()
			req.URL, _ = url.Parse("wss://" + addr)
			if err := req.Write(wsc); err != nil {
				t.Fatalf("Error sending request: %v", err)
			}
			br := bufio.NewReader(wsc)
			resp, err := http.ReadResponse(br, req)
			if resp != nil {
				resp.Body.Close()
			}
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("Expected status %v, got %v", http.StatusSwitchingProtocols, resp.StatusCode)
			}
			// Wait for the INFO
			l := testWSReadFrame(t, br)
			if !bytes.HasPrefix(l, []byte("INFO {")) {
				t.Fatalf("Expected INFO, got %s", l)
			}
			var info serverInfo
			if err := json.Unmarshal(l[5:], &info); err != nil {
				t.Fatalf("Unable to unmarshal info: %v", err)
			}
			// Send CONNECT and PING
			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("CONNECT {\"verbose\":false,\"protocol\":1}\r\nPING\r\n"))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			// Wait for the PONG
			if msg := testWSReadFrame(t, br); !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Expected PONG, got %s", msg)
			}

			var uname string
			var accname string
			c := s.getClient(info.CID)
			if c != nil {
				c.mu.Lock()
				uname = c.opts.Username
				if c.acc != nil {
					accname = c.acc.GetName()
				}
				c.mu.Unlock()
			}
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

func TestWSHandshakeTimeout(t *testing.T) {
	o := testWSOptions()
	o.Websocket.HandshakeTimeout = time.Millisecond
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
	}
	o.Websocket.TLSConfig, _ = GenTLSConfig(tc)
	s := RunServer(o)
	defer s.Shutdown()

	logger := &captureErrorLogger{errCh: make(chan string, 1)}
	s.SetLogger(logger, false, false)

	addr := fmt.Sprintf("%s:%d", o.Websocket.Host, o.Websocket.Port)
	wsc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer wsc.Close()

	// Delay the handshake
	wsc = tls.Client(wsc, &tls.Config{InsecureSkipVerify: true})
	time.Sleep(20 * time.Millisecond)
	// We expect error since the server should have cut us off
	if err := wsc.(*tls.Conn).Handshake(); err == nil {
		t.Fatal("Expected error during handshake")
	}

	// Check that server logs error
	select {
	case e := <-logger.errCh:
		if !strings.Contains(e, "timeout") {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Should have timed-out")
	}
}

func TestWSServerReportUpgradeFailure(t *testing.T) {
	o := testWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	logger := &captureErrorLogger{errCh: make(chan string, 1)}
	s.SetLogger(logger, false, false)

	addr := fmt.Sprintf("%s:%d", o.Websocket.Host, o.Websocket.Port)
	req := testWSCreateValidReq()
	req.URL, _ = url.Parse("wss://" + addr)

	wsc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer wsc.Close()
	wsc = tls.Client(wsc, &tls.Config{InsecureSkipVerify: true})
	if err := wsc.(*tls.Conn).Handshake(); err != nil {
		t.Fatalf("Error during handshake: %v", err)
	}
	// Remove a required field from the request to have it fail
	req.Header.Del("Connection")
	// Send the request
	if err := req.Write(wsc); err != nil {
		t.Fatalf("Error sending request: %v", err)
	}
	br := bufio.NewReader(wsc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected status %v, got %v", http.StatusBadRequest, resp.StatusCode)
	}

	// Check that server logs error
	select {
	case e := <-logger.errCh:
		if !strings.Contains(e, "invalid value for header 'Connection'") {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Should have timed-out")
	}
}

func TestWSCloseMsgSendOnConnectionClose(t *testing.T) {
	o := testWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	wsc, br := testWSCreateClient(t, false, false, o.Websocket.Host, o.Websocket.Port)
	defer wsc.Close()

	checkClientsCount(t, s, 1)
	var c *client
	s.mu.Lock()
	for _, cli := range s.clients {
		c = cli
		break
	}
	s.mu.Unlock()

	c.closeConnection(ProtocolViolation)
	msg := testWSReadFrame(t, br)
	if len(msg) < 2 {
		t.Fatalf("Should have 2 bytes to represent the status, got %v", msg)
	}
	if sc := int(binary.BigEndian.Uint16(msg[:2])); sc != wsCloseStatusProtocolError {
		t.Fatalf("Expected status to be %v, got %v", wsCloseStatusProtocolError, sc)
	}
	expectedPayload := ProtocolViolation.String()
	if p := string(msg[2:]); p != expectedPayload {
		t.Fatalf("Expected payload to be %q, got %q", expectedPayload, p)
	}
}

func TestWSAdvertise(t *testing.T) {
	o := testWSOptions()
	o.Cluster.Port = 0
	o.HTTPPort = 0
	o.Websocket.Advertise = "xxx:host:yyy"
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer s.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s.SetLogger(l, false, false)
	go s.Start()
	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, "Unable to get websocket connect URLs") {
			t.Fatalf("Unexpected error: %q", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have failed to start")
	}
	s.Shutdown()

	o1 := testWSOptions()
	o1.Websocket.Advertise = "host1:1234"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	wsc, br := testWSCreateClient(t, false, false, o1.Websocket.Host, o1.Websocket.Port)
	defer wsc.Close()

	o2 := testWSOptions()
	o2.Websocket.Advertise = "host2:5678"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", o1.Cluster.Host, o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkInfo := func(expected []string) {
		t.Helper()
		infob := testWSReadFrame(t, br)
		info := &Info{}
		json.Unmarshal(infob[5:], info)
		if n := len(info.ClientConnectURLs); n != len(expected) {
			t.Fatalf("Unexpected info: %+v", info)
		}
		good := 0
		for _, u := range info.ClientConnectURLs {
			for _, eu := range expected {
				if u == eu {
					good++
				}
			}
		}
		if good != len(expected) {
			t.Fatalf("Unexpected connect urls: %q", info.ClientConnectURLs)
		}
	}
	checkInfo([]string{"host1:1234", "host2:5678"})

	// Now shutdown s2 and expect another INFO
	s2.Shutdown()
	checkInfo([]string{"host1:1234"})

	// Restart with another advertise and check that it gets updated
	o2.Websocket.Advertise = "host3:9012"
	s2 = RunServer(o2)
	defer s2.Shutdown()
	checkInfo([]string{"host1:1234", "host3:9012"})
}

func TestWSFrameOutbound(t *testing.T) {
	for _, test := range []struct {
		name         string
		maskingWrite bool
	}{
		{"no write masking", false},
		{"write masking", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _, _ := testWSSetupForRead()
			c.ws.maskwrite = test.maskingWrite

			getKey := func(buf []byte) []byte {
				return buf[len(buf)-4:]
			}

			var bufs net.Buffers
			bufs = append(bufs, []byte("this "))
			bufs = append(bufs, []byte("is "))
			bufs = append(bufs, []byte("a "))
			bufs = append(bufs, []byte("set "))
			bufs = append(bufs, []byte("of "))
			bufs = append(bufs, []byte("buffers"))
			en := 2
			for _, b := range bufs {
				en += len(b)
			}
			if test.maskingWrite {
				en += 4
			}
			c.mu.Lock()
			c.out.nb = bufs
			res, n := c.collapsePtoNB()
			c.mu.Unlock()
			if n != int64(en) {
				t.Fatalf("Expected size to be %v, got %v", en, n)
			}
			if eb := 1 + len(bufs); eb != len(res) {
				t.Fatalf("Expected %v buffers, got %v", eb, len(res))
			}
			var ob []byte
			for i := 1; i < len(res); i++ {
				ob = append(ob, res[i]...)
			}
			if test.maskingWrite {
				wsMaskBuf(getKey(res[0]), ob)
			}
			if !bytes.Equal(ob, []byte("this is a set of buffers")) {
				t.Fatalf("Unexpected outbound: %q", ob)
			}

			bufs = nil
			c.out.pb = 0
			c.ws.fs = 0
			c.ws.frames = nil
			c.ws.browser = true
			bufs = append(bufs, []byte("some smaller "))
			bufs = append(bufs, []byte("buffers"))
			bufs = append(bufs, make([]byte, wsFrameSizeForBrowsers+10))
			bufs = append(bufs, []byte("then some more"))
			en = 2 + len(bufs[0]) + len(bufs[1])
			en += 4 + len(bufs[2]) - 10
			en += 2 + len(bufs[3]) + 10
			c.mu.Lock()
			c.out.nb = bufs
			res, n = c.collapsePtoNB()
			c.mu.Unlock()
			if test.maskingWrite {
				en += 3 * 4
			}
			if n != int64(en) {
				t.Fatalf("Expected size to be %v, got %v", en, n)
			}
			if len(res) != 8 {
				t.Fatalf("Unexpected number of outbound buffers: %v", len(res))
			}
			if len(res[4]) != wsFrameSizeForBrowsers {
				t.Fatalf("Big frame should have been limited to %v, got %v", wsFrameSizeForBrowsers, len(res[4]))
			}
			if len(res[6]) != 10 {
				t.Fatalf("Frame 6 should have the partial of 10 bytes, got %v", len(res[6]))
			}
			if test.maskingWrite {
				b := &bytes.Buffer{}
				key := getKey(res[0])
				b.Write(res[1])
				b.Write(res[2])
				ud := b.Bytes()
				wsMaskBuf(key, ud)
				if string(ud) != "some smaller buffers" {
					t.Fatalf("Unexpected result: %q", ud)
				}

				b.Reset()
				key = getKey(res[3])
				b.Write(res[4])
				ud = b.Bytes()
				wsMaskBuf(key, ud)
				for i := 0; i < len(ud); i++ {
					if ud[i] != 0 {
						t.Fatalf("Unexpected result: %v", ud)
					}
				}

				b.Reset()
				key = getKey(res[5])
				b.Write(res[6])
				b.Write(res[7])
				ud = b.Bytes()
				wsMaskBuf(key, ud)
				for i := 0; i < len(ud[:10]); i++ {
					if ud[i] != 0 {
						t.Fatalf("Unexpected result: %v", ud[:10])
					}
				}
				if string(ud[10:]) != "then some more" {
					t.Fatalf("Unexpected result: %q", ud[10:])
				}
			}

			bufs = nil
			c.out.pb = 0
			c.ws.fs = 0
			c.ws.frames = nil
			c.ws.browser = true
			bufs = append(bufs, []byte("some smaller "))
			bufs = append(bufs, []byte("buffers"))
			// Have one of the exact max size
			bufs = append(bufs, make([]byte, wsFrameSizeForBrowsers))
			bufs = append(bufs, []byte("then some more"))
			en = 2 + len(bufs[0]) + len(bufs[1])
			en += 4 + len(bufs[2])
			en += 2 + len(bufs[3])
			c.mu.Lock()
			c.out.nb = bufs
			res, n = c.collapsePtoNB()
			c.mu.Unlock()
			if test.maskingWrite {
				en += 3 * 4
			}
			if n != int64(en) {
				t.Fatalf("Expected size to be %v, got %v", en, n)
			}
			if len(res) != 7 {
				t.Fatalf("Unexpected number of outbound buffers: %v", len(res))
			}
			if len(res[4]) != wsFrameSizeForBrowsers {
				t.Fatalf("Big frame should have been limited to %v, got %v", wsFrameSizeForBrowsers, len(res[4]))
			}
			if test.maskingWrite {
				key := getKey(res[5])
				wsMaskBuf(key, res[6])
			}
			if string(res[6]) != "then some more" {
				t.Fatalf("Frame 6 incorrect: %q", res[6])
			}

			bufs = nil
			c.out.pb = 0
			c.ws.fs = 0
			c.ws.frames = nil
			c.ws.browser = true
			bufs = append(bufs, []byte("some smaller "))
			bufs = append(bufs, []byte("buffers"))
			// Have one of the exact max size, and last in the list
			bufs = append(bufs, make([]byte, wsFrameSizeForBrowsers))
			en = 2 + len(bufs[0]) + len(bufs[1])
			en += 4 + len(bufs[2])
			c.mu.Lock()
			c.out.nb = bufs
			res, n = c.collapsePtoNB()
			c.mu.Unlock()
			if test.maskingWrite {
				en += 2 * 4
			}
			if n != int64(en) {
				t.Fatalf("Expected size to be %v, got %v", en, n)
			}
			if len(res) != 5 {
				t.Fatalf("Unexpected number of outbound buffers: %v", len(res))
			}
			if len(res[4]) != wsFrameSizeForBrowsers {
				t.Fatalf("Big frame should have been limited to %v, got %v", wsFrameSizeForBrowsers, len(res[4]))
			}

			bufs = nil
			c.out.pb = 0
			c.ws.fs = 0
			c.ws.frames = nil
			c.ws.browser = true
			bufs = append(bufs, []byte("some smaller buffer"))
			bufs = append(bufs, make([]byte, wsFrameSizeForBrowsers-5))
			bufs = append(bufs, []byte("then some more"))
			en = 2 + len(bufs[0])
			en += 4 + len(bufs[1])
			en += 2 + len(bufs[2])
			c.mu.Lock()
			c.out.nb = bufs
			res, n = c.collapsePtoNB()
			c.mu.Unlock()
			if test.maskingWrite {
				en += 3 * 4
			}
			if n != int64(en) {
				t.Fatalf("Expected size to be %v, got %v", en, n)
			}
			if len(res) != 6 {
				t.Fatalf("Unexpected number of outbound buffers: %v", len(res))
			}
			if len(res[3]) != wsFrameSizeForBrowsers-5 {
				t.Fatalf("Big frame should have been limited to %v, got %v", wsFrameSizeForBrowsers, len(res[4]))
			}
			if test.maskingWrite {
				key := getKey(res[4])
				wsMaskBuf(key, res[5])
			}
			if string(res[5]) != "then some more" {
				t.Fatalf("Frame 6 incorrect %q", res[5])
			}

			bufs = nil
			c.out.pb = 0
			c.ws.fs = 0
			c.ws.frames = nil
			c.ws.browser = true
			bufs = append(bufs, make([]byte, wsFrameSizeForBrowsers+100))
			c.mu.Lock()
			c.out.nb = bufs
			res, _ = c.collapsePtoNB()
			c.mu.Unlock()
			if len(res) != 4 {
				t.Fatalf("Unexpected number of frames: %v", len(res))
			}
		})
	}
}

func TestWSWebrowserClient(t *testing.T) {
	o := testWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	wsc, br := testWSCreateClient(t, false, true, o.Websocket.Host, o.Websocket.Port)
	defer wsc.Close()

	checkClientsCount(t, s, 1)
	var c *client
	s.mu.Lock()
	for _, cli := range s.clients {
		c = cli
		break
	}
	s.mu.Unlock()

	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("SUB foo 1\r\nPING\r\n"))
	wsc.Write(proto)
	if res := testWSReadFrame(t, br); !bytes.Equal(res, []byte(pongProto)) {
		t.Fatalf("Expected PONG back")
	}

	c.mu.Lock()
	ok := c.isWebsocket() && c.ws.browser == true
	c.mu.Unlock()
	if !ok {
		t.Fatalf("Client is not marked as webrowser client")
	}

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Send a big message and check that it is received in smaller frames
	psize := 204813
	nc.Publish("foo", make([]byte, psize))
	nc.Flush()

	rsize := psize + len(fmt.Sprintf("MSG foo %d\r\n\r\n", psize))
	nframes := 0
	for total := 0; total < rsize; nframes++ {
		res := testWSReadFrame(t, br)
		total += len(res)
	}
	if expected := psize / wsFrameSizeForBrowsers; expected > nframes {
		t.Fatalf("Expected %v frames, got %v", expected, nframes)
	}
}

type testWSWrappedConn struct {
	net.Conn
	mu      sync.RWMutex
	buf     *bytes.Buffer
	partial bool
}

func (wc *testWSWrappedConn) Write(p []byte) (int, error) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	var err error
	n := len(p)
	if wc.partial && n > 10 {
		n = 10
		err = io.ErrShortWrite
	}
	p = p[:n]
	wc.buf.Write(p)
	wc.Conn.Write(p)
	return n, err
}

func TestWSCompressionBasic(t *testing.T) {
	payload := "This is the content of a message that will be compresseddddddddddddddddddddd."
	msgProto := fmt.Sprintf("MSG foo 1 %d\r\n%s\r\n", len(payload), payload)

	cbuf := &bytes.Buffer{}
	compressor, _ := flate.NewWriter(cbuf, flate.BestSpeed)
	compressor.Write([]byte(msgProto))
	compressor.Close()
	compressed := cbuf.Bytes()
	// The last 4 bytes are dropped
	compressed = compressed[:len(compressed)-4]

	o := testWSOptions()
	o.Websocket.Compression = true
	s := RunServer(o)
	defer s.Shutdown()

	c, br := testWSCreateClient(t, true, false, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()

	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, true, []byte("SUB foo 1\r\nPING\r\n"))
	c.Write(proto)
	l := testWSReadFrame(t, br)
	if !bytes.Equal(l, []byte(pongProto)) {
		t.Fatalf("Expected PONG, got %q", l)
	}

	var wc *testWSWrappedConn
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		wc = &testWSWrappedConn{Conn: c.nc, buf: &bytes.Buffer{}}
		c.nc = wc
		c.mu.Unlock()
	}
	s.mu.Unlock()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	natsPub(t, nc, "foo", []byte(payload))

	res := &bytes.Buffer{}
	for total := 0; total < len(msgProto); {
		l := testWSReadFrame(t, br)
		n, _ := res.Write(l)
		total += n
	}
	if !bytes.Equal([]byte(msgProto), res.Bytes()) {
		t.Fatalf("Unexpected result: %q", res)
	}

	// Now check the wrapped connection buffer to check that data was actually compressed.
	wc.mu.RLock()
	res = wc.buf
	wc.mu.RUnlock()
	if bytes.Contains(res.Bytes(), []byte(payload)) {
		t.Fatalf("Looks like frame was not compressed: %q", res.Bytes())
	}
	header := res.Bytes()[:2]
	body := res.Bytes()[2:]
	expectedB0 := byte(wsBinaryMessage) | wsFinalBit | wsRsv1Bit
	expectedPS := len(compressed)
	expectedB1 := byte(expectedPS)

	if b := header[0]; b != expectedB0 {
		t.Fatalf("Expected first byte to be %v, got %v", expectedB0, b)
	}
	if b := header[1]; b != expectedB1 {
		t.Fatalf("Expected second byte to be %v, got %v", expectedB1, b)
	}
	if len(body) != expectedPS {
		t.Fatalf("Expected payload length to be %v, got %v", expectedPS, len(body))
	}
	if !bytes.Equal(body, compressed) {
		t.Fatalf("Unexpected compress body: %q", body)
	}
}

func TestWSCompressionWithPartialWrite(t *testing.T) {
	payload := "This is the content of a message that will be compresseddddddddddddddddddddd."
	msgProto := fmt.Sprintf("MSG foo 1 %d\r\n%s\r\n", len(payload), payload)

	o := testWSOptions()
	o.Websocket.Compression = true
	s := RunServer(o)
	defer s.Shutdown()

	c, br := testWSCreateClient(t, true, false, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()

	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, true, []byte("SUB foo 1\r\nPING\r\n"))
	c.Write(proto)
	l := testWSReadFrame(t, br)
	if !bytes.Equal(l, []byte(pongProto)) {
		t.Fatalf("Expected PONG, got %q", l)
	}

	pingPayload := []byte("my ping")
	pingFromWSClient := testWSCreateClientMsg(wsPingMessage, 1, true, false, pingPayload)

	var wc *testWSWrappedConn
	var ws *client
	s.mu.Lock()
	for _, c := range s.clients {
		ws = c
		c.mu.Lock()
		wc = &testWSWrappedConn{
			Conn: c.nc,
			buf:  &bytes.Buffer{},
		}
		c.nc = wc
		c.mu.Unlock()
		break
	}
	s.mu.Unlock()

	wc.mu.Lock()
	wc.partial = true
	wc.mu.Unlock()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	expected := &bytes.Buffer{}
	for i := 0; i < 10; i++ {
		if i > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		expected.Write([]byte(msgProto))
		natsPub(t, nc, "foo", []byte(payload))
		if i == 1 {
			c.Write(pingFromWSClient)
		}
	}

	var gotPingResponse bool
	res := &bytes.Buffer{}
	for total := 0; total < 10*len(msgProto); {
		l := testWSReadFrame(t, br)
		if bytes.Equal(l, pingPayload) {
			gotPingResponse = true
		} else {
			n, _ := res.Write(l)
			total += n
		}
	}
	if !bytes.Equal(expected.Bytes(), res.Bytes()) {
		t.Fatalf("Unexpected result: %q", res)
	}
	if !gotPingResponse {
		t.Fatal("Did not get the ping response")
	}

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ws.mu.Lock()
		pb := ws.out.pb
		wf := ws.ws.frames
		fs := ws.ws.fs
		ws.mu.Unlock()
		if pb != 0 || len(wf) != 0 || fs != 0 {
			return fmt.Errorf("Expected pb, wf and fs to be 0, got %v, %v, %v", pb, wf, fs)
		}
		return nil
	})
}

func TestWSCompressionFrameSizeLimit(t *testing.T) {
	for _, test := range []struct {
		name      string
		maskWrite bool
	}{
		{"no write masking", false},
		{"write masking", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := testWSOptions()
			opts.MaxPending = MAX_PENDING_SIZE
			s := &Server{opts: opts}
			c := &client{srv: s, ws: &websocket{compress: true, browser: true, maskwrite: test.maskWrite}}
			c.initClient()

			uncompressedPayload := make([]byte, 2*wsFrameSizeForBrowsers)
			for i := 0; i < len(uncompressedPayload); i++ {
				uncompressedPayload[i] = byte(rand.Intn(256))
			}

			c.mu.Lock()
			c.out.nb = append(net.Buffers(nil), uncompressedPayload)
			nb, _ := c.collapsePtoNB()
			c.mu.Unlock()

			bb := &bytes.Buffer{}
			var key []byte
			for i, b := range nb {
				// frame header buffer are always very small. The payload should not be more
				// than 10 bytes since that is what we passed as the limit.
				if len(b) > wsFrameSizeForBrowsers {
					t.Fatalf("Frame size too big: %v (%q)", len(b), b)
				}
				if test.maskWrite {
					if i%2 == 0 {
						key = b[len(b)-4:]
					} else {
						wsMaskBuf(key, b)
					}
				}
				// Check frame headers for the proper formatting.
				if i%2 == 0 {
					// Only the first frame should have the compress bit set.
					if b[0]&wsRsv1Bit != 0 {
						if i > 0 {
							t.Fatalf("Compressed bit should not be in continuation frame")
						}
					} else if i == 0 {
						t.Fatalf("Compressed bit missing")
					}
				} else {
					// Collect the payload
					bb.Write(b)
				}
			}
			buf := bb.Bytes()
			buf = append(buf, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff)
			dbr := bytes.NewBuffer(buf)
			d := flate.NewReader(dbr)
			uncompressed, err := ioutil.ReadAll(d)
			if err != nil {
				t.Fatalf("Error reading frame: %v", err)
			}
			if !bytes.Equal(uncompressed, uncompressedPayload) {
				t.Fatalf("Unexpected uncomressed data: %q", uncompressed)
			}
		})
	}
}

func TestWSBasicAuth(t *testing.T) {
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		err  string
	}{
		{
			"top level auth, no override, wrong u/p",
			func() *Options {
				o := testWSOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"websocket", "client", "-ERR 'Authorization Violation'",
		},
		{
			"top level auth, no override, correct u/p",
			func() *Options {
				o := testWSOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"normal", "client", "",
		},
		{
			"no top level auth, ws auth, wrong u/p",
			func() *Options {
				o := testWSOptions()
				o.Websocket.Username = "websocket"
				o.Websocket.Password = "client"
				return o
			},
			"normal", "client", "-ERR 'Authorization Violation'",
		},
		{
			"no top level auth, ws auth, correct u/p",
			func() *Options {
				o := testWSOptions()
				o.Websocket.Username = "websocket"
				o.Websocket.Password = "client"
				return o
			},
			"websocket", "client", "",
		},
		{
			"top level auth, ws override, wrong u/p",
			func() *Options {
				o := testWSOptions()
				o.Username = "normal"
				o.Password = "client"
				o.Websocket.Username = "websocket"
				o.Websocket.Password = "client"
				return o
			},
			"normal", "client", "-ERR 'Authorization Violation'",
		},
		{
			"top level auth, ws override, correct u/p",
			func() *Options {
				o := testWSOptions()
				o.Username = "normal"
				o.Password = "client"
				o.Websocket.Username = "websocket"
				o.Websocket.Password = "client"
				return o
			},
			"websocket", "client", "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, _ := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			connectProto := fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"user\":\"%s\",\"pass\":\"%s\"}\r\nPING\r\n",
				test.user, test.pass)

			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if test.err == "" && !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Expected to receive PONG, got %q", msg)
			} else if test.err != "" && !bytes.HasPrefix(msg, []byte(test.err)) {
				t.Fatalf("Expected to receive %q, got %q", test.err, msg)
			}
		})
	}
}

func TestWSAuthTimeout(t *testing.T) {
	for _, test := range []struct {
		name string
		at   float64
		wat  float64
		err  string
	}{
		{"use top-level auth timeout", 10.0, 0.0, ""},
		{"use websocket auth timeout", 10.0, 0.05, "-ERR 'Authentication Timeout'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testWSOptions()
			o.AuthTimeout = test.at
			o.Websocket.Username = "websocket"
			o.Websocket.Password = "client"
			o.Websocket.AuthTimeout = test.wat
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, l := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			var info serverInfo
			json.Unmarshal([]byte(l[5:]), &info)
			// Make sure that we are told that auth is required.
			if !info.AuthRequired {
				t.Fatalf("Expected auth required, was not: %q", l)
			}
			start := time.Now()
			// Wait before sending connect
			time.Sleep(100 * time.Millisecond)
			connectProto := "CONNECT {\"verbose\":false,\"protocol\":1,\"user\":\"websocket\",\"pass\":\"client\"}\r\nPING\r\n"
			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if test.err != "" && !bytes.HasPrefix(msg, []byte(test.err)) {
				t.Fatalf("Expected to receive %q error, got %q", test.err, msg)
			} else if test.err == "" && !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Unexpected error: %q", msg)
			}
			if dur := time.Since(start); dur > time.Second {
				t.Fatalf("Too long to get timeout error: %v", dur)
			}
		})
	}
}

func TestWSTokenAuth(t *testing.T) {
	for _, test := range []struct {
		name  string
		opts  func() *Options
		token string
		err   string
	}{
		{
			"top level auth, no override, wrong token",
			func() *Options {
				o := testWSOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"badtoken", "-ERR 'Authorization Violation'",
		},
		{
			"top level auth, no override, correct token",
			func() *Options {
				o := testWSOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"goodtoken", "",
		},
		{
			"no top level auth, ws auth, wrong token",
			func() *Options {
				o := testWSOptions()
				o.Websocket.Token = "goodtoken"
				return o
			},
			"badtoken", "-ERR 'Authorization Violation'",
		},
		{
			"no top level auth, ws auth, correct token",
			func() *Options {
				o := testWSOptions()
				o.Websocket.Token = "goodtoken"
				return o
			},
			"goodtoken", "",
		},
		{
			"top level auth, ws override, wrong token",
			func() *Options {
				o := testWSOptions()
				o.Authorization = "clienttoken"
				o.Websocket.Token = "websockettoken"
				return o
			},
			"clienttoken", "-ERR 'Authorization Violation'",
		},
		{
			"top level auth, ws override, correct token",
			func() *Options {
				o := testWSOptions()
				o.Authorization = "clienttoken"
				o.Websocket.Token = "websockettoken"
				return o
			},
			"websockettoken", "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, _ := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			connectProto := fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"auth_token\":\"%s\"}\r\nPING\r\n",
				test.token)

			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if test.err == "" && !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Expected to receive PONG, got %q", msg)
			} else if test.err != "" && !bytes.HasPrefix(msg, []byte(test.err)) {
				t.Fatalf("Expected to receive %q, got %q", test.err, msg)
			}
		})
	}
}

func TestWSBindToProperAccount(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		accounts {
			a {
				users [
					{user: a, password: pwd, allowed_connection_types: ["%s", "%s"]}
				]
			}
			b {
				users [
					{user: b, password: pwd}
				]
			}
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`, jwt.ConnectionTypeStandard, strings.ToLower(jwt.ConnectionTypeWebsocket)))) // on purpose use lower case to ensure that it is converted.
	defer removeFile(t, conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, fmt.Sprintf("nats://a:pwd@127.0.0.1:%d", o.Port))
	defer nc.Close()

	sub := natsSubSync(t, nc, "foo")

	wsc, br, _ := testNewWSClient(t, testWSClientOptions{host: o.Websocket.Host, port: o.Websocket.Port, noTLS: true})
	// Send CONNECT and PING
	wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false,
		[]byte(fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"user\":\"%s\",\"pass\":\"%s\"}\r\nPING\r\n", "a", "pwd")))
	if _, err := wsc.Write(wsmsg); err != nil {
		t.Fatalf("Error sending message: %v", err)
	}
	// Wait for the PONG
	if msg := testWSReadFrame(t, br); !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
		t.Fatalf("Expected PONG, got %s", msg)
	}

	wsmsg = testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte("PUB foo 7\r\nfrom ws\r\n"))
	if _, err := wsc.Write(wsmsg); err != nil {
		t.Fatalf("Error sending message: %v", err)
	}

	natsNexMsg(t, sub, time.Second)
}

func TestWSUsersAuth(t *testing.T) {
	users := []*User{{Username: "user", Password: "pwd"}}
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		err  string
	}{
		{
			"no filtering, wrong user",
			func() *Options {
				o := testWSOptions()
				o.Users = users
				return o
			},
			"wronguser", "pwd", "-ERR 'Authorization Violation'",
		},
		{
			"no filtering, correct user",
			func() *Options {
				o := testWSOptions()
				o.Users = users
				return o
			},
			"user", "pwd", "",
		},
		{
			"filering, user not allowed",
			func() *Options {
				o := testWSOptions()
				o.Users = users
				// Only allowed for regular clients
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard})
				return o
			},
			"user", "pwd", "-ERR 'Authorization Violation'",
		},
		{
			"filtering, user allowed",
			func() *Options {
				o := testWSOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket})
				return o
			},
			"user", "pwd", "",
		},
		{
			"filtering, wrong password",
			func() *Options {
				o := testWSOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket})
				return o
			},
			"user", "badpassword", "-ERR 'Authorization Violation'",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, _ := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			connectProto := fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"user\":\"%s\",\"pass\":\"%s\"}\r\nPING\r\n",
				test.user, test.pass)

			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if test.err == "" && !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Expected to receive PONG, got %q", msg)
			} else if test.err != "" && !bytes.HasPrefix(msg, []byte(test.err)) {
				t.Fatalf("Expected to receive %q, got %q", test.err, msg)
			}
		})
	}
}

func TestWSNoAuthUserValidation(t *testing.T) {
	o := testWSOptions()
	o.Users = []*User{{Username: "user", Password: "pwd"}}
	// Should fail because it is not part of o.Users.
	o.Websocket.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}
	// Set a valid no auth user for global options, but still should fail because
	// of o.Websocket.NoAuthUser
	o.NoAuthUser = "user"
	o.Websocket.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}
}

func TestWSNoAuthUser(t *testing.T) {
	for _, test := range []struct {
		name         string
		override     bool
		useAuth      bool
		expectedUser string
		expectedAcc  string
	}{
		{"no override, no user provided", false, false, "noauth", "normal"},
		{"no override, user povided", false, true, "user", "normal"},
		{"override, no user provided", true, false, "wsnoauth", "websocket"},
		{"override, user provided", true, true, "wsuser", "websocket"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testWSOptions()
			normalAcc := NewAccount("normal")
			websocketAcc := NewAccount("websocket")
			o.Accounts = []*Account{normalAcc, websocketAcc}
			o.Users = []*User{
				{Username: "noauth", Password: "pwd", Account: normalAcc},
				{Username: "user", Password: "pwd", Account: normalAcc},
				{Username: "wsnoauth", Password: "pwd", Account: websocketAcc},
				{Username: "wsuser", Password: "pwd", Account: websocketAcc},
			}
			o.NoAuthUser = "noauth"
			if test.override {
				o.Websocket.NoAuthUser = "wsnoauth"
			}
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, l := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			var info serverInfo
			json.Unmarshal([]byte(l[5:]), &info)

			var connectProto string
			if test.useAuth {
				connectProto = fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"user\":\"%s\",\"pass\":\"pwd\"}\r\nPING\r\n",
					test.expectedUser)
			} else {
				connectProto = "CONNECT {\"verbose\":false,\"protocol\":1}\r\nPING\r\n"
			}
			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Unexpected error: %q", msg)
			}

			c := s.getClient(info.CID)
			c.mu.Lock()
			uname := c.opts.Username
			aname := c.acc.GetName()
			c.mu.Unlock()
			if uname != test.expectedUser {
				t.Fatalf("Expected selected user to be %q, got %q", test.expectedUser, uname)
			}
			if aname != test.expectedAcc {
				t.Fatalf("Expected selected account to be %q, got %q", test.expectedAcc, aname)
			}
		})
	}
}

func TestWSNkeyAuth(t *testing.T) {
	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()

	wsnkp, _ := nkeys.CreateUser()
	wspub, _ := wsnkp.PublicKey()

	badkp, _ := nkeys.CreateUser()
	badpub, _ := badkp.PublicKey()

	for _, test := range []struct {
		name string
		opts func() *Options
		nkey string
		kp   nkeys.KeyPair
		err  string
	}{
		{
			"no filtering, wrong nkey",
			func() *Options {
				o := testWSOptions()
				o.Nkeys = []*NkeyUser{{Nkey: pub}}
				return o
			},
			badpub, badkp, "-ERR 'Authorization Violation'",
		},
		{
			"no filtering, correct nkey",
			func() *Options {
				o := testWSOptions()
				o.Nkeys = []*NkeyUser{{Nkey: pub}}
				return o
			},
			pub, nkp, "",
		},
		{
			"filtering, nkey not allowed",
			func() *Options {
				o := testWSOptions()
				o.Nkeys = []*NkeyUser{
					{
						Nkey:                   pub,
						AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard}),
					},
					{
						Nkey:                   wspub,
						AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeWebsocket}),
					},
				}
				return o
			},
			pub, nkp, "-ERR 'Authorization Violation'",
		},
		{
			"filtering, correct nkey",
			func() *Options {
				o := testWSOptions()
				o.Nkeys = []*NkeyUser{
					{Nkey: pub},
					{
						Nkey:                   wspub,
						AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket}),
					},
				}
				return o
			},
			wspub, wsnkp, "",
		},
		{
			"filtering, wrong nkey",
			func() *Options {
				o := testWSOptions()
				o.Nkeys = []*NkeyUser{
					{
						Nkey:                   wspub,
						AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket}),
					},
				}
				return o
			},
			badpub, badkp, "-ERR 'Authorization Violation'",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := RunServer(o)
			defer s.Shutdown()

			wsc, br, infoMsg := testWSCreateClientGetInfo(t, false, false, o.Websocket.Host, o.Websocket.Port)
			defer wsc.Close()

			// Sign Nonce
			var info nonceInfo
			json.Unmarshal([]byte(infoMsg[5:]), &info)
			sigraw, _ := test.kp.Sign([]byte(info.Nonce))
			sig := base64.RawURLEncoding.EncodeToString(sigraw)

			connectProto := fmt.Sprintf("CONNECT {\"verbose\":false,\"protocol\":1,\"nkey\":\"%s\",\"sig\":\"%s\"}\r\nPING\r\n", test.nkey, sig)

			wsmsg := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, []byte(connectProto))
			if _, err := wsc.Write(wsmsg); err != nil {
				t.Fatalf("Error sending message: %v", err)
			}
			msg := testWSReadFrame(t, br)
			if test.err == "" && !bytes.HasPrefix(msg, []byte("PONG\r\n")) {
				t.Fatalf("Expected to receive PONG, got %q", msg)
			} else if test.err != "" && !bytes.HasPrefix(msg, []byte(test.err)) {
				t.Fatalf("Expected to receive %q, got %q", test.err, msg)
			}
		})
	}
}

func TestWSJWTWithAllowedConnectionTypes(t *testing.T) {
	o := testWSOptions()
	setupAddTrusted(o)
	s := RunServer(o)
	buildMemAccResolver(s)
	defer s.Shutdown()

	for _, test := range []struct {
		name            string
		connectionTypes []string
		expectedAnswer  string
	}{
		{"not allowed", []string{jwt.ConnectionTypeStandard}, "-ERR"},
		{"allowed", []string{jwt.ConnectionTypeStandard, strings.ToLower(jwt.ConnectionTypeWebsocket)}, "+OK"},
		{"allowed with unknown", []string{jwt.ConnectionTypeWebsocket, "SomeNewType"}, "+OK"},
		{"not allowed with unknown", []string{"SomeNewType"}, "-ERR"},
	} {
		t.Run(test.name, func(t *testing.T) {
			nuc := newJWTTestUserClaims()
			nuc.AllowedConnectionTypes = test.connectionTypes
			claimOpt := testClaimsOptions{
				nuc:          nuc,
				expectAnswer: test.expectedAnswer,
			}
			_, c, _, _ := testWSWithClaims(t, s, testWSClientOptions{host: o.Websocket.Host, port: o.Websocket.Port}, claimOpt)
			c.Close()
		})
	}
}

func TestWSJWTCookieUser(t *testing.T) {

	nucSigFunc := func() *jwt.UserClaims { return newJWTTestUserClaims() }
	nucBearerFunc := func() *jwt.UserClaims {
		ret := newJWTTestUserClaims()
		ret.BearerToken = true
		return ret
	}

	o := testWSOptions()
	setupAddTrusted(o)
	setupAddCookie(o)
	s := RunServer(o)
	buildMemAccResolver(s)
	defer s.Shutdown()

	genJwt := func(t *testing.T, nuc *jwt.UserClaims) string {
		okp, _ := nkeys.FromSeed(oSeed)

		akp, _ := nkeys.CreateAccount()
		apub, _ := akp.PublicKey()

		nac := jwt.NewAccountClaims(apub)
		ajwt, err := nac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating account JWT: %v", err)
		}

		nkp, _ := nkeys.CreateUser()
		pub, _ := nkp.PublicKey()
		nuc.Subject = pub
		jwt, err := nuc.Encode(akp)
		if err != nil {
			t.Fatalf("Error generating user JWT: %v", err)
		}
		addAccountToMemResolver(s, apub, ajwt)
		return jwt
	}

	cliOpts := testWSClientOptions{
		host: o.Websocket.Host,
		port: o.Websocket.Port,
	}
	for _, test := range []struct {
		name         string
		nuc          *jwt.UserClaims
		opts         func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions)
		expectAnswer string
	}{
		{
			name: "protocol auth, non-bearer key, with signature",
			nuc:  nucSigFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				return cliOpts, testClaimsOptions{nuc: claims}
			},
			expectAnswer: "+OK",
		},
		{
			name: "protocol auth, non-bearer key, w/o required signature",
			nuc:  nucSigFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				return cliOpts, testClaimsOptions{nuc: claims, dontSign: true}
			},
			expectAnswer: "-ERR",
		},
		{
			name: "protocol auth, bearer key, w/o signature",
			nuc:  nucBearerFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				return cliOpts, testClaimsOptions{nuc: claims, dontSign: true}
			},
			expectAnswer: "+OK",
		},
		{
			name: "cookie auth, non-bearer key, protocol auth fail",
			nuc:  nucSigFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				co := cliOpts
				co.extraHeaders = map[string]string{}
				co.extraHeaders["Cookie"] = o.Websocket.JWTCookie + "=" + genJwt(t, claims)
				return co, testClaimsOptions{connectRequest: struct{}{}}
			},
			expectAnswer: "-ERR",
		},
		{
			name: "cookie auth, bearer key, protocol auth success with implied cookie jwt",
			nuc:  nucBearerFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				co := cliOpts
				co.extraHeaders = map[string]string{}
				co.extraHeaders["Cookie"] = o.Websocket.JWTCookie + "=" + genJwt(t, claims)
				return co, testClaimsOptions{connectRequest: struct{}{}}
			},
			expectAnswer: "+OK",
		},
		{
			name: "cookie auth, non-bearer key, protocol auth success via override jwt in CONNECT opts",
			nuc:  nucSigFunc(),
			opts: func(t *testing.T, claims *jwt.UserClaims) (testWSClientOptions, testClaimsOptions) {
				co := cliOpts
				co.extraHeaders = map[string]string{}
				co.extraHeaders["Cookie"] = o.Websocket.JWTCookie + "=" + genJwt(t, claims)
				return co, testClaimsOptions{nuc: nucBearerFunc()}
			},
			expectAnswer: "+OK",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cliOpt, claimOpt := test.opts(t, test.nuc)
			claimOpt.expectAnswer = test.expectAnswer
			_, c, _, _ := testWSWithClaims(t, s, cliOpt, claimOpt)
			c.Close()
		})
	}
	s.Shutdown()
}

func TestWSReloadTLSConfig(t *testing.T) {
	template := `
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: '%s'
				key_file: '%s'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template,
		"../test/configs/certs/server-noip.pem",
		"../test/configs/certs/server-key-noip.pem")))
	defer removeFile(t, conf)

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.Websocket.Port)
	wsc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer wsc.Close()

	tc := &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"}
	tlsConfig, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	tlsConfig.ServerName = "127.0.0.1"
	tlsConfig.RootCAs = tlsConfig.ClientCAs
	tlsConfig.ClientCAs = nil
	wsc = tls.Client(wsc, tlsConfig.Clone())
	if err := wsc.(*tls.Conn).Handshake(); err == nil || !strings.Contains(err.Error(), "SAN") {
		t.Fatalf("Unexpected error: %v", err)
	}
	wsc.Close()

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template,
		"../test/configs/certs/server-cert.pem",
		"../test/configs/certs/server-key.pem"))

	wsc, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer wsc.Close()

	wsc = tls.Client(wsc, tlsConfig.Clone())
	if err := wsc.(*tls.Conn).Handshake(); err != nil {
		t.Fatalf("Error on TLS handshake: %v", err)
	}
}

// ==================================================================
// = Benchmark tests
// ==================================================================

const testWSBenchSubject = "a"

var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedString(sz int) string {
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return string(b)
}

func sizedStringForCompression(sz int) string {
	b := make([]byte, sz)
	c := byte(0)
	s := 0
	for i := range b {
		if s%20 == 0 {
			c = ch[rand.Intn(len(ch))]
		}
		b[i] = c
	}
	return string(b)
}

func testWSFlushConn(b *testing.B, compress bool, c net.Conn, br *bufio.Reader) {
	buf := testWSCreateClientMsg(wsBinaryMessage, 1, true, compress, []byte(pingProto))
	c.Write(buf)
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	res := testWSReadFrame(b, br)
	c.SetReadDeadline(time.Time{})
	if !bytes.HasPrefix(res, []byte(pongProto)) {
		b.Fatalf("Failed read of PONG: %s\n", res)
	}
}

func wsBenchPub(b *testing.B, numPubs int, compress bool, payload string) {
	b.StopTimer()
	opts := testWSOptions()
	opts.Websocket.Compression = compress
	s := RunServer(opts)
	defer s.Shutdown()

	n := b.N
	extra := 0
	pubProto := []byte(fmt.Sprintf("PUB %s %d\r\n%s\r\n", testWSBenchSubject, len(payload), payload))
	singleOpBuf := testWSCreateClientMsg(wsBinaryMessage, 1, true, compress, pubProto)

	// Simulate client that would buffer messages before framing/sending.
	// Figure out how many we can fit in one frame based on b.N and length of pubProto
	const bufSize = 32768
	tmpa := [bufSize]byte{}
	tmp := tmpa[:0]
	pb := 0
	for i := 0; i < b.N; i++ {
		tmp = append(tmp, pubProto...)
		pb++
		if len(tmp) >= bufSize {
			break
		}
	}
	sendBuf := testWSCreateClientMsg(wsBinaryMessage, 1, true, compress, tmp)
	n = b.N / pb
	extra = b.N - (n * pb)

	wg := sync.WaitGroup{}
	wg.Add(numPubs)

	type pub struct {
		c  net.Conn
		br *bufio.Reader
		bw *bufio.Writer
	}
	var pubs []pub
	for i := 0; i < numPubs; i++ {
		wsc, br := testWSCreateClient(b, compress, false, opts.Websocket.Host, opts.Websocket.Port)
		defer wsc.Close()
		bw := bufio.NewWriterSize(wsc, bufSize)
		pubs = append(pubs, pub{wsc, br, bw})
	}

	// Average the amount of bytes sent by iteration
	avg := len(sendBuf) / pb
	if extra > 0 {
		avg += len(singleOpBuf)
		avg /= 2
	}
	b.SetBytes(int64(numPubs * avg))
	b.StartTimer()

	for i := 0; i < numPubs; i++ {
		p := pubs[i]
		go func(p pub) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				p.bw.Write(sendBuf)
			}
			for i := 0; i < extra; i++ {
				p.bw.Write(singleOpBuf)
			}
			p.bw.Flush()
			testWSFlushConn(b, compress, p.c, p.br)
		}(p)
	}
	wg.Wait()
	b.StopTimer()
}

func Benchmark_WS_Pubx1_CN_____0b(b *testing.B) {
	wsBenchPub(b, 1, false, "")
}

func Benchmark_WS_Pubx1_CY_____0b(b *testing.B) {
	wsBenchPub(b, 1, true, "")
}

func Benchmark_WS_Pubx1_CN___128b(b *testing.B) {
	s := sizedString(128)
	wsBenchPub(b, 1, false, s)
}

func Benchmark_WS_Pubx1_CY___128b(b *testing.B) {
	s := sizedStringForCompression(128)
	wsBenchPub(b, 1, true, s)
}

func Benchmark_WS_Pubx1_CN__1024b(b *testing.B) {
	s := sizedString(1024)
	wsBenchPub(b, 1, false, s)
}

func Benchmark_WS_Pubx1_CY__1024b(b *testing.B) {
	s := sizedStringForCompression(1024)
	wsBenchPub(b, 1, true, s)
}

func Benchmark_WS_Pubx1_CN__4096b(b *testing.B) {
	s := sizedString(4 * 1024)
	wsBenchPub(b, 1, false, s)
}

func Benchmark_WS_Pubx1_CY__4096b(b *testing.B) {
	s := sizedStringForCompression(4 * 1024)
	wsBenchPub(b, 1, true, s)
}

func Benchmark_WS_Pubx1_CN__8192b(b *testing.B) {
	s := sizedString(8 * 1024)
	wsBenchPub(b, 1, false, s)
}

func Benchmark_WS_Pubx1_CY__8192b(b *testing.B) {
	s := sizedStringForCompression(8 * 1024)
	wsBenchPub(b, 1, true, s)
}

func Benchmark_WS_Pubx1_CN_32768b(b *testing.B) {
	s := sizedString(32 * 1024)
	wsBenchPub(b, 1, false, s)
}

func Benchmark_WS_Pubx1_CY_32768b(b *testing.B) {
	s := sizedStringForCompression(32 * 1024)
	wsBenchPub(b, 1, true, s)
}

func Benchmark_WS_Pubx5_CN_____0b(b *testing.B) {
	wsBenchPub(b, 5, false, "")
}

func Benchmark_WS_Pubx5_CY_____0b(b *testing.B) {
	wsBenchPub(b, 5, true, "")
}

func Benchmark_WS_Pubx5_CN___128b(b *testing.B) {
	s := sizedString(128)
	wsBenchPub(b, 5, false, s)
}

func Benchmark_WS_Pubx5_CY___128b(b *testing.B) {
	s := sizedStringForCompression(128)
	wsBenchPub(b, 5, true, s)
}

func Benchmark_WS_Pubx5_CN__1024b(b *testing.B) {
	s := sizedString(1024)
	wsBenchPub(b, 5, false, s)
}

func Benchmark_WS_Pubx5_CY__1024b(b *testing.B) {
	s := sizedStringForCompression(1024)
	wsBenchPub(b, 5, true, s)
}

func Benchmark_WS_Pubx5_CN__4096b(b *testing.B) {
	s := sizedString(4 * 1024)
	wsBenchPub(b, 5, false, s)
}

func Benchmark_WS_Pubx5_CY__4096b(b *testing.B) {
	s := sizedStringForCompression(4 * 1024)
	wsBenchPub(b, 5, true, s)
}

func Benchmark_WS_Pubx5_CN__8192b(b *testing.B) {
	s := sizedString(8 * 1024)
	wsBenchPub(b, 5, false, s)
}

func Benchmark_WS_Pubx5_CY__8192b(b *testing.B) {
	s := sizedStringForCompression(8 * 1024)
	wsBenchPub(b, 5, true, s)
}

func Benchmark_WS_Pubx5_CN_32768b(b *testing.B) {
	s := sizedString(32 * 1024)
	wsBenchPub(b, 5, false, s)
}

func Benchmark_WS_Pubx5_CY_32768b(b *testing.B) {
	s := sizedStringForCompression(32 * 1024)
	wsBenchPub(b, 5, true, s)
}

func wsBenchSub(b *testing.B, numSubs int, compress bool, payload string) {
	b.StopTimer()
	opts := testWSOptions()
	opts.Websocket.Compression = compress
	s := RunServer(opts)
	defer s.Shutdown()

	var subs []*bufio.Reader
	for i := 0; i < numSubs; i++ {
		wsc, br := testWSCreateClient(b, compress, false, opts.Websocket.Host, opts.Websocket.Port)
		defer wsc.Close()
		subProto := testWSCreateClientMsg(wsBinaryMessage, 1, true, compress,
			[]byte(fmt.Sprintf("SUB %s 1\r\nPING\r\n", testWSBenchSubject)))
		wsc.Write(subProto)
		// Waiting for PONG
		testWSReadFrame(b, br)
		subs = append(subs, br)
	}

	wg := sync.WaitGroup{}
	wg.Add(numSubs)

	// Use regular NATS client to publish messages
	nc := natsConnect(b, s.ClientURL())
	defer nc.Close()

	b.StartTimer()

	for i := 0; i < numSubs; i++ {
		br := subs[i]
		go func(br *bufio.Reader) {
			defer wg.Done()
			for count := 0; count < b.N; {
				msgs := testWSReadFrame(b, br)
				count += bytes.Count(msgs, []byte("MSG "))
			}
		}(br)
	}
	for i := 0; i < b.N; i++ {
		natsPub(b, nc, testWSBenchSubject, []byte(payload))
	}
	wg.Wait()
	b.StopTimer()
}

func Benchmark_WS_Subx1_CN_____0b(b *testing.B) {
	wsBenchSub(b, 1, false, "")
}

func Benchmark_WS_Subx1_CY_____0b(b *testing.B) {
	wsBenchSub(b, 1, true, "")
}

func Benchmark_WS_Subx1_CN___128b(b *testing.B) {
	s := sizedString(128)
	wsBenchSub(b, 1, false, s)
}

func Benchmark_WS_Subx1_CY___128b(b *testing.B) {
	s := sizedStringForCompression(128)
	wsBenchSub(b, 1, true, s)
}

func Benchmark_WS_Subx1_CN__1024b(b *testing.B) {
	s := sizedString(1024)
	wsBenchSub(b, 1, false, s)
}

func Benchmark_WS_Subx1_CY__1024b(b *testing.B) {
	s := sizedStringForCompression(1024)
	wsBenchSub(b, 1, true, s)
}

func Benchmark_WS_Subx1_CN__4096b(b *testing.B) {
	s := sizedString(4096)
	wsBenchSub(b, 1, false, s)
}

func Benchmark_WS_Subx1_CY__4096b(b *testing.B) {
	s := sizedStringForCompression(4096)
	wsBenchSub(b, 1, true, s)
}

func Benchmark_WS_Subx1_CN__8192b(b *testing.B) {
	s := sizedString(8192)
	wsBenchSub(b, 1, false, s)
}

func Benchmark_WS_Subx1_CY__8192b(b *testing.B) {
	s := sizedStringForCompression(8192)
	wsBenchSub(b, 1, true, s)
}

func Benchmark_WS_Subx1_CN_32768b(b *testing.B) {
	s := sizedString(32768)
	wsBenchSub(b, 1, false, s)
}

func Benchmark_WS_Subx1_CY_32768b(b *testing.B) {
	s := sizedStringForCompression(32768)
	wsBenchSub(b, 1, true, s)
}

func Benchmark_WS_Subx5_CN_____0b(b *testing.B) {
	wsBenchSub(b, 5, false, "")
}

func Benchmark_WS_Subx5_CY_____0b(b *testing.B) {
	wsBenchSub(b, 5, true, "")
}

func Benchmark_WS_Subx5_CN___128b(b *testing.B) {
	s := sizedString(128)
	wsBenchSub(b, 5, false, s)
}

func Benchmark_WS_Subx5_CY___128b(b *testing.B) {
	s := sizedStringForCompression(128)
	wsBenchSub(b, 5, true, s)
}

func Benchmark_WS_Subx5_CN__1024b(b *testing.B) {
	s := sizedString(1024)
	wsBenchSub(b, 5, false, s)
}

func Benchmark_WS_Subx5_CY__1024b(b *testing.B) {
	s := sizedStringForCompression(1024)
	wsBenchSub(b, 5, true, s)
}

func Benchmark_WS_Subx5_CN__4096b(b *testing.B) {
	s := sizedString(4096)
	wsBenchSub(b, 5, false, s)
}

func Benchmark_WS_Subx5_CY__4096b(b *testing.B) {
	s := sizedStringForCompression(4096)
	wsBenchSub(b, 5, true, s)
}

func Benchmark_WS_Subx5_CN__8192b(b *testing.B) {
	s := sizedString(8192)
	wsBenchSub(b, 5, false, s)
}

func Benchmark_WS_Subx5_CY__8192b(b *testing.B) {
	s := sizedStringForCompression(8192)
	wsBenchSub(b, 5, true, s)
}

func Benchmark_WS_Subx5_CN_32768b(b *testing.B) {
	s := sizedString(32768)
	wsBenchSub(b, 5, false, s)
}

func Benchmark_WS_Subx5_CY_32768b(b *testing.B) {
	s := sizedStringForCompression(32768)
	wsBenchSub(b, 5, true, s)
}
