// Package gateway terminates client WebSockets and (in later PRs) routes room traffic to owners.
//
// This is the transport skeleton: accept + authenticate a WebSocket, frame protobuf
// ClientMessage/ServerMessage envelopes, answer the app-level Ping with Pong, and tear down
// cleanly. Room handling (Join/Commit/Broadcast/Leave → owner RPC) lands in later PRs; those
// frames get an UNIMPLEMENTED error for now.
package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

const (
	maxFrameBytes = 1 << 20 // per-frame read cap (1 MiB)
	writeTimeout  = 10 * time.Second
	outQueue      = 64 // buffered outbound frames per connection
)

// Server is an http.Handler that upgrades requests to the Aether client WebSocket.
type Server struct {
	auth Authenticator
}

// NewServer returns a gateway WebSocket server that authenticates handshakes with auth.
func NewServer(auth Authenticator) *Server {
	return &Server{auth: auth}
}

// ServeHTTP authenticates the handshake, upgrades to a WebSocket, and serves the connection.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Principal will seed client_id derivation in a later PR; here it only gates access.
	if _, err := s.auth.Authenticate(r.Context(), r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already wrote the failure response
	}
	(&conn{ws: ws, out: make(chan *aetherv1.ServerMessage, outQueue)}).run(r.Context())
}

// conn is one client WebSocket: a read loop decoding ClientMessage frames and a single writer
// goroutine encoding ServerMessage frames (a WS permits only one concurrent writer).
type conn struct {
	ws  *websocket.Conn
	out chan *aetherv1.ServerMessage
}

func (c *conn) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.ws.SetReadLimit(maxFrameBytes)

	writerDone := make(chan struct{})
	go func() {
		c.writeLoop(ctx)
		close(writerDone)
	}()

	c.readLoop(ctx) // blocks until the client disconnects or errors
	cancel()        // stop the writer
	<-writerDone
	_ = c.ws.Close(websocket.StatusNormalClosure, "")
}

// readLoop decodes inbound frames until the connection closes.
func (c *conn) readLoop(ctx context.Context) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return // normal close or transport error — tear down
		}
		if typ != websocket.MessageBinary {
			c.send(ctx, errorFrame("INVALID", "expected a binary protobuf frame"))
			continue
		}
		var m aetherv1.ClientMessage
		if err := proto.Unmarshal(data, &m); err != nil {
			c.send(ctx, errorFrame("INVALID", "malformed ClientMessage"))
			continue
		}
		c.dispatch(ctx, &m)
	}
}

// dispatch handles one decoded frame. The skeleton answers Ping; room frames are UNIMPLEMENTED
// until their PRs wire the owner RPC.
func (c *conn) dispatch(ctx context.Context, m *aetherv1.ClientMessage) {
	switch b := m.GetBody().(type) {
	case *aetherv1.ClientMessage_Ping:
		c.send(ctx, &aetherv1.ServerMessage{
			Body: &aetherv1.ServerMessage_Pong{Pong: &aetherv1.Pong{Id: b.Ping.GetId()}},
		})
	case *aetherv1.ClientMessage_Join, *aetherv1.ClientMessage_Commit,
		*aetherv1.ClientMessage_Broadcast, *aetherv1.ClientMessage_Leave:
		c.send(ctx, errorFrame("UNIMPLEMENTED", "room handling lands in a later gateway PR"))
	default:
		c.send(ctx, errorFrame("INVALID", "empty or unknown frame"))
	}
}

// writeLoop is the sole writer: it drains the outbound queue to the socket.
func (c *conn) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.out:
			data, err := proto.Marshal(m)
			if err != nil {
				continue // a ServerMessage we built ourselves shouldn't fail to marshal
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err = c.ws.Write(wctx, websocket.MessageBinary, data)
			cancel()
			if err != nil {
				return // socket wedged/closed — let run() tear down
			}
		}
	}
}

// send enqueues a frame for the writer, dropping it only if the connection is going away.
func (c *conn) send(ctx context.Context, m *aetherv1.ServerMessage) {
	select {
	case c.out <- m:
	case <-ctx.Done():
	}
}

func errorFrame(code, msg string) *aetherv1.ServerMessage {
	return &aetherv1.ServerMessage{
		Body: &aetherv1.ServerMessage_Error{Error: &aetherv1.Error{Code: code, Message: msg}},
	}
}
