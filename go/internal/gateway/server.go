// Package gateway terminates client WebSockets and (in later PRs) routes room traffic to owners.
//
// This is the transport skeleton: accept + authenticate a WebSocket, frame protobuf
// ClientMessage/ServerMessage envelopes, answer the app-level Ping with Pong, keep the connection
// alive with periodic WS pings (dropping a silently-dead/half-open client), disconnect a client
// too slow to drain its outbound queue, and tear down cleanly without leaking goroutines. Room
// handling (Join/Commit/Broadcast/Leave → owner RPC) lands in later PRs; those frames get an
// UNIMPLEMENTED error for now.
package gateway

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

const (
	maxFrameBytes = 1 << 20 // per-frame read cap (1 MiB)
	writeTimeout  = 10 * time.Second
	outQueue      = 64 // buffered outbound frames per connection
	pingInterval  = 30 * time.Second
	pingTimeout   = 10 * time.Second
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

// conn is one client WebSocket: a read loop decoding ClientMessage frames, a single writer
// goroutine encoding ServerMessage frames (a WS permits only one concurrent writer), and a ping
// keepalive. All three share a context that any one cancels on exit, so the whole connection tears
// down together instead of leaking a goroutine, the socket, or the TCP conn.
type conn struct {
	ws     *websocket.Conn
	out    chan *aetherv1.ServerMessage
	cancel context.CancelFunc
}

func (c *conn) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()

	c.ws.SetReadLimit(maxFrameBytes)

	// Both background loops cancel the shared context on exit: a wedged writer or a missed pong
	// then unblocks the read loop (and any send) rather than deadlocking it.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); defer cancel(); c.writeLoop(ctx) }()
	go func() { defer wg.Done(); defer cancel(); c.pingLoop(ctx) }()

	c.readLoop(ctx) // blocks until the client disconnects, errors, or the context is cancelled
	cancel()        // ensure the loops stop even on a clean read-side close
	wg.Wait()
	_ = c.ws.Close(websocket.StatusNormalClosure, "")
}

// readLoop decodes inbound frames until the connection closes or the context is cancelled.
func (c *conn) readLoop(ctx context.Context) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return // normal close, transport error, or ctx cancelled — tear down
		}
		if typ != websocket.MessageBinary {
			c.send(errorFrame("INVALID", "expected a binary protobuf frame"))
			continue
		}
		var m aetherv1.ClientMessage
		if err := proto.Unmarshal(data, &m); err != nil {
			c.send(errorFrame("INVALID", "malformed ClientMessage"))
			continue
		}
		c.dispatch(&m)
	}
}

// dispatch handles one decoded frame. The skeleton answers Ping; room frames are UNIMPLEMENTED
// until their PRs wire the owner RPC.
func (c *conn) dispatch(m *aetherv1.ClientMessage) {
	switch b := m.GetBody().(type) {
	case *aetherv1.ClientMessage_Ping:
		c.send(&aetherv1.ServerMessage{
			Body: &aetherv1.ServerMessage_Pong{Pong: &aetherv1.Pong{Id: b.Ping.GetId()}},
		})
	case *aetherv1.ClientMessage_Join, *aetherv1.ClientMessage_Commit,
		*aetherv1.ClientMessage_Broadcast, *aetherv1.ClientMessage_Leave:
		c.send(errorFrame("UNIMPLEMENTED", "room handling lands in a later gateway PR"))
	default:
		c.send(errorFrame("INVALID", "empty or unknown frame"))
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
				return // socket wedged/closed — run()'s deferred cancel tears the conn down
			}
		}
	}
}

// pingLoop sends periodic WS pings and tears the connection down if a pong doesn't return in time
// — detecting a silently-dead (half-open) client that Read alone would never notice.
func (c *conn) pingLoop(ctx context.Context) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				return // no pong in time (or shutting down) — exit; run()'s cancel tears down
			}
		}
	}
}

// send enqueues a frame for the writer. A full queue means the client isn't draining, so we
// disconnect it (per design §9) — it recovers by reconnecting and resuming from lastSeq — rather
// than blocking the read loop forever.
func (c *conn) send(m *aetherv1.ServerMessage) {
	select {
	case c.out <- m:
	default:
		c.cancel()
	}
}

func errorFrame(code, msg string) *aetherv1.ServerMessage {
	return &aetherv1.ServerMessage{
		Body: &aetherv1.ServerMessage_Error{Error: &aetherv1.Error{Code: code, Message: msg}},
	}
}
