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
	"log"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
)

const (
	maxFrameBytes = 1 << 20 // per-frame read cap (1 MiB)
	writeTimeout  = 10 * time.Second
	outQueue      = 64 // buffered outbound frames per connection
	pingInterval  = 30 * time.Second
	pingTimeout   = 10 * time.Second
)

// defaultClientIDSecret is a DEV-ONLY HMAC key for client_id derivation, used only when no secret
// is injected. Production must set a real cluster-wide secret via WithClientIDSecret — every
// gateway must share it so a reconnect to any gateway derives the same id. Its use is warned about
// once at startup so a forgotten injection is caught, not silently shipped.
var (
	defaultClientIDSecret = []byte("aether-dev-client-id-secret")
	devSecretWarnOnce     sync.Once
)

// Server is an http.Handler that upgrades requests to the Aether client WebSocket and serves the
// room protocol against owners resolved through the locator.
type Server struct {
	auth    Authenticator
	locator *OwnerLocator
	secret  []byte // HMAC key for client_id derivation (cluster-wide)
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithClientIDSecret sets the cluster-wide HMAC key used to derive client_ids. All gateways MUST
// share it so a client's id (and thus its dedup identity) is stable across reconnects to any
// gateway. Defaults to a dev-only key.
func WithClientIDSecret(secret []byte) ServerOption { return func(s *Server) { s.secret = secret } }

// NewServer returns a gateway WebSocket server: it authenticates handshakes with auth and routes
// room traffic to owners via locator.
func NewServer(auth Authenticator, locator *OwnerLocator, opts ...ServerOption) *Server {
	s := &Server{auth: auth, locator: locator}
	for _, opt := range opts {
		opt(s)
	}
	if s.secret == nil {
		// Fail loud, not silent: a prod gateway that forgot WithClientIDSecret would otherwise
		// derive ids under a publicly-known key. (Once per process so tests don't spam.)
		devSecretWarnOnce.Do(func() {
			log.Println("gateway: WARNING using the DEV client_id secret; set WithClientIDSecret(<cluster secret>) in production")
		})
		s.secret = defaultClientIDSecret
	}
	return s
}

// ServeHTTP authenticates the handshake, upgrades to a WebSocket, and serves the connection.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, err := s.auth.Authenticate(r.Context(), r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already wrote the failure response
	}
	(&conn{
		srv:       s,
		principal: principal,
		ws:        ws,
		out:       make(chan *aetherv1.ServerMessage, outQueue),
		rooms:     map[string]context.CancelFunc{},
	}).run(r.Context())
}

// conn is one client WebSocket: a read loop decoding ClientMessage frames, a single writer
// goroutine encoding ServerMessage frames (a WS permits only one concurrent writer), and a ping
// keepalive. All three share a context that any one cancels on exit, so the whole connection tears
// down together instead of leaking a goroutine, the socket, or the TCP conn.
type conn struct {
	srv       *Server
	principal Principal
	clientID  string // derived at Join (HMAC of principal+nonce); the dedup identity for commits
	ws        *websocket.Conn
	out       chan *aetherv1.ServerMessage
	cancel    context.CancelFunc

	wg    sync.WaitGroup                // writeLoop + pingLoop + per-room relays
	rooms map[string]context.CancelFunc // joined room -> its relay's cancel (read-loop goroutine only)
}

func (c *conn) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()

	c.ws.SetReadLimit(maxFrameBytes)

	// Both background loops cancel the shared context on exit: a wedged writer or a missed pong
	// then unblocks the read loop (and any send) rather than deadlocking it.
	c.wg.Add(2)
	go func() { defer c.wg.Done(); defer cancel(); c.writeLoop(ctx) }()
	go func() { defer c.wg.Done(); defer cancel(); c.pingLoop(ctx) }()

	c.readLoop(ctx) // blocks until the client disconnects, errors, or the context is cancelled
	cancel()        // stop the loops and every per-room relay (their ctxs descend from this one)
	c.wg.Wait()     // writeLoop + pingLoop + relays all drained before we close the socket
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
		c.dispatch(ctx, &m)
	}
}

// dispatch handles one decoded frame. Join is served now; the remaining room frames are
// UNIMPLEMENTED until their PRs wire the owner RPC.
func (c *conn) dispatch(ctx context.Context, m *aetherv1.ClientMessage) {
	switch b := m.GetBody().(type) {
	case *aetherv1.ClientMessage_Ping:
		c.send(&aetherv1.ServerMessage{
			Body: &aetherv1.ServerMessage_Pong{Pong: &aetherv1.Pong{Id: b.Ping.GetId()}},
		})
	case *aetherv1.ClientMessage_Join:
		c.handleJoin(ctx, b.Join)
	case *aetherv1.ClientMessage_Leave:
		c.handleLeave(b.Leave)
	case *aetherv1.ClientMessage_Commit:
		c.handleCommit(ctx, b.Commit)
	case *aetherv1.ClientMessage_Broadcast:
		c.send(errorFrame("UNIMPLEMENTED", "the ephemeral tier lands in a later gateway PR"))
	default:
		c.send(errorFrame("INVALID", "empty or unknown frame"))
	}
}

// handleJoin serves a fresh Join: derive the client's stable id, resolve the room's owner, fetch
// the current snapshot, and reply Joined. (Live event relay and resume catch-up land next, in
// G6b/G9; FROZEN/retry on no-owner lands with routing, G10.)
func (c *conn) handleJoin(ctx context.Context, join *aetherv1.Join) {
	if join.GetSessionNonce() == "" {
		// Without a nonce, all of a principal's sessions collapse onto one client_id and their
		// client_seq counters collide in the owner's dedup space — silently dropping commits. Make
		// the session-separation contract a server-enforced requirement, not a client courtesy.
		c.send(errorFrame("INVALID", "session_nonce required"))
		return
	}
	clientID := deriveClientID(c.srv.secret, c.principal.ID, join.GetSessionNonce())
	// Pin the dedup identity on the FIRST Join. A later Join with a different nonce would shift
	// c.clientID mid-session, so a subsequent commit (even to an earlier room) would go out under a
	// different identity and a replay wouldn't dedup — breaking exactly-once. Reject the mismatch
	// rather than letting last-Join-win.
	if c.clientID == "" {
		c.clientID = clientID
	} else if clientID != c.clientID {
		c.send(errorFrame("INVALID", "session_nonce must match the connection's first Join"))
		return
	}

	owner, _, err := c.srv.locator.Owner(join.GetRoomId())
	if err != nil {
		c.send(errorFrame("UNAVAILABLE", "room has no reachable owner"))
		return
	}

	resp, err := owner.GetSnapshot(ctx, connect.NewRequest(&aetherv1.GetSnapshotRequest{
		RoomId: join.GetRoomId(),
	}))
	if err != nil {
		c.send(errorFrame("UNAVAILABLE", "could not fetch room snapshot"))
		return
	}

	c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Joined{Joined: &aetherv1.Joined{
		RoomId:     join.GetRoomId(),
		ClientId:   clientID,
		CurrentSeq: resp.Msg.GetRoomSeq(),
		Snapshot:   &aetherv1.Snapshot{RoomSeq: resp.Msg.GetRoomSeq(), State: resp.Msg.GetState()},
	}}})

	// Relay live events from just after the snapshot — no gap (Subscribe replays from from_seq),
	// no dup (snapshot is state ≤ room_seq, the stream is events > room_seq). A re-Join replaces
	// the prior relay.
	if cancel, ok := c.rooms[join.GetRoomId()]; ok {
		cancel()
	}
	relayCtx, cancel := context.WithCancel(ctx)
	c.rooms[join.GetRoomId()] = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.relay(relayCtx, join.GetRoomId(), owner, resp.Msg.GetRoomSeq())
	}()
}

// relay streams a room's events from its owner and forwards them to the client WS, until the relay
// context is cancelled (client disconnect or Leave) or the owner stream ends. If the stream dies
// for an owner-side reason (owner death — not the client leaving), it signals RoomStatus{FROZEN} so
// the client knows its live feed dropped and can re-Join, rather than freezing silently. (Automatic
// re-resolve + re-subscribe on owner failover lands with routing, G10; an owner *re-home* with the
// owner still alive is already covered — the owner's Tail self-heals via its poll-ticker.)
func (c *conn) relay(ctx context.Context, roomID string, owner aetherv1connect.RoomServiceClient, fromSeq uint64) {
	stream, err := owner.Subscribe(ctx, connect.NewRequest(&aetherv1.SubscribeRequest{
		RoomId: roomID, FromSeq: fromSeq,
	}))
	if err != nil {
		c.signalRoomFrozen(ctx, roomID)
		return
	}
	defer func() { _ = stream.Close() }()
	for stream.Receive() {
		c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Event{Event: stream.Msg().GetEvent()}})
	}
	c.signalRoomFrozen(ctx, roomID)
}

// signalRoomFrozen tells the client a room's live feed dropped — but only when the relay died for
// an owner-side reason. A cancelled relay context means the client left or disconnected (a clean
// end), so nothing is sent.
func (c *conn) signalRoomFrozen(ctx context.Context, roomID string) {
	if ctx.Err() != nil {
		return
	}
	c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_RoomStatus{RoomStatus: &aetherv1.RoomStatus{
		RoomId: roomID,
		Status: aetherv1.RoomStatus_STATUS_FROZEN,
	}}})
}

// handleLeave stops the live relay for a room — the client no longer wants its events.
func (c *conn) handleLeave(leave *aetherv1.Leave) {
	if cancel, ok := c.rooms[leave.GetRoomId()]; ok {
		cancel()
		delete(c.rooms, leave.GetRoomId())
	}
}

// handleCommit forwards a durable commit to the room's owner. The committed Event returns to the
// client via its relay (fan-out is the ack), so success is silent here — only a rejection or a
// failure produces a frame. A commit to a room the client hasn't joined is refused NOT_JOINED.
func (c *conn) handleCommit(ctx context.Context, commit *aetherv1.Commit) {
	room := commit.GetRoomId()
	if _, joined := c.rooms[room]; !joined {
		c.send(nackFrame(room, commit.GetClientSeq(), aetherv1.NackReason_NACK_REASON_NOT_JOINED))
		return
	}

	owner, addr, err := c.srv.locator.Owner(room)
	if err != nil {
		c.send(errorFrame("UNAVAILABLE", "room has no reachable owner"))
		return
	}

	resp, err := owner.Commit(ctx, connect.NewRequest(&aetherv1.CommitRequest{
		RoomId:    room,
		ClientId:  c.clientID,
		ClientSeq: commit.GetClientSeq(),
		Body:      commit.GetBody(),
	}))
	if err != nil {
		// Not (or no longer) the owner, or a transport failure: drop the dead client and signal
		// unavailable. Re-resolve + retry (and FROZEN) land with routing (G10).
		c.srv.locator.Invalidate(addr)
		c.send(errorFrame("UNAVAILABLE", "commit could not be routed to the owner"))
		return
	}
	if nack := resp.Msg.GetNack(); nack != nil {
		c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Nack{Nack: nack}})
	}
	// committed / duplicate: the Event reaches the client via its relay; nothing to send here.
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

func nackFrame(roomID string, clientSeq uint64, reason aetherv1.NackReason) *aetherv1.ServerMessage {
	return &aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Nack{Nack: &aetherv1.Nack{
		RoomId: roomID, ClientSeq: clientSeq, Reason: reason,
	}}}
}
