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
	maxFrameBytes      = 1 << 20 // per-frame read cap (1 MiB)
	writeTimeout       = 10 * time.Second
	outQueue           = 64                     // buffered outbound frames per connection
	opsQueue           = 64                     // buffered inbound room frames awaiting the ops worker
	ephemeralOutLimit  = outQueue * 3 / 4       // ephemerals may fill up to here; the top reserves room for events
	relayRetryInterval = 200 * time.Millisecond // backoff between owner re-resolve attempts during failover
	pingInterval       = 30 * time.Second
	pingTimeout        = 10 * time.Second
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
		ops:       make(chan *aetherv1.ClientMessage, opsQueue),
		rooms:     map[string]context.CancelFunc{},
	}).run(r.Context())
}

// conn is one client WebSocket: a read loop decoding ClientMessage frames, an ops worker that
// serves room frames off the read loop, a single writer goroutine encoding ServerMessage frames (a
// WS permits only one concurrent writer), and a ping keepalive. All share a context that any one
// cancels on exit, so the whole connection tears down together instead of leaking a goroutine, the
// socket, or the TCP conn.
//
// The read loop only decodes and ENQUEUES room frames (answering Ping inline); the ops worker
// drains that queue and runs the handlers. This keeps the read loop draining the socket — so WS
// keepalive and the app Ping stay responsive — even while a handler blocks on a slow owner RPC.
// One worker preserves per-connection arrival order (a client's commits stay client_seq ordered).
type conn struct {
	srv       *Server
	principal Principal
	clientID  string // derived at Join (HMAC of principal+nonce); the dedup identity for commits
	ws        *websocket.Conn
	out       chan *aetherv1.ServerMessage
	ops       chan *aetherv1.ClientMessage // decoded room frames awaiting the worker (Ping excluded)
	cancel    context.CancelFunc

	wg    sync.WaitGroup                // writeLoop + pingLoop + opsLoop + per-room relays
	rooms map[string]context.CancelFunc // joined room -> its relay's cancel (ops-worker goroutine only)
}

func (c *conn) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()

	c.ws.SetReadLimit(maxFrameBytes)

	// The background loops cancel the shared context on exit: a wedged writer or a missed pong then
	// unblocks the read loop (and any send) rather than deadlocking it. The ops worker only exits on
	// cancellation, so its cancel is a no-op — included for symmetry.
	c.wg.Add(3)
	go func() { defer c.wg.Done(); defer cancel(); c.writeLoop(ctx) }()
	go func() { defer c.wg.Done(); defer cancel(); c.pingLoop(ctx) }()
	go func() { defer c.wg.Done(); defer cancel(); c.opsLoop(ctx) }()

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
		// Answer Ping INLINE so app-level keepalive stays responsive even while the worker is busy
		// on a slow owner RPC; hand every other frame to the worker so the read loop keeps draining
		// the socket (WS keepalive, slow-owner isolation).
		if ping := m.GetPing(); ping != nil {
			c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Pong{Pong: &aetherv1.Pong{Id: ping.GetId()}}})
			continue
		}
		c.enqueue(&m)
	}
}

// enqueue hands a decoded room frame to the ops worker without blocking the read loop. On a full
// queue the outcome depends on the tier: an ephemeral Broadcast is droppable (design §9 — ephemerals
// are dropped first under load), so a cursor flood is dropped, not fatal; a durable frame is not
// droppable, so an over-producing client is disconnected (it resumes from lastSeq on reconnect)
// rather than blocking the read loop and starving WS keepalive.
func (c *conn) enqueue(m *aetherv1.ClientMessage) {
	select {
	case c.ops <- m:
		return
	default:
	}
	if _, ok := m.GetBody().(*aetherv1.ClientMessage_Broadcast); ok {
		return // drop the ephemeral broadcast
	}
	c.cancel()
}

// opsLoop serves room frames in arrival order, OFF the read loop, so a slow owner RPC (a Join
// snapshot, a Commit) can't stall frame reading. A single worker preserves per-connection order.
func (c *conn) opsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.ops:
			c.dispatch(ctx, m)
		}
	}
}

// dispatch serves one room frame on the ops worker. Ping is handled inline on the read loop, so it
// does not appear here. The ephemeral Broadcast tier is UNIMPLEMENTED until its gateway PR (G8c).
func (c *conn) dispatch(ctx context.Context, m *aetherv1.ClientMessage) {
	switch b := m.GetBody().(type) {
	case *aetherv1.ClientMessage_Join:
		c.handleJoin(ctx, b.Join)
	case *aetherv1.ClientMessage_Leave:
		c.handleLeave(b.Leave)
	case *aetherv1.ClientMessage_Commit:
		c.handleCommit(ctx, b.Commit)
	case *aetherv1.ClientMessage_Broadcast:
		c.handleBroadcast(ctx, b.Broadcast)
	default:
		c.send(errorFrame("INVALID", "empty or unknown frame"))
	}
}

// handleJoin serves a Join: derive the client's stable id, resolve the room's owner, and start the
// paired event + ephemeral relays. from_seq selects the catch-up strategy:
//   - from_seq == 0 (fresh): fetch the snapshot and reply Joined carrying it; relay events after it.
//   - from_seq  > 0 (resume): the client already holds state up to its cursor, so SKIP the snapshot
//     and relay only the gap (events > from_seq) then live — the cheap reconnect path. The owner's
//     Subscribe replays the gap from the log, so it's correct even across a gateway change or
//     re-home. (A from_seq below the log floor will deep-resume via snapshot once compaction exists;
//     no floor today, so every cursor is replayable — see ownerrpc Subscribe's OUT_OF_RANGE TODO.)
//
// FROZEN/retry on no-owner lands with routing (G10).
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

	room := join.GetRoomId()
	owner, _, err := c.srv.locator.Owner(room)
	if err != nil {
		c.send(errorFrame("UNAVAILABLE", "room has no reachable owner"))
		return
	}

	// fromSeq is the cursor the relay streams events after. Fresh join sets it from the snapshot;
	// resume takes the client's supplied cursor and skips the snapshot entirely.
	fromSeq := join.GetFromSeq()
	if fromSeq == 0 {
		resp, err := owner.GetSnapshot(ctx, connect.NewRequest(&aetherv1.GetSnapshotRequest{RoomId: room}))
		if err != nil {
			c.send(errorFrame("UNAVAILABLE", "could not fetch room snapshot"))
			return
		}
		fromSeq = resp.Msg.GetRoomSeq()
		c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Joined{Joined: &aetherv1.Joined{
			RoomId:     room,
			ClientId:   clientID,
			CurrentSeq: fromSeq,
			Snapshot:   &aetherv1.Snapshot{RoomSeq: fromSeq, State: resp.Msg.GetState()},
		}}})
	} else {
		// Resume: no snapshot — the client keeps its state and the relay fills the gap from its cursor.
		c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Joined{Joined: &aetherv1.Joined{
			RoomId:     room,
			ClientId:   clientID,
			CurrentSeq: fromSeq,
		}}})
	}

	// Relay events strictly after fromSeq — no gap (Subscribe replays from the cursor), no dup
	// (fresh: snapshot is state ≤ fromSeq; resume: client already holds ≤ fromSeq). A re-Join
	// replaces the prior relay.
	if cancel, ok := c.rooms[room]; ok {
		cancel()
	}
	relayCtx, cancel := context.WithCancel(ctx)
	c.rooms[room] = cancel
	// One supervisor goroutine owns the room's live feed: it (re)subscribes the event stream and the
	// PAIRED ephemeral stream, and on an owner-side failure re-resolves the owner and re-subscribes
	// both from the cursor — signalling FROZEN→LIVE. A single Leave/re-Join cancels it.
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.relay(relayCtx, room, fromSeq) }()
}

// relay supervises a room's live feed with auto-recovery, until ctx is cancelled (client Leave or
// disconnect). Each iteration resolves the room's current owner and streams its events (and the
// paired ephemeral feed); when an owner-side failure ends the stream — owner death, a re-home to a
// node we can't yet see — it signals RoomStatus{FROZEN}, re-resolves the owner, and re-subscribes
// from the cursor (the last room_seq delivered), emitting RoomStatus{LIVE} once delivery resumes.
//
// Recovery is gap-free because the cursor drives the re-subscribe and the owner replays from the
// durable log; it works across an owner change because the room's events are the shared log, not
// gateway- or owner-local state. (An owner re-home with the owner still alive needs no recovery here
// — the owner's Tail self-heals via its poll-ticker; this loop covers the cases that actually break
// the stream.) The ephemeral feed is re-subscribed as a unit with the event feed each attempt (it
// has no independent re-home signal — see roomruntime.TailEphemeral).
func (c *conn) relay(ctx context.Context, roomID string, fromSeq uint64) {
	cursor := fromSeq
	frozen := false
	for {
		if ctx.Err() != nil {
			return
		}
		owner, addr, err := c.srv.locator.Owner(roomID)
		if err != nil {
			// No reachable owner right now (lease lapsed / mid re-home) — freeze and retry.
			c.freeze(ctx, roomID, &frozen)
			if !waitRetry(ctx) {
				return
			}
			continue
		}

		cursor = c.streamRoom(ctx, roomID, owner, cursor, &frozen)
		if ctx.Err() != nil {
			return // clean shutdown — client left
		}
		// The stream ended for an owner-side reason: drop the stale owner so the next resolve re-dials.
		c.srv.locator.Invalidate(addr)
		c.freeze(ctx, roomID, &frozen)
		if !waitRetry(ctx) {
			return
		}
	}
}

// streamRoom subscribes to one owner's event stream and the paired ephemeral stream, forwarding both
// to the client until the event stream ends or ctx is cancelled. A successful (re)subscribe while
// frozen emits RoomStatus{LIVE} and clears *frozen. It returns the highest room_seq delivered so the
// supervisor can re-subscribe without a gap.
//
// The ephemeral relay is started FIRST, before owner.Subscribe: a Connect server-stream blocks the
// Subscribe call until the owner's first send, which for a quiet room never comes — so spawning the
// ephemeral relay after it would stall cursors/presence until the first commit. Starting it first
// lets ephemerals flow immediately on join.
func (c *conn) streamRoom(
	ctx context.Context, roomID string, owner aetherv1connect.RoomServiceClient, cursor uint64, frozen *bool,
) uint64 {
	// Pair the ephemeral relay to this attempt: re-subscribed with the event stream, torn down with it.
	attemptCtx, attemptCancel := context.WithCancel(ctx)
	defer attemptCancel()
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.ephemeralRelay(attemptCtx, roomID, owner) }()

	stream, err := owner.Subscribe(ctx, connect.NewRequest(&aetherv1.SubscribeRequest{
		RoomId: roomID, FromSeq: cursor,
	}))
	if err != nil {
		return cursor
	}
	defer func() { _ = stream.Close() }()

	if *frozen {
		c.signalRoomStatus(ctx, roomID, aetherv1.RoomStatus_STATUS_LIVE)
		*frozen = false
	}

	for stream.Receive() {
		ev := stream.Msg().GetEvent()
		c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Event{Event: ev}})
		cursor = ev.GetRoomSeq()
	}
	return cursor
}

// ephemeralRelay streams a room's live ephemerals from its owner to the client WS until the relay
// ctx is cancelled or the stream ends. Paired with the event relay (started together at Join). It
// does NOT signal FROZEN: the ephemeral tier gives no independent re-home signal (on re-home it just
// goes silent), so the event relay is what detects a dropped feed and drives re-subscribing both
// (G10). Frames go via sendEphemeral, which drops under outbound-queue pressure rather than
// disconnecting — so a cursor flood can't starve committed events on the shared socket (design §9).
func (c *conn) ephemeralRelay(ctx context.Context, roomID string, owner aetherv1connect.RoomServiceClient) {
	stream, err := owner.SubscribeEphemeral(ctx, connect.NewRequest(&aetherv1.SubscribeEphemeralRequest{
		RoomId: roomID,
	}))
	if err != nil {
		return // best-effort; the paired event relay surfaces a dropped feed
	}
	defer func() { _ = stream.Close() }()
	for stream.Receive() {
		c.sendEphemeral(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_Ephemeral{Ephemeral: stream.Msg().GetEphemeral()}})
	}
}

// signalRoomStatus reports a room's live-feed status (LIVE | FROZEN) to the client, unless the relay
// ctx is already cancelled — a clean client Leave/disconnect, which needs no signal.
func (c *conn) signalRoomStatus(ctx context.Context, roomID string, status aetherv1.RoomStatus_Status) {
	if ctx.Err() != nil {
		return
	}
	c.send(&aetherv1.ServerMessage{Body: &aetherv1.ServerMessage_RoomStatus{RoomStatus: &aetherv1.RoomStatus{
		RoomId: roomID,
		Status: status,
	}}})
}

// freeze emits RoomStatus{FROZEN} once on the live→frozen transition (idempotent while frozen, so a
// flapping owner doesn't spam the client).
func (c *conn) freeze(ctx context.Context, roomID string, frozen *bool) {
	if *frozen {
		return
	}
	c.signalRoomStatus(ctx, roomID, aetherv1.RoomStatus_STATUS_FROZEN)
	*frozen = true
}

// waitRetry sleeps one retry interval, returning false if ctx is cancelled meanwhile (so the relay
// stops promptly on disconnect instead of waiting out the backoff).
func waitRetry(ctx context.Context) bool {
	t := time.NewTimer(relayRetryInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
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

// handleBroadcast forwards an ephemeral broadcast to the room's owner. Best-effort: there is no ack
// and no Nack tier, so a transient failure just drops the ephemeral (no disconnect over lossy data).
// A broadcast to a room the client hasn't joined is a usage error and gets an Error frame. The
// committed Event is fanned to subscribers by the owner; this connection sees it via its own
// ephemeral relay (set up at Join), like any other subscriber.
func (c *conn) handleBroadcast(ctx context.Context, b *aetherv1.Broadcast) {
	room := b.GetRoomId()
	if _, joined := c.rooms[room]; !joined {
		c.send(errorFrame("INVALID", "broadcast to a room the client has not joined"))
		return
	}

	owner, addr, err := c.srv.locator.Owner(room)
	if err != nil {
		return // no reachable owner — drop the ephemeral (best-effort)
	}
	if _, err := owner.Broadcast(ctx, connect.NewRequest(&aetherv1.BroadcastRequest{
		RoomId:         room,
		OriginClientId: c.clientID,
		Body:           b.GetBody(),
	})); err != nil {
		c.srv.locator.Invalidate(addr) // stale owner — drop the ephemeral, re-resolve next time
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

// sendEphemeral enqueues an ephemeral frame but DROPS it under outbound-queue pressure instead of
// disconnecting (unlike send). Per design §9 ephemerals are dropped first under load, so a cursor
// flood can neither starve committed events on the shared queue nor trip the slow-client disconnect.
func (c *conn) sendEphemeral(m *aetherv1.ServerMessage) {
	if len(c.out) >= ephemeralOutLimit {
		return // reserve the top of the queue for events; drop the ephemeral (lossy by design)
	}
	select {
	case c.out <- m:
	default: // racing fill — drop, never disconnect on an ephemeral
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
