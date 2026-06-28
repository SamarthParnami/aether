// Package roomruntime is the room owner: it wires the pure roomcore reducer to the durable
// log (logstore) and the delivery bus (fanout) to serve the write journey.
//
// The journey for a durable commit is: confirm ownership → dedup → assign room_seq → APPEND
// to the log and only then ("ack-after-persist") → fan out the committed event. The fan-out
// IS the ack: the event carries the originator's client_seq, so the sender recognizes its own
// commit coming back. A room's state is reconstructed by replaying its log, so any node can
// rebuild it — the basis of failover.
//
// Ownership (two layered guards):
//   - Soft: before serving a room a Runtime confirms a coord lease (claim-or-renew). A node
//     that cannot hold the lease refuses to act as owner (ErrNotOwner) and drops its in-memory
//     copy of the room. This avoids wasted work and lets a survivor take over once the dead
//     owner's lease lapses, rebuilding state from the log.
//   - Hard: the durable log's conditional Append (logstore.ErrConflict) still fails a write
//     from a stale owner even if the lease is briefly wrong — the backstop against split-brain.
//
// Lease renewal here is "claim-on-serve": each Commit/Join renews the lease, so a busy room
// stays owned and a quiet one's lease lapses (acceptable — the log is the truth; whoever next
// touches the room re-homes it). A background renewal loop (keeping quiet rooms pinned, reading
// a monotonic clock per the coord package note) is layered on later.
package roomruntime

import (
	"context"
	"errors"
	"sync"
	"time"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomcore"
)

// defaultLeaseTTL is the lease lifetime used when WithLeaseTTL is not supplied. It must be
// comfortably larger than the (later) renewal interval and the max inter-node clock skew.
const defaultLeaseTTL = 10 * time.Second

// defaultTailPollInterval is the coarse backstop at which Tail re-reads the durable log even with
// no fan-out wake. It is the correctness FLOOR beneath the wake buses (the local in-memory bus now;
// a shared Redis fan-out later): re-reading the log — which is independent of any bus — keeps a
// reader correct, with bounded staleness, whenever wakes don't arrive: a re-home (the new owner's
// commits fire ITS bus), a cross-node subscriber, a dropped pub/sub message, or a full Redis
// outage. The wake is the fast path; the poll only governs the degraded case, so it is deliberately
// coarse — cheap per stream at watcher scale.
const defaultTailPollInterval = 3 * time.Second

// ErrNotOwner is returned by Commit/Join when this node does not hold the room's ownership
// lease — another node is the live owner. The caller (gateway) should route to the real owner
// or retry after the lease lapses; it must not treat the operation as applied.
var ErrNotOwner = errors.New("roomruntime: not the room owner")

// Runtime owns a set of rooms on this node. Ownership is enforced per room via the coord
// lease; a Runtime serves only rooms whose lease it currently holds.
type Runtime struct {
	nodeID   string
	addr     string // this node's dialable RPC address, published with each lease claim
	log      logstore.LogStore
	fanout   fanout.Fanout
	coord    coord.Coordinator
	now      func() time.Time
	ttl      time.Duration
	tailPoll time.Duration // Tail log re-read interval (bounded staleness when wakeups are missing)

	// A single Runtime-wide mutex serializes all rooms, and is held across the durable append
	// — a deliberate single-node Phase-1 simplification. Per-room serialization (a per-room
	// lock or owner-actor goroutine) replaces it when this is sharded/scaled. Fan-out already
	// happens outside the lock (see Commit) so subscribers can't stall or deadlock it.
	mu    sync.Mutex
	rooms map[string]*roomcore.Room
}

// Option configures a Runtime. Unset options fall back to single-node defaults: a private
// in-memory coordinator, node id "local", the wall clock, and defaultLeaseTTL.
type Option func(*Runtime)

// WithNodeID sets this node's lease-owner identity. Each node in a cluster needs a distinct id.
func WithNodeID(id string) Option { return func(r *Runtime) { r.nodeID = id } }

// WithAddr sets this node's dialable RPC address, published into the lease on each claim so the
// directory can route gateways to the owner. Defaults to "" (single-node / tests don't route): an
// empty addr is a deliberately NON-routable owner, so the gateway's router must treat an empty
// Lease.Addr as "no owner" and re-resolve, turning a misconfigured node (forgot WithAddr) into a
// fast re-resolve rather than a silent black hole.
func WithAddr(addr string) Option { return func(r *Runtime) { r.addr = addr } }

// WithCoordinator injects the shared ownership coordinator. Nodes that can contend for the
// same room must share one coordinator (in prod, the DynamoDB-backed impl).
func WithCoordinator(co coord.Coordinator) Option { return func(r *Runtime) { r.coord = co } }

// WithClock injects the clock used to drive lease expiry. Defaults to time.Now; tests pass a
// virtual clock so failover timing is deterministic.
func WithClock(now func() time.Time) Option { return func(r *Runtime) { r.now = now } }

// WithLeaseTTL sets the lease lifetime acquired on each claim/renew.
func WithLeaseTTL(ttl time.Duration) Option { return func(r *Runtime) { r.ttl = ttl } }

// WithTailPollInterval sets how often Tail re-reads the log absent a fan-out wakeup — the bound on
// read staleness when the room is served by a node that isn't being woken (e.g. after a re-home).
// Defaults to defaultTailPollInterval.
func WithTailPollInterval(d time.Duration) Option { return func(r *Runtime) { r.tailPoll = d } }

// New returns a Runtime backed by the given log and fan-out bus. Without options it is a
// self-contained single-node owner (private coordinator) — every room it touches is its own.
func New(log logstore.LogStore, fo fanout.Fanout, opts ...Option) *Runtime {
	r := &Runtime{
		nodeID:   "local",
		log:      log,
		fanout:   fo,
		coord:    coord.NewMemory(),
		now:      time.Now,
		ttl:      defaultLeaseTTL,
		tailPoll: defaultTailPollInterval,
		rooms:    map[string]*roomcore.Room{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Commit processes a durable commit and returns the committed Event.
//
//   - applied:  a new commit — assigned room_seq, persisted, and fanned out (event != nil).
//   - !applied: a duplicate (replayed) commit — ignored, exactly-once (event == nil, err == nil).
//   - err:      ErrNotOwner if this node does not hold the room lease; otherwise persistence
//     failed. On a sequence conflict the in-memory room is dropped so it rebuilds from the log
//     (a conflict means this node is not really the owner).
func (r *Runtime) Commit(
	ctx context.Context, roomID, clientID string, clientSeq uint64, body *aetherv1.EventBody,
) (*aetherv1.Event, bool, error) {
	ev, applied, err := r.commitLocked(ctx, roomID, clientID, clientSeq, body)
	if applied {
		// Fan-out is the ack — published OUTSIDE the lock on purpose. Subscriber handlers run
		// synchronously, so publishing under r.mu would let a slow subscriber stall every room
		// and would deadlock a subscriber that calls back into Commit/Join (mutex is non-reentrant).
		r.fanout.Publish(roomID, ev)
	}
	return ev, applied, err
}

// commitLocked runs the durable critical section under the lock and returns the committed
// event without publishing it (the caller publishes after unlocking).
func (r *Runtime) commitLocked(
	ctx context.Context, roomID, clientID string, clientSeq uint64, body *aetherv1.EventBody,
) (*aetherv1.Event, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.acquire(roomID); err != nil {
		return nil, false, err
	}
	room, err := r.ensureRoom(ctx, roomID)
	if err != nil {
		return nil, false, err
	}

	ev, ok := room.Apply(clientID, clientSeq, body)
	if !ok {
		return nil, false, nil // duplicate — already applied
	}

	// Ack-after-persist: the event is real only once the durable, conditional append wins.
	if err := r.log.Append(ctx, roomID, ev.GetRoomSeq(), ev); err != nil {
		delete(r.rooms, roomID) // stale/lost ownership — discard so the next access rebuilds
		return nil, false, err
	}
	return ev, true, nil
}

// Join returns the room's current materialized state for catch-up: the latest room_seq and a
// snapshot a joining client adopts before applying any newer events. (Client-id assignment
// and the gap replay belong to the gateway/SDK, layered on later.)
//
// Join takes the ownership lease exactly like a write (claim-on-serve: a node can only
// materialize a room it owns). Two consequences for the caller: a gateway MUST route Join to
// the room's current owner (coord.Current) and never fan a join to an arbitrary node, or an
// idle bystander would seize ownership just by being asked to read; and concurrent joins to a
// quiet (unowned) room can briefly bounce ownership — each re-claims, the loser gets
// ErrNotOwner and retries. Acceptable for Phase 1; the gateway's owner-routing makes it a
// non-issue in practice.
func (r *Runtime) Join(ctx context.Context, roomID string) (*aetherv1.Joined, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.acquire(roomID); err != nil {
		return nil, err
	}
	room, err := r.ensureRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	// Snapshot() returns a CLONE — never hand out the room's live, mutable state, or a later
	// Commit's fold would race (fatal concurrent map read/write) a caller still reading it, and
	// the snapshot wouldn't actually be pinned at CurrentSeq.
	snap := room.Snapshot()
	return &aetherv1.Joined{
		RoomId:     roomID,
		CurrentSeq: snap.RoomSeq,
		Snapshot:   &aetherv1.Snapshot{RoomSeq: snap.RoomSeq, State: snap.State},
	}, nil
}

// Release gracefully relinquishes ownership of a room (e.g. on a planned shutdown) so a
// survivor can take over immediately instead of waiting out the lease TTL. The in-memory room
// is dropped; a later request re-homes it (re-claiming if the room is still free).
func (r *Runtime) Release(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.coord.Release(roomID, r.nodeID)
	delete(r.rooms, roomID)
}

// acquire confirms this node holds the room's ownership lease, claiming it if the room is free
// or expired and renewing it if already held. On failure another node is the live owner: the
// stale in-memory room is dropped and ErrNotOwner returned. Caller must hold r.mu.
//
// The granted Lease (and its fencing Token) is intentionally discarded: in Phase 1 the only
// thing fenced is the durable write, and that is fenced by the room_seq conditional Append, not
// the token — so the token is not load-bearing here. The token's eventual job is fencing the
// one durable write NOT conditioned on room_seq, logstore.WriteSnapshot (today an unconditional
// overwrite), so a zombie owner can't clobber a fresher snapshot with a stale one. We thread
// the token into snapshot writes when snapshots are added.
func (r *Runtime) acquire(roomID string) error {
	if _, ok := r.coord.Claim(roomID, r.nodeID, r.addr, r.now(), r.ttl); !ok {
		delete(r.rooms, roomID) // we no longer own it; don't serve from a stale copy
		return ErrNotOwner
	}
	return nil
}

// ensureRoom returns the in-memory room, rebuilding it from the durable log on first access.
// Caller must hold r.mu.
func (r *Runtime) ensureRoom(ctx context.Context, roomID string) (*roomcore.Room, error) {
	if room := r.rooms[roomID]; room != nil {
		return room, nil
	}
	events, err := r.log.Read(ctx, roomID, 0) // full log; snapshots are an optimization added later
	if err != nil {
		return nil, err
	}
	room := roomcore.RestoreAndReplay(roomcore.Snapshot{}, events)
	r.rooms[roomID] = room
	return room, nil
}
