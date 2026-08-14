// Package coord is the room-ownership coordination layer: leases plus the room->owner
// directory.
//
// Exactly one node owns a room at a time. The lease is fail-safe — a node that cannot
// confirm ownership (Renew returns false) must stop acting as owner. The lease is the
// *soft* coordination that decides who should own a room and lets gateways route to it;
// the durable log's conditional write (logstore.ErrConflict) is the *hard* backstop that
// makes split-brain writes fail even if the lease is briefly wrong.
//
// Time (`now`) and the TTL are passed in explicitly so the layer is deterministic and
// testable — there is no hidden wall clock. Using time.Time / time.Duration (rather than an
// abstract unit) means callers express TTLs the same way in tests and prod (e.g. 6*time.Second)
// and drive `now` from the sim clock under test or the wall clock in prod, with no converter.
//
// Clock skew: in prod `now` is the wall clock, so a lease's Expiry is set by the owner's
// clock but evaluated by a survivor's on failover. Failover *timing* therefore assumes
// TTL >> max inter-node clock skew. This is not a safety hole — the hard backstop above
// (conditional write + fencing token) still prevents a double-owner write; skew can only
// widen the failover-detection window. Lease *renewal scheduling* should read a monotonic
// clock; only the expiry comparisons here use these wall-clock instants.
package coord

import (
	"context"
	"time"
)

// Lease is a time-bound ownership token for a room.
type Lease struct {
	Owner  string    // node id currently holding the room
	Addr   string    // owner's dialable RPC address, published atomically with the claim
	Expiry time.Time // instant at which the lease lapses
	Token  uint64    // fencing token; increments on every ownership takeover
}

// Coordinator manages room ownership and answers the directory lookup.
//
// Every method takes a ctx and returns an error, and the bool and the error are INDEPENDENT: the
// bool is what the store said (held / not held), the error is that the store did not answer at
// all. A durable adapter must never fold the second into the first — a DynamoDB throttle or a
// timeout rendered as "not held" is indistinguishable from "this room is unowned", and routing
// reads "unowned" as an invitation to place the room somewhere. That would aim a claim storm at a
// store already failing. Callers check err FIRST and freeze on ambiguity; only a nil error makes
// the bool meaningful (01-design-backbone.md:240).
//
// # Two requirements on any durable implementation
//
// 1. BOUND EVERY CALL, well under the lease TTL. Not a suggestion: roomruntime.acquire calls Claim
// as the first thing inside the Runtime-wide mutex, on the Join, Commit, Tail, TailEphemeral and
// Broadcast paths. That lock is a documented Phase-1 simplification accepted for logstore.Append,
// which fails fast and is conditional per room — a coord brownout is the opposite: slow, node-wide,
// and correlated across every room at once. An unbounded retry budget inside that lock converts a
// store brownout into a full node stall, and this layer's whole thesis is that ambiguity should
// freeze rather than storm — which assumes the node can still answer in order to say "frozen".
// Convoyed behind one mutex it cannot. The timeout belongs to the implementation; the requirement
// belongs here, where the interface is set.
//
// 2. HONOUR ctx. A cancelled context must produce an error, never a silent success. Memory checks
// ctx for this reason even though an in-memory map cannot block: a cancelled context means the
// caller has given up however fast the answer would have been, and a fake that ignored it would
// leave the suite blind to callers passing an already-dead context.
//
// 3. Lease.Expiry IS THE CALLER'S CLOCK plus the requested ttl — never a server-side timestamp.
// Callers compare it against their own now to decide whether they still hold a room, so an expiry
// minted from the store's clock would skew that comparison by the full inter-node offset. Computing
// it server-side is the more natural thing to write in a DynamoDB adapter, and Memory's now.Add(ttl)
// makes the coupling invisible until then, which is why it is stated rather than left to be
// inferred.
type Coordinator interface {
	// Claim attempts to acquire roomID for owner, publishing owner's dialable RPC address (addr)
	// atomically with the claim — so the directory never names an owner the gateway can't reach
	// (no "owns-but-not-dialable" window). It succeeds if the room is unowned, the existing lease
	// has expired, or owner already holds it. A takeover (acquiring a free/expired lease) bumps the
	// fencing token; a re-claim by the current holder keeps it and re-affirms addr. Returns the
	// granted lease and true, or a zero lease and false when another node holds an unexpired lease.
	// A non-nil error means the claim's outcome is UNKNOWN — the caller has not lost the room and
	// must not drop it; it must not treat the false as "another node owns this".
	Claim(ctx context.Context, roomID, owner, addr string, now time.Time, ttl time.Duration) (Lease, bool, error)

	// Renew extends owner's lease. Returns false (ownership lost) if owner is not the
	// current unexpired holder.
	//
	// RESERVED — it has no production call site and is not expected to gain one. Ownership is
	// renewed by claim-on-serve (Claim is idempotent for the current holder and re-affirms addr),
	// so a reader should not assume a renew path exists. It is kept for a future background
	// renewal loop that pins quiet rooms; such a loop would still re-Claim if it needs to update
	// addr (see Memory.Renew).
	Renew(ctx context.Context, roomID, owner string, now time.Time, ttl time.Duration) (Lease, bool, error)

	// Release relinquishes ownership if owner holds it — a graceful handoff on shutdown,
	// so a survivor can claim immediately instead of waiting out the TTL. An error means the
	// release may not have landed: the lease then lapses on its own TTL, which is the slow path
	// Release exists to avoid, so callers should surface it rather than discard it.
	Release(ctx context.Context, roomID, owner string) error

	// Current returns the unexpired lease for a room: the directory lookup gateways use to
	// route, including the owner's Addr. Returns false if the room is unowned or its lease has
	// lapsed. A non-nil error means the directory could not be read — NOT that the room is
	// unowned. This is the distinction the whole interface change exists to make.
	Current(ctx context.Context, roomID string, now time.Time) (Lease, bool, error)
}
