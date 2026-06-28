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

import "time"

// Lease is a time-bound ownership token for a room.
type Lease struct {
	Owner  string    // node id currently holding the room
	Addr   string    // owner's dialable RPC address, published atomically with the claim
	Expiry time.Time // instant at which the lease lapses
	Token  uint64    // fencing token; increments on every ownership takeover
}

// Coordinator manages room ownership and answers the directory lookup.
type Coordinator interface {
	// Claim attempts to acquire roomID for owner, publishing owner's dialable RPC address (addr)
	// atomically with the claim — so the directory never names an owner the gateway can't reach
	// (no "owns-but-not-dialable" window). It succeeds if the room is unowned, the existing lease
	// has expired, or owner already holds it. A takeover (acquiring a free/expired lease) bumps the
	// fencing token; a re-claim by the current holder keeps it and re-affirms addr. Returns the
	// granted lease and true, or a zero lease and false when another node holds an unexpired lease.
	Claim(roomID, owner, addr string, now time.Time, ttl time.Duration) (Lease, bool)

	// Renew extends owner's lease. Returns false (ownership lost) if owner is not the
	// current unexpired holder.
	Renew(roomID, owner string, now time.Time, ttl time.Duration) (Lease, bool)

	// Release relinquishes ownership if owner holds it — a graceful handoff on shutdown,
	// so a survivor can claim immediately instead of waiting out the TTL.
	Release(roomID, owner string)

	// Current returns the unexpired lease for a room: the directory lookup gateways use to
	// route, including the owner's Addr. Returns false if the room is unowned or its lease has
	// lapsed.
	Current(roomID string, now time.Time) (Lease, bool)
}
