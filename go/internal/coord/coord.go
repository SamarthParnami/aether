// Package coord is the room-ownership coordination layer: leases plus the room->owner
// directory.
//
// Exactly one node owns a room at a time. The lease is fail-safe — a node that cannot
// confirm ownership (Renew returns false) must stop acting as owner. The lease is the
// *soft* coordination that decides who should own a room and lets gateways route to it;
// the durable log's conditional write (logstore.ErrConflict) is the *hard* backstop that
// makes split-brain writes fail even if the lease is briefly wrong.
//
// Time is passed in explicitly as `now` (same logical unit as the TTLs) so the layer is
// deterministic and testable — there is no hidden wall clock.
package coord

// Lease is a time-bound ownership token for a room.
type Lease struct {
	Owner  string // node id currently holding the room
	Expiry uint64 // logical time at which the lease lapses
	Token  uint64 // fencing token; increments on every ownership takeover
}

// Coordinator manages room ownership and answers the directory lookup.
type Coordinator interface {
	// Claim attempts to acquire roomID for owner. It succeeds if the room is unowned, the
	// existing lease has expired, or owner already holds it. A takeover (acquiring a
	// free/expired lease) bumps the fencing token; a re-claim by the current holder keeps
	// it. Returns the granted lease and true, or a zero lease and false when another node
	// holds an unexpired lease.
	Claim(roomID, owner string, now, ttl uint64) (Lease, bool)

	// Renew extends owner's lease. Returns false (ownership lost) if owner is not the
	// current unexpired holder.
	Renew(roomID, owner string, now, ttl uint64) (Lease, bool)

	// Release relinquishes ownership if owner holds it — a graceful handoff on shutdown,
	// so a survivor can claim immediately instead of waiting out the TTL.
	Release(roomID, owner string)

	// Current returns the unexpired lease for a room: the directory lookup gateways use to
	// route. Returns false if the room is unowned or its lease has lapsed.
	Current(roomID string, now uint64) (Lease, bool)
}
