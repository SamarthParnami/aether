// Package fanout is the outbound delivery bus: a room's owner Publishes committed events,
// and subscribers (gateways, which then push to their connected clients) receive them.
//
// The real implementation is Redis Streams + sharded pub/sub; this interface keeps the
// room-runtime decoupled from it, and an in-memory implementation backs tests and
// single-node development. Delivery is per-room.
package fanout

import (
	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Subscription is a handle used to stop receiving a room's events.
type Subscription interface {
	// Cancel stops future deliveries to this subscriber. It is NOT synchronized against a
	// Publish already in flight: a delivery whose recipient set was captured before Cancel
	// returned may still invoke the handler once afterwards. Handlers must tolerate a
	// trailing event after Cancel.
	Cancel()
}

// Fanout delivers a room's events from its owner to all current subscribers.
//
// Delivery is LIVE-ONLY: an event published while a room has no subscriber is dropped —
// there is no backlog. Catch-up after a (re)subscribe is the durable log's job (logstore
// replay from a cursor), not the fan-out bus's. (The real Redis-Streams impl retains a
// window of history, but callers must not depend on that for correctness.)
type Fanout interface {
	// Publish delivers event to every current subscriber of roomID.
	Publish(roomID string, event *aetherv1.Event)

	// Subscribe registers fn to receive roomID's events until the returned Subscription is
	// cancelled. Subscribers are delivered to in subscription order.
	Subscribe(roomID string, fn func(*aetherv1.Event)) Subscription
}

// EphemeralFanout delivers a room's EPHEMERAL messages (cursors, presence, "typing…", reactions)
// from its owner to all current subscribers. It mirrors Fanout but is the strictly best-effort,
// lossy tier: ephemerals are never written to the durable log, so — unlike the event path — there
// is NO replay to repair a miss. An ephemeral published while a room has no subscriber is simply
// gone, and a (re)subscriber starts from "now" with no catch-up. This is by design: the ephemeral
// tier trades durability for the low latency a high-frequency signal (a moving cursor) needs.
//
// Keeping it a SEPARATE bus from the durable event Fanout is deliberate (05-design-gateway.md G8,
// Option B): the owner stays the room's single hub, but ephemeral traffic gets its own delivery
// path so a cursor flood cannot backpressure or reorder real committed events AT THE OWNER.
//
// This separation must be carried THROUGH the gateway or it is undone: both tiers' relays feed a
// client's single bounded out-queue (whose slow-client overflow disconnects the conn), so a cursor
// flood would crowd events out of that shared queue or trip the disconnect — re-coupling the two
// paths at the socket. So G8c's ephemeral relay must drop/coalesce ephemerals under out-queue
// pressure (design §9 — "ephemerals dropped first") rather than contend equally with events.
type EphemeralFanout interface {
	// Publish delivers eph to every current subscriber of roomID. Best-effort: no ack, no dedup,
	// no ordering guarantee across publishers.
	Publish(roomID string, eph *aetherv1.Ephemeral)

	// Subscribe registers fn to receive roomID's ephemerals until the returned Subscription is
	// cancelled. Subscribers are delivered to in subscription order.
	Subscribe(roomID string, fn func(*aetherv1.Ephemeral)) Subscription
}
