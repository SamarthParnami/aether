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
	Cancel()
}

// Fanout delivers a room's events from its owner to all current subscribers.
type Fanout interface {
	// Publish delivers event to every current subscriber of roomID.
	Publish(roomID string, event *aetherv1.Event)

	// Subscribe registers fn to receive roomID's events until the returned Subscription is
	// cancelled. Subscribers are delivered to in subscription order.
	Subscribe(roomID string, fn func(*aetherv1.Event)) Subscription
}
