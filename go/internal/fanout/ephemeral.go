package fanout

import (
	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// MemoryEphemeral is an in-process EphemeralFanout for tests and single-node dev, backed by the
// same generic per-room bus as the event Memory — so it inherits identical subscription-order,
// panic-isolation, and live-only semantics. The only difference is the message type: there is no
// durable backstop behind it, so a published ephemeral with no live subscriber is dropped, by
// design. Safe for concurrent use.
type MemoryEphemeral struct {
	bus *bus[*aetherv1.Ephemeral]
}

// NewMemoryEphemeral returns an empty in-memory ephemeral fanout.
func NewMemoryEphemeral() *MemoryEphemeral {
	return &MemoryEphemeral{bus: newBus[*aetherv1.Ephemeral]()}
}

// Subscribe implements EphemeralFanout.
func (m *MemoryEphemeral) Subscribe(roomID string, fn func(*aetherv1.Ephemeral)) Subscription {
	return m.bus.subscribe(roomID, fn)
}

// Publish implements EphemeralFanout.
func (m *MemoryEphemeral) Publish(roomID string, eph *aetherv1.Ephemeral) {
	m.bus.publish(roomID, eph)
}

var _ EphemeralFanout = (*MemoryEphemeral)(nil)
