package fanout

import (
	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Memory is an in-process Fanout for tests and single-node dev, backed by the generic per-room
// bus. Delivery is in subscription order, panic-isolated, and live-only (see bus). Safe for
// concurrent use.
type Memory struct {
	bus *bus[*aetherv1.Event]
}

// NewMemory returns an empty in-memory fanout.
func NewMemory() *Memory { return &Memory{bus: newBus[*aetherv1.Event]()} }

// Subscribe implements Fanout.
func (m *Memory) Subscribe(roomID string, fn func(*aetherv1.Event)) Subscription {
	return m.bus.subscribe(roomID, fn)
}

// Publish implements Fanout.
func (m *Memory) Publish(roomID string, event *aetherv1.Event) { m.bus.publish(roomID, event) }

var _ Fanout = (*Memory)(nil)
