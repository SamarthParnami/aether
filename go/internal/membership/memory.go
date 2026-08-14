package membership

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrInvalidNode is returned by Heartbeat for a node that could not be routed to if it won a
// placement. See NewView for the failure it prevents.
var ErrInvalidNode = errors.New("membership: node needs a non-empty ID and Addr")

// Memory is an in-memory Registry for tests and local development. Safe for concurrent use.
// The durable implementation — a second item type on the same DynamoDB table coord uses, with
// expiry filtered by the caller exactly as it is here — lands later behind this interface.
type Memory struct {
	mu    sync.Mutex
	nodes map[string]registration
}

type registration struct {
	node     Node
	expiry   time.Time
	draining bool
}

// NewMemory returns an empty in-memory registry.
func NewMemory() *Memory {
	return &Memory{nodes: map[string]registration{}}
}

// Heartbeat implements Registry. A node that heartbeats after deregistering is live again: the
// registration is replaced wholesale, which clears the draining mark. That is deliberate — a
// pod whose drain was aborted must be able to rejoin without an operator clearing state.
//
// It is also the un-drain trap described on Registry.Deregister: this revival cannot tell an
// aborted drain from a heartbeat loop that simply has not been stopped yet, and the mandated
// drain order guarantees such a heartbeat is in flight. See that comment before wiring P9.
func (m *Memory) Heartbeat(_ context.Context, n Node, now time.Time, ttl time.Duration) error {
	if n.ID == "" || n.Addr == "" {
		return fmt.Errorf("%w (got id=%q addr=%q)", ErrInvalidNode, n.ID, n.Addr)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[n.ID] = registration{node: n, expiry: now.Add(ttl)}
	return nil
}

// Deregister implements Registry. It marks the row drained rather than deleting it, mirroring
// coord.Release: a drain stays observable instead of being indistinguishable from a node that
// never registered.
//
// now is unused here — the durable impl writes an unconditional expiry of zero — but stays in
// the signature so an implementation that wants to record when a drain started can.
func (m *Memory) Deregister(_ context.Context, nodeID string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r, ok := m.nodes[nodeID]; ok {
		r.expiry = time.Time{} // zero time is before any real `now`
		r.draining = true
		m.nodes[nodeID] = r
	}
	return nil
}

// View implements Registry. Expiry is evaluated here, against the caller's now, rather than
// being left to the store's own reaping — the same rule coord.Current follows, and the one that
// keeps the durable port honest: DynamoDB TTL deletion lags by up to 48h, so it is garbage
// collection and must never carry expiry semantics.
func (m *Memory) View(_ context.Context, now time.Time) (View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	live := make([]Node, 0, len(m.nodes))
	for _, r := range m.nodes {
		if r.draining || !now.Before(r.expiry) {
			continue
		}
		live = append(live, r.node)
	}
	return NewView(live), nil // NewView sorts, so map iteration order does not leak out
}

var _ Registry = (*Memory)(nil)
