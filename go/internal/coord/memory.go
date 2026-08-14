package coord

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-memory Coordinator for tests and local development. Safe for concurrent
// use. The durable implementation (DynamoDB conditional writes + TTL) lands later behind
// this same interface.
//
// Every method returns a nil error: an in-memory map cannot fail to answer. The error exists for
// the durable adapter, and tests that need one inject it with Faulty.
type Memory struct {
	mu     sync.Mutex
	leases map[string]Lease
}

// NewMemory returns an empty in-memory coordinator.
func NewMemory() *Memory {
	return &Memory{leases: map[string]Lease{}}
}

// Claim implements Coordinator.
func (m *Memory) Claim(
	_ context.Context, roomID, owner, addr string, now time.Time, ttl time.Duration,
) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	held := exists && now.Before(cur.Expiry)

	switch {
	case !held:
		// Free (unowned or expired): take it as a fresh ownership and bump the token so any
		// stale writes from a previous owner are fenced out.
		l := Lease{Owner: owner, Addr: addr, Expiry: now.Add(ttl), Token: cur.Token + 1}
		m.leases[roomID] = l
		return l, true, nil
	case cur.Owner == owner:
		// Re-claim by the current holder acts like a renew: extend, keep the token, re-affirm addr.
		l := Lease{Owner: owner, Addr: addr, Expiry: now.Add(ttl), Token: cur.Token}
		m.leases[roomID] = l
		return l, true, nil
	default:
		// Held by another node, unexpired.
		return Lease{}, false, nil
	}
}

// Renew implements Coordinator.
func (m *Memory) Renew(
	_ context.Context, roomID, owner string, now time.Time, ttl time.Duration,
) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || cur.Owner != owner || !now.Before(cur.Expiry) {
		return Lease{}, false, nil // lost
	}
	// Keep the published Addr — the serve path re-affirms it via Claim; a future background
	// renewal loop that wants to update addr should re-Claim rather than rely on Renew.
	l := Lease{Owner: owner, Addr: cur.Addr, Expiry: now.Add(ttl), Token: cur.Token}
	m.leases[roomID] = l
	return l, true, nil
}

// Release implements Coordinator. It marks the lease expired but RETAINS the fencing
// token (rather than deleting the entry) so the token stays monotonic across a
// release-then-reclaim — otherwise a later owner could reuse a lower token and fencing
// would break.
func (m *Memory) Release(_ context.Context, roomID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, ok := m.leases[roomID]; ok && cur.Owner == owner {
		cur.Expiry = time.Time{} // zero time is before any real `now`, but Token is preserved
		m.leases[roomID] = cur
	}
	return nil
}

// Current implements Coordinator.
func (m *Memory) Current(_ context.Context, roomID string, now time.Time) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || !now.Before(cur.Expiry) {
		return Lease{}, false, nil
	}
	return cur, true, nil
}

var _ Coordinator = (*Memory)(nil)
