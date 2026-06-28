package coord

import (
	"sync"
	"time"
)

// Memory is an in-memory Coordinator for tests and local development. Safe for concurrent
// use. The durable implementation (DynamoDB conditional writes + TTL) lands later behind
// this same interface.
type Memory struct {
	mu     sync.Mutex
	leases map[string]Lease
}

// NewMemory returns an empty in-memory coordinator.
func NewMemory() *Memory {
	return &Memory{leases: map[string]Lease{}}
}

// Claim implements Coordinator.
func (m *Memory) Claim(roomID, owner, addr string, now time.Time, ttl time.Duration) (Lease, bool) {
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
		return l, true
	case cur.Owner == owner:
		// Re-claim by the current holder acts like a renew: extend, keep the token, re-affirm addr.
		l := Lease{Owner: owner, Addr: addr, Expiry: now.Add(ttl), Token: cur.Token}
		m.leases[roomID] = l
		return l, true
	default:
		// Held by another node, unexpired.
		return Lease{}, false
	}
}

// Renew implements Coordinator.
func (m *Memory) Renew(roomID, owner string, now time.Time, ttl time.Duration) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || cur.Owner != owner || !now.Before(cur.Expiry) {
		return Lease{}, false // lost
	}
	// Keep the published Addr — the serve path re-affirms it via Claim; a future background
	// renewal loop that wants to update addr should re-Claim rather than rely on Renew.
	l := Lease{Owner: owner, Addr: cur.Addr, Expiry: now.Add(ttl), Token: cur.Token}
	m.leases[roomID] = l
	return l, true
}

// Release implements Coordinator. It marks the lease expired but RETAINS the fencing
// token (rather than deleting the entry) so the token stays monotonic across a
// release-then-reclaim — otherwise a later owner could reuse a lower token and fencing
// would break.
func (m *Memory) Release(roomID, owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, ok := m.leases[roomID]; ok && cur.Owner == owner {
		cur.Expiry = time.Time{} // zero time is before any real `now`, but Token is preserved
		m.leases[roomID] = cur
	}
}

// Current implements Coordinator.
func (m *Memory) Current(roomID string, now time.Time) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || !now.Before(cur.Expiry) {
		return Lease{}, false
	}
	return cur, true
}

var _ Coordinator = (*Memory)(nil)
