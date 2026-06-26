package coord

import "sync"

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
func (m *Memory) Claim(roomID, owner string, now, ttl uint64) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	held := exists && now < cur.Expiry

	switch {
	case !held:
		// Free (unowned or expired): take it as a fresh ownership and bump the token so any
		// stale writes from a previous owner are fenced out.
		l := Lease{Owner: owner, Expiry: now + ttl, Token: cur.Token + 1}
		m.leases[roomID] = l
		return l, true
	case cur.Owner == owner:
		// Re-claim by the current holder acts like a renew: extend, keep the token.
		l := Lease{Owner: owner, Expiry: now + ttl, Token: cur.Token}
		m.leases[roomID] = l
		return l, true
	default:
		// Held by another node, unexpired.
		return Lease{}, false
	}
}

// Renew implements Coordinator.
func (m *Memory) Renew(roomID, owner string, now, ttl uint64) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || cur.Owner != owner || now >= cur.Expiry {
		return Lease{}, false // lost
	}
	l := Lease{Owner: owner, Expiry: now + ttl, Token: cur.Token}
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
		cur.Expiry = 0 // expired now (now is always >= 0), but Token is preserved
		m.leases[roomID] = cur
	}
}

// Current implements Coordinator.
func (m *Memory) Current(roomID string, now uint64) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || now >= cur.Expiry {
		return Lease{}, false
	}
	return cur, true
}

var _ Coordinator = (*Memory)(nil)
