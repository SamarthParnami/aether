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
// It honours ctx like any other implementation: a cancelled context returns ctx.Err() rather than
// an answer. An in-memory map cannot BLOCK, but that is a different question from whether the
// caller is still entitled to the result — a cancelled context means the caller has given up
// however fast the answer would have been, which is why the stdlib checks ctx on fast paths too.
//
// It is also what keeps the fake and the durable adapter honest about the same contract. Memory is
// the DEFAULT coordinator (roomruntime.New installs it with no options), so a Memory that ignored
// ctx would leave every default-harness test blind to a caller passing a dead context — and
// requirement 2 on Coordinator would be a rule that the only in-tree implementation did not
// follow.
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
	ctx context.Context, roomID, owner, addr string, now time.Time, ttl time.Duration,
) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}

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
	ctx context.Context, roomID, owner string, now time.Time, ttl time.Duration,
) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}

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
func (m *Memory) Release(ctx context.Context, roomID, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, ok := m.leases[roomID]; ok && cur.Owner == owner {
		cur.Expiry = time.Time{} // zero time is before any real `now`, but Token is preserved
		m.leases[roomID] = cur
	}
	return nil
}

// Current implements Coordinator.
func (m *Memory) Current(ctx context.Context, roomID string, now time.Time) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	cur, exists := m.leases[roomID]
	if !exists || !now.Before(cur.Expiry) {
		return Lease{}, false, nil
	}
	return cur, true, nil
}

var _ Coordinator = (*Memory)(nil)
