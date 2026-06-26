package fanout

import (
	"sync"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Memory is an in-process Fanout for tests and single-node dev. Subscribers are kept in
// subscription order (a slice, not a map) so delivery order is deterministic — which
// matters when this is driven from the deterministic-simulation tests. Safe for
// concurrent use.
type Memory struct {
	mu   sync.Mutex
	subs map[string][]subscriber
	next int
}

type subscriber struct {
	id int
	fn func(*aetherv1.Event)
}

// NewMemory returns an empty in-memory fanout.
func NewMemory() *Memory {
	return &Memory{subs: map[string][]subscriber{}}
}

// Subscribe implements Fanout.
func (m *Memory) Subscribe(roomID string, fn func(*aetherv1.Event)) Subscription {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.next
	m.next++
	m.subs[roomID] = append(m.subs[roomID], subscriber{id: id, fn: fn})
	return &memSub{m: m, roomID: roomID, id: id}
}

// Publish implements Fanout. Handlers are invoked outside the lock (in subscription order)
// so a subscriber may itself publish or (un)subscribe without deadlocking. Each handler is
// isolated: a panicking subscriber is contained (recovered) so it can neither skip later
// subscribers nor unwind into the owner's commit path. (Fail-isolated, not fail-fast — one
// bad gateway handler must not take down delivery for a whole room.)
func (m *Memory) Publish(roomID string, event *aetherv1.Event) {
	m.mu.Lock()
	subs := m.subs[roomID]
	fns := make([]func(*aetherv1.Event), len(subs))
	for i, s := range subs {
		fns[i] = s.fn
	}
	m.mu.Unlock()

	for _, fn := range fns {
		deliver(fn, event)
	}
}

// deliver invokes one handler, containing any panic so it stays isolated to that subscriber.
func deliver(fn func(*aetherv1.Event), event *aetherv1.Event) {
	defer func() { _ = recover() }()
	fn(event)
}

type memSub struct {
	m      *Memory
	roomID string
	id     int
}

// Cancel implements Subscription.
func (s *memSub) Cancel() {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()

	subs := s.m.subs[s.roomID]
	for i, sub := range subs {
		if sub.id == s.id {
			subs = append(subs[:i], subs[i+1:]...)
			if len(subs) == 0 {
				delete(s.m.subs, s.roomID) // don't leave empty slices to leak the map entry
			} else {
				s.m.subs[s.roomID] = subs
			}
			return
		}
	}
}

var _ Fanout = (*Memory)(nil)
