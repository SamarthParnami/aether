package fanout

import "sync"

// bus is the shared per-room delivery mechanism behind both tiers: the durable event Fanout and
// the best-effort EphemeralFanout. It is generic over the message type T delivered (*Event or
// *Ephemeral) so the two tiers share one carefully-behaved implementation rather than drifting.
//
// Behaviour (identical for both tiers): subscribers are kept in subscription order (a slice, not a
// map) so delivery order is deterministic — which matters when this is driven from the
// deterministic-simulation tests; handlers are invoked OUTSIDE the lock and panic-isolated; and
// delivery is LIVE-ONLY (a message published to a room with no subscriber is dropped, no backlog).
// Safe for concurrent use.
type bus[T any] struct {
	mu   sync.Mutex
	subs map[string][]busSub[T]
	next int
}

type busSub[T any] struct {
	id int
	fn func(T)
}

func newBus[T any]() *bus[T] { return &bus[T]{subs: map[string][]busSub[T]{}} }

func (b *bus[T]) subscribe(roomID string, fn func(T)) Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.next
	b.next++
	b.subs[roomID] = append(b.subs[roomID], busSub[T]{id: id, fn: fn})
	return &busSubscription[T]{bus: b, roomID: roomID, id: id}
}

// publish snapshots the recipient set under the lock, then invokes handlers outside it (in
// subscription order) so a subscriber may itself publish or (un)subscribe without deadlocking.
// Each handler is isolated: a panicking subscriber is contained so it can neither skip later
// subscribers nor unwind into the publisher (the owner's commit / broadcast path).
func (b *bus[T]) publish(roomID string, msg T) {
	b.mu.Lock()
	subs := b.subs[roomID]
	fns := make([]func(T), len(subs))
	for i, s := range subs {
		fns[i] = s.fn
	}
	b.mu.Unlock()

	for _, fn := range fns {
		deliver(fn, msg)
	}
}

// deliver invokes one handler, containing any panic so it stays isolated to that subscriber.
func deliver[T any](fn func(T), msg T) {
	defer func() { _ = recover() }()
	fn(msg)
}

type busSubscription[T any] struct {
	bus    *bus[T]
	roomID string
	id     int
}

// Cancel implements Subscription. It is NOT synchronized against a publish already in flight: a
// delivery whose recipient set was captured before Cancel returned may still invoke the handler
// once afterwards (handlers must tolerate a trailing message, per Subscription's contract).
func (s *busSubscription[T]) Cancel() {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()

	subs := s.bus.subs[s.roomID]
	for i, sub := range subs {
		if sub.id == s.id {
			subs = append(subs[:i], subs[i+1:]...)
			if len(subs) == 0 {
				delete(s.bus.subs, s.roomID) // don't leave empty slices to leak the map entry
			} else {
				s.bus.subs[s.roomID] = subs
			}
			return
		}
	}
}
