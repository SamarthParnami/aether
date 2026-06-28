package roomruntime

import (
	"context"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Tail streams a room's committed events to send in strict room_seq order, gap-free, starting just
// after fromSeq: it replays history from the durable log and then continues live. It blocks until
// send returns an error or ctx is cancelled. This is the owner side of the gateway's Subscribe RPC
// — one call serves both resume catch-up and the live tail.
//
// The fan-out bus is used ONLY as a wakeup signal; the events themselves are always read from the
// log (the ordered source of truth). So the stream is inherently immune to fan-out reordering (the
// owner publishes outside the commit lock, so concurrent commits can fan out 6 before 5) and to a
// dropped or duplicated delivery — at worst a spurious wakeup costs one extra, empty log read. This
// is simpler than forwarding fan-out events and repairing their order, with the same guarantee.
//
// Ownership is confirmed once at the start: a non-owner returns ErrNotOwner so the caller (gateway)
// re-resolves the owner. The stream then runs without holding the room lock. Mid-stream ownership
// loss makes the tail go quiet (this node stops receiving commits) rather than error; the gateway
// re-subscribes to the new owner when it detects failover on the commit path. Proactive
// ownership-loss detection here is a later refinement.
//
// (A fromSeq below the log floor — once compaction exists — will return a "too old" signal so the
// caller can fall back to a snapshot. No floor exists yet, so every fromSeq is replayable today.)
func (r *Runtime) Tail(
	ctx context.Context, roomID string, fromSeq uint64, send func(*aetherv1.Event) error,
) error {
	r.mu.Lock()
	err := r.acquire(roomID)
	r.mu.Unlock()
	if err != nil {
		return err
	}

	// Subscribe BEFORE the first read so no commit slips through the replay→live seam. The handler
	// only signals "something changed" into a coalescing one-slot channel; the events are read from
	// the log below.
	wake := make(chan struct{}, 1)
	sub := r.fanout.Subscribe(roomID, func(*aetherv1.Event) {
		select {
		case wake <- struct{}{}:
		default: // a wake is already pending — coalesce
		}
	})
	defer sub.Cancel()

	next := fromSeq + 1 // the next room_seq we owe the caller
	for {
		events, err := r.log.Read(ctx, roomID, next-1) // events with room_seq > next-1
		if err != nil {
			return err
		}
		for _, ev := range events {
			if err := send(ev); err != nil {
				return err
			}
			next = ev.GetRoomSeq() + 1
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			// a commit happened (or a spurious wake) — loop to read everything new from the log
		}
	}
}
