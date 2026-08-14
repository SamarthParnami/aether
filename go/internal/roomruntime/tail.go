package roomruntime

import (
	"context"
	"time"

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
// prefers the real owner, which has the live fan-out for low-latency delivery. The stream then runs
// without holding the room lock, and tolerates a mid-stream re-home: because the log is shared, a
// poll tick (tailPoll) re-reads it every interval even when no fan-out wake arrives — so if the room
// moves to another node (whose commits fire ITS fan-out, not ours), this reader still picks up the
// new owner's writes within one interval instead of freezing. That matters for the watcher-heavy
// shape (one presenter, many readers): watchers never commit, so there is no commit-path failover to
// rely on, and the old owner can keep serving a quiet tail it's no longer woken for. The shared Redis
// fan-out will later deliver cross-node wakes directly, but the poll stays the floor beneath it —
// re-reading the bus-independent log is what keeps reads correct even if Redis itself is down.
//
// (A fromSeq below the log floor — once compaction exists — will return a "too old" signal so the
// caller can fall back to a snapshot. No floor exists yet, so every fromSeq is replayable today.)
func (r *Runtime) Tail(
	ctx context.Context, roomID string, fromSeq uint64, send func(*aetherv1.Event) error,
) error {
	r.mu.Lock()
	err := r.acquire(ctx, roomID)
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

	// A poll tick re-reads the log even with no wake, bounding read staleness if this node isn't
	// the one being woken (a re-home moved commits to another node's fan-out). It also RE-ACQUIRES
	// (see the tick branch below).
	ticker := time.NewTicker(r.tailPoll)
	defer ticker.Stop()

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
			// A commit on this node fired its fan-out — read everything new, and push the poll out:
			// the backstop should only fire after tailPoll of actual silence, so a healthy
			// (wake-fed) stream does ~zero empty log reads. Only this goroutine touches the ticker.
			ticker.Reset(r.tailPoll)
		case <-ticker.C:
			// Poll fallback — pick up another node's writes (after a re-home) or any missed wake —
			// and RENEW, because reading is serving too.
			//
			// Ownership is claim-on-serve and only the write paths were serving, so a room with
			// readers and no writers — a class where everyone is subscribed and nobody is
			// committing — lost its lease after one TTL while this node went on streaming it.
			// Lease liveness and "is this node serving the room" had drifted apart, which is what
			// made WithMaxRooms under-count: rooms held and streamed, but invisible to a gate that
			// counts live leases.
			//
			// This is why tailPoll must stay comfortably under the lease TTL — the renewal interval
			// IS the poll interval, so a poll longer than the TTL lets an actively-read room lapse
			// between ticks and be re-admitted through the capacity gate.
			//
			// The renewal is BEST-EFFORT and never fatal to the stream. Every failure mode is a
			// reason to keep serving, not to stop:
			//
			//   - ErrNotOwner: a placement loser's watchers keep reading CORRECTLY, because Tail
			//     reads events from the shared log and never from fan-out, so the loser converges
			//     on the winner's commits within one poll tick. TestTailPollSurvivesReHome pins
			//     exactly that. (There is an argument for the opposite — a per-tick ownership check
			//     that stops a loser serving a lagging read path — but it would reverse a tested
			//     guarantee, so it wants deciding on its own, not as a side effect of a capacity
			//     fix. See Runtime.Shutdown, where the same tension is recorded.)
			//   - ErrCoordUnavailable: not knowing whether we still hold the lease is not evidence
			//     that we do not, exactly as acquire itself refuses to render an unanswered claim as
			//     ErrNotOwner. Returning here would let a store blip shorter than one poll interval
			//     disconnect every reader on the node at once — ambiguity must freeze, not drop.
			//   - ErrDraining / ErrAtCapacity: these gate ADMISSION. Letting them reach an
			//     established stream is precisely the "cut every live session the instant preStop
			//     fired" that gating admission-only exists to prevent.
			//
			// So the tick renews when it can and is silent when it cannot. Reads stay correct
			// regardless, because they come from the shared log rather than from ownership.
			//
			// TODO(observability): a renewal that never succeeds — coord down, or the room
			// permanently refused — silently returns this node to under-counting its own capacity,
			// which is the defect this renewal was added to fix, with no signal. The package has no
			// logger or metrics yet, so there is nowhere to put it; when observability lands this
			// wants a counter. Same shape as membership's note that a durable View should
			// distinguish "no rows" from "rows, all expired": both are conditions whose only
			// symptom is a graph that stopped moving.
			r.mu.Lock()
			_ = r.acquire(ctx, roomID)
			r.mu.Unlock()
		}
	}
}
