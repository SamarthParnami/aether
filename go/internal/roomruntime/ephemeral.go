package roomruntime

import (
	"context"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// ephemeralTailBuffer bounds the per-subscriber backlog TailEphemeral holds between the broadcaster
// and the (possibly slow) stream send. It absorbs a normal burst; on overflow the ephemeral is
// DROPPED rather than queued — the tier is lossy by design, and dropping is what keeps a slow
// subscriber from ever blocking the broadcaster (the fan-out bus invokes handlers synchronously).
const ephemeralTailBuffer = 64

// Broadcast fans an ephemeral message (cursor, presence, "typing…", reaction) to the room's
// current ephemeral subscribers. It is the owner side of the gateway's Broadcast RPC and the
// publish half of the best-effort tier: lossy, unordered, no ack, never persisted.
//
// Ownership is confirmed (claim-on-serve) exactly like a durable write, for one reason: the
// ephemeral bus is the owner's, so a broadcast must publish on the node whose TailEphemeral
// subscribers actually are — otherwise it would fan into a node nobody is listening on. A
// non-owner returns ErrNotOwner so the gateway re-resolves to the real owner.
func (r *Runtime) Broadcast(
	ctx context.Context, roomID, originClientID string, body *aetherv1.EphemeralBody,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	err := r.acquire(roomID)
	r.mu.Unlock()
	if err != nil {
		return err
	}

	r.efanout.Publish(roomID, &aetherv1.Ephemeral{
		RoomId:         roomID,
		OriginClientId: originClientID,
		Body:           body,
	})
	return nil
}

// TailEphemeral streams a room's live ephemerals to send until send errors or ctx is cancelled.
// It is the owner side of the gateway's SubscribeEphemeral RPC.
//
// Unlike the durable Tail there is NO log and NO poll backstop: ephemerals are never persisted, so
// there is nothing to replay — a subscriber receives only what is broadcast after it subscribes,
// and gets no catch-up. Two consequences follow from that:
//   - The fan-out handler must never block the broadcaster (the bus delivers synchronously), so it
//     hands off through a bounded channel and DROPS on overflow rather than applying backpressure.
//   - There is no shared store to self-heal a mid-stream re-home (the durable Tail re-reads the
//     shared log; here there is none). So if the room moves to another node this stream silently
//     goes quiet; recovering it is the gateway's job — re-resolve the owner and re-subscribe — the
//     same re-home recovery G10 handles for the event relay.
//
// Ownership is confirmed once at the start so the subscription lands on the node that actually
// receives this room's broadcasts; a non-owner returns ErrNotOwner for the gateway to re-resolve.
func (r *Runtime) TailEphemeral(
	ctx context.Context, roomID string, send func(*aetherv1.Ephemeral) error,
) error {
	r.mu.Lock()
	err := r.acquire(roomID)
	r.mu.Unlock()
	if err != nil {
		return err
	}

	// The handler runs in the broadcaster's goroutine, so it must not block: hand off to a bounded
	// channel and drop on overflow (lossy by design).
	ch := make(chan *aetherv1.Ephemeral, ephemeralTailBuffer)
	sub := r.efanout.Subscribe(roomID, func(e *aetherv1.Ephemeral) {
		select {
		case ch <- e:
		default: // backlog full — drop, never block the broadcaster
		}
	})
	defer sub.Cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-ch:
			if err := send(e); err != nil {
				return err
			}
		}
	}
}
