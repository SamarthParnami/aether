// Package roomruntime is the room owner: it wires the pure roomcore reducer to the durable
// log (logstore) and the delivery bus (fanout) to serve the write journey.
//
// The journey for a durable commit is: dedup → assign room_seq → APPEND to the log and only
// then ("ack-after-persist") → fan out the committed event. The fan-out IS the ack: the
// event carries the originator's client_seq, so the sender recognizes its own commit coming
// back. A room's state is reconstructed by replaying its log, so any node can rebuild it —
// the basis of failover (added with coordination in a later PR).
package roomruntime

import (
	"context"
	"sync"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomcore"
)

// Runtime owns a set of rooms on this node. (Coordination/ownership is layered on later;
// for now a Runtime is assumed to be the sole owner of any room it serves.)
type Runtime struct {
	log    logstore.LogStore
	fanout fanout.Fanout

	mu    sync.Mutex
	rooms map[string]*roomcore.Room
}

// New returns a Runtime backed by the given log and fan-out bus.
func New(log logstore.LogStore, fo fanout.Fanout) *Runtime {
	return &Runtime{log: log, fanout: fo, rooms: map[string]*roomcore.Room{}}
}

// Commit processes a durable commit and returns the committed Event.
//
//   - applied:  a new commit — assigned room_seq, persisted, and fanned out (event != nil).
//   - !applied: a duplicate (replayed) commit — ignored, exactly-once (event == nil, err == nil).
//   - err:      persistence failed; on a sequence conflict the in-memory room is dropped so
//     it rebuilds from the log (a conflict means this node is not really the owner).
func (r *Runtime) Commit(
	ctx context.Context, roomID, clientID string, clientSeq uint64, body *aetherv1.EventBody,
) (event *aetherv1.Event, applied bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	room, err := r.ensureRoom(ctx, roomID)
	if err != nil {
		return nil, false, err
	}

	ev, ok := room.Apply(clientID, clientSeq, body)
	if !ok {
		return nil, false, nil // duplicate — already applied
	}

	// Ack-after-persist: the event is real only once the durable, conditional append wins.
	if err := r.log.Append(ctx, roomID, ev.GetRoomSeq(), ev); err != nil {
		delete(r.rooms, roomID) // stale/lost ownership — discard so the next access rebuilds
		return nil, false, err
	}

	r.fanout.Publish(roomID, ev) // fan-out is the ack
	return ev, true, nil
}

// Join returns the room's current materialized state for catch-up: the latest room_seq and a
// snapshot a joining client adopts before applying any newer events. (Client-id assignment
// and the gap replay belong to the gateway/SDK, layered on later.)
func (r *Runtime) Join(ctx context.Context, roomID string) (*aetherv1.Joined, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	room, err := r.ensureRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return &aetherv1.Joined{
		RoomId:     roomID,
		CurrentSeq: room.Seq(),
		Snapshot:   &aetherv1.Snapshot{RoomSeq: room.Seq(), State: room.State()},
	}, nil
}

// ensureRoom returns the in-memory room, rebuilding it from the durable log on first access.
// Caller must hold r.mu.
func (r *Runtime) ensureRoom(ctx context.Context, roomID string) (*roomcore.Room, error) {
	if room := r.rooms[roomID]; room != nil {
		return room, nil
	}
	events, err := r.log.Read(ctx, roomID, 0) // full log; snapshots are an optimization added later
	if err != nil {
		return nil, err
	}
	room := roomcore.RestoreAndReplay(roomcore.Snapshot{}, events)
	r.rooms[roomID] = room
	return room, nil
}
