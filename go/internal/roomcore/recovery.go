package roomcore

import (
	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Snapshot captures a room's full state for persistence and re-homing.
//
// Unlike the wire Snapshot sent to clients (state + room_seq only), this also carries
// the dedup high-water marks. That matters for correctness: when a survivor rebuilds a
// room from a snapshot, it must still reject a replayed commit that was already applied
// before the snapshot — otherwise re-homing could double-apply a durable write.
type Snapshot struct {
	RoomSeq uint64
	State   *aetherv1.RoomState
	Dedup   map[string]uint64
}

// Snapshot captures the room's current state for persistence.
func (r *Room) Snapshot() Snapshot {
	return Snapshot{
		RoomSeq: r.Seq(),
		State:   cloneState(r.state),
		Dedup:   cloneDedup(r.dedup),
	}
}

// Restore rebuilds a room from a snapshot.
func Restore(s Snapshot) *Room {
	state := s.State
	if state == nil {
		state = &aetherv1.RoomState{Entries: map[string][]byte{}}
	}
	return &Room{
		state:   cloneState(state),
		nextSeq: s.RoomSeq + 1,
		dedup:   cloneDedup(s.Dedup),
	}
}

// RestoreAndReplay rebuilds a room from a snapshot and folds the persisted event tail
// (events with room_seq > snapshot.RoomSeq) on top — the re-homing / cold-start path.
// It reconstructs the materialized state, the seq counter, and the dedup marks from
// the events.
func RestoreAndReplay(s Snapshot, tail []*aetherv1.Event) *Room {
	r := Restore(s)
	for _, e := range tail {
		r.applyEvent(e)
	}
	return r
}

// applyEvent re-applies an already-committed event during reconstruction: fold it,
// advance the seq counter, and restore the originating client's dedup mark. (Apply is
// for fresh commits; this is for replaying the durable log.)
func (r *Room) applyEvent(e *aetherv1.Event) {
	fold(r.state, e.GetBody())
	if next := e.GetRoomSeq() + 1; next > r.nextSeq {
		r.nextSeq = next
	}
	if id := e.GetOriginClientId(); id != "" && e.GetOriginClientSeq() > r.dedup[id] {
		r.dedup[id] = e.GetOriginClientSeq()
	}
}

func cloneDedup(m map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
