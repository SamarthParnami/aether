// Package roomcore is the pure, I/O-free state machine for a single room.
//
// It folds durable events into RoomState, assigns the authoritative room_seq, and
// dedups replayed commits. It knows nothing about storage, networking, time, or
// concurrency — all of that is wrapped around it by the room-runtime service. Being
// pure makes it deterministic and exhaustively testable (see the property tests and
// golden vectors in this package).
package roomcore

import (
	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Room is the in-memory model the owner holds for one room.
type Room struct {
	state   *aetherv1.RoomState
	nextSeq uint64            // next room_seq to assign (starts at 1)
	dedup   map[string]uint64 // client_id -> highest client_seq processed
}

// New returns an empty room. room_seq numbering starts at 1.
func New() *Room {
	return &Room{
		state:   &aetherv1.RoomState{Entries: map[string][]byte{}},
		nextSeq: 1,
		dedup:   map[string]uint64{},
	}
}

// State returns the current materialized state. Callers must not mutate it.
func (r *Room) State() *aetherv1.RoomState { return r.state }

// Seq returns the highest room_seq assigned so far (0 if none).
func (r *Room) Seq() uint64 { return r.nextSeq - 1 }

// Apply processes a durable commit.
//
// For a new commit it assigns the next room_seq, folds the body into state, records
// the dedup high-water mark, and returns the resulting Event with applied=true. A
// replayed/duplicate commit (client_seq <= the high-water mark for that client) is
// ignored: it returns (nil, false) and leaves state and seq untouched. This is what
// makes durable writes exactly-once across reconnect replays.
func (r *Room) Apply(clientID string, clientSeq uint64, body *aetherv1.EventBody) (*aetherv1.Event, bool) {
	if clientSeq <= r.dedup[clientID] {
		return nil, false // duplicate
	}

	seq := r.nextSeq
	r.nextSeq++
	r.dedup[clientID] = clientSeq
	fold(r.state, body)

	return &aetherv1.Event{
		RoomSeq:         seq,
		OriginClientId:  clientID,
		OriginClientSeq: clientSeq,
		Body:            body,
	}, true
}

// Replay rebuilds state by folding events in order on top of a snapshot. It is pure:
// the snapshot is not mutated. This is the reconstruction path used on re-homing and
// cold start. Snapshot-equivalence — Replay(snapshot@N, tail) == Replay(genesis, all)
// — is verified by the property tests.
func Replay(snapshot *aetherv1.RoomState, events []*aetherv1.Event) *aetherv1.RoomState {
	state := cloneState(snapshot)
	for _, e := range events {
		fold(state, e.GetBody())
	}
	return state
}

// fold applies one event body to state (mutating state). Last-write-wins for the
// generic Phase-1 KeyValueSet event. The value is copied so state never aliases the
// event.
func fold(state *aetherv1.RoomState, body *aetherv1.EventBody) {
	kv := body.GetKvSet()
	if kv == nil {
		return
	}
	v := kv.GetValue()
	cp := make([]byte, len(v))
	copy(cp, v)
	state.Entries[kv.GetKey()] = cp
}

func cloneState(s *aetherv1.RoomState) *aetherv1.RoomState {
	out := &aetherv1.RoomState{Entries: make(map[string][]byte, len(s.GetEntries()))}
	for k, v := range s.GetEntries() {
		cp := make([]byte, len(v))
		copy(cp, v)
		out.Entries[k] = cp
	}
	return out
}
