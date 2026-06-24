package roomcore

import (
	"testing"

	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// --- helpers shared across the test files in this package ---

func kvBody(key, val string) *aetherv1.EventBody {
	return &aetherv1.EventBody{
		Kind: &aetherv1.EventBody_KvSet{
			KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)},
		},
	}
}

func kvEvent(seq uint64, key, val string) *aetherv1.Event {
	return &aetherv1.Event{RoomSeq: seq, Body: kvBody(key, val)}
}

func emptyState() *aetherv1.RoomState {
	return &aetherv1.RoomState{Entries: map[string][]byte{}}
}

// --- unit tests ---

func TestApplyAssignsSequentialSeq(t *testing.T) {
	r := New()
	e1, ok1 := r.Apply("c1", 1, kvBody("a", "1"))
	e2, ok2 := r.Apply("c1", 2, kvBody("b", "2"))
	if !ok1 || !ok2 {
		t.Fatal("expected both commits applied")
	}
	if e1.GetRoomSeq() != 1 || e2.GetRoomSeq() != 2 {
		t.Errorf("room_seq = %d,%d; want 1,2", e1.GetRoomSeq(), e2.GetRoomSeq())
	}
	if r.Seq() != 2 {
		t.Errorf("Seq() = %d; want 2", r.Seq())
	}
}

func TestApplyDedupsReplay(t *testing.T) {
	r := New()
	r.Apply("c1", 1, kvBody("slide", "7"))
	before := proto.Clone(r.State()).(*aetherv1.RoomState)

	ev, ok := r.Apply("c1", 1, kvBody("slide", "9")) // replay of client_seq 1
	if ok || ev != nil {
		t.Fatal("replay should not be applied")
	}
	if !proto.Equal(before, r.State()) {
		t.Error("state changed on replayed commit")
	}
	if r.Seq() != 1 {
		t.Errorf("Seq() advanced to %d on replay; want 1", r.Seq())
	}
}

func TestLastWriteWins(t *testing.T) {
	r := New()
	r.Apply("c1", 1, kvBody("slide", "7"))
	r.Apply("c1", 2, kvBody("slide", "9"))
	if got := string(r.State().GetEntries()["slide"]); got != "9" {
		t.Errorf("slide = %q; want 9", got)
	}
}

func TestReplayMatchesApply(t *testing.T) {
	r := New()
	events := []*aetherv1.Event{}
	body := []*aetherv1.EventBody{kvBody("a", "1"), kvBody("a", "2"), kvBody("b", "x")}
	for i, b := range body {
		ev, _ := r.Apply("c1", uint64(i+1), b)
		events = append(events, ev)
	}
	replayed := Replay(emptyState(), events)
	if !proto.Equal(r.State(), replayed) {
		t.Errorf("Replay state %v != Apply state %v", replayed, r.State())
	}
}
