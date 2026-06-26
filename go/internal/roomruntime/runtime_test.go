package roomruntime_test

import (
	"context"
	"testing"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

func kvBody(key, val string) *aetherv1.EventBody {
	return &aetherv1.EventBody{
		Kind: &aetherv1.EventBody_KvSet{
			KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)},
		},
	}
}

// A commit travels the whole journey: assigned a room_seq, persisted to the log, and fanned
// out — and the fanned event carries the originator's client_seq (fan-out is the ack).
func TestCommitPersistsAndFansOut(t *testing.T) {
	ctx := context.Background()
	log := logstore.NewMemory()
	fo := fanout.NewMemory()
	rt := roomruntime.New(log, fo)

	var received []*aetherv1.Event
	fo.Subscribe("room", func(e *aetherv1.Event) { received = append(received, e) })

	ev, applied, err := rt.Commit(ctx, "room", "client-A", 1, kvBody("slide", "7"))
	if err != nil || !applied {
		t.Fatalf("commit: applied=%v err=%v", applied, err)
	}
	if ev.GetRoomSeq() != 1 || ev.GetOriginClientSeq() != 1 {
		t.Errorf("event room_seq/origin = %d/%d, want 1/1", ev.GetRoomSeq(), ev.GetOriginClientSeq())
	}

	// persisted
	if head, _ := log.Head(ctx, "room"); head != 1 {
		t.Errorf("log head = %d, want 1", head)
	}
	// fanned out, with the originator's dedup key
	if len(received) != 1 || received[0].GetOriginClientSeq() != 1 {
		t.Fatalf("fan-out = %d events; want 1 carrying origin_client_seq=1", len(received))
	}
}

// A replayed commit is exactly-once: not re-applied, not re-persisted, not re-fanned.
func TestDuplicateCommitIsIgnored(t *testing.T) {
	ctx := context.Background()
	log := logstore.NewMemory()
	fo := fanout.NewMemory()
	rt := roomruntime.New(log, fo)

	fanned := 0
	fo.Subscribe("room", func(*aetherv1.Event) { fanned++ })

	rt.Commit(ctx, "room", "A", 1, kvBody("k", "v"))
	ev, applied, err := rt.Commit(ctx, "room", "A", 1, kvBody("k", "MUTATED")) // replay
	if err != nil {
		t.Fatal(err)
	}
	if applied || ev != nil {
		t.Fatal("replayed commit should not be applied")
	}
	if head, _ := log.Head(ctx, "room"); head != 1 {
		t.Errorf("log head = %d after replay, want 1", head)
	}
	if fanned != 1 {
		t.Errorf("fan-out count = %d after replay, want 1", fanned)
	}
}

// The reconstruction property that failover rests on: a fresh Runtime on the same log
// rebuilds the exact room state (and dedups already-applied commits) — no shared memory.
func TestReconstructsFromLogOnAnotherRuntime(t *testing.T) {
	ctx := context.Background()
	log := logstore.NewMemory()

	rt1 := roomruntime.New(log, fanout.NewMemory())
	rt1.Commit(ctx, "room", "A", 1, kvBody("slide", "3"))
	rt1.Commit(ctx, "room", "A", 2, kvBody("slide", "9"))
	rt1.Commit(ctx, "room", "B", 1, kvBody("presenter", "alice"))

	// A different Runtime, sharing only the durable log, must see the same state.
	rt2 := roomruntime.New(log, fanout.NewMemory())
	joined, err := rt2.Join(ctx, "room")
	if err != nil {
		t.Fatal(err)
	}
	if joined.GetCurrentSeq() != 3 {
		t.Errorf("current_seq = %d, want 3", joined.GetCurrentSeq())
	}
	entries := joined.GetSnapshot().GetState().GetEntries()
	if string(entries["slide"]) != "9" || string(entries["presenter"]) != "alice" {
		t.Errorf("reconstructed state = %v, want slide=9 presenter=alice", entries)
	}

	// dedup must survive reconstruction: replaying A/1 on the rebuilt runtime is a no-op.
	if _, applied, _ := rt2.Commit(ctx, "room", "A", 1, kvBody("slide", "HACK")); applied {
		t.Fatal("rebuilt runtime re-applied an already-committed commit")
	}
}
