package logstore_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomcore"
)

func kvEvent(seq uint64, key, val string) *aetherv1.Event {
	return &aetherv1.Event{
		RoomSeq: seq,
		Body: &aetherv1.EventBody{
			Kind: &aetherv1.EventBody_KvSet{
				KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)},
			},
		},
	}
}

func TestAppendSequential(t *testing.T) {
	ctx := context.Background()
	m := logstore.NewMemory()
	for i := uint64(1); i <= 3; i++ {
		if err := m.Append(ctx, "r", i, kvEvent(i, "k", "v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if head, _ := m.Head(ctx, "r"); head != 3 {
		t.Fatalf("head = %d, want 3", head)
	}
}

// The conditional-append guard: a wrong expectedSeq (gap or duplicate) is rejected.
func TestAppendConditionalRejectsGapAndDuplicate(t *testing.T) {
	ctx := context.Background()
	m := logstore.NewMemory()
	if err := m.Append(ctx, "r", 1, kvEvent(1, "k", "v")); err != nil {
		t.Fatal(err)
	}

	// gap: expected 2, try 3
	if err := m.Append(ctx, "r", 3, kvEvent(3, "k", "v")); !errors.Is(err, logstore.ErrConflict) {
		t.Fatalf("gap append err = %v, want ErrConflict", err)
	}
	// duplicate / stale writer: expected 2, try 1 again (split-brain race)
	if err := m.Append(ctx, "r", 1, kvEvent(1, "k", "v2")); !errors.Is(err, logstore.ErrConflict) {
		t.Fatalf("duplicate append err = %v, want ErrConflict", err)
	}
}

func TestReadReturnsTailInOrder(t *testing.T) {
	ctx := context.Background()
	m := logstore.NewMemory()
	for i := uint64(1); i <= 5; i++ {
		_ = m.Append(ctx, "r", i, kvEvent(i, "k", "v"))
	}
	got, _ := m.Read(ctx, "r", 2) // expect room_seq 3,4,5
	if len(got) != 3 || got[0].GetRoomSeq() != 3 || got[2].GetRoomSeq() != 5 {
		t.Fatalf("Read(2) = %d events starting at %d", len(got), got[0].GetRoomSeq())
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := logstore.NewMemory()
	if _, _, ok, _ := m.ReadSnapshot(ctx, "r"); ok {
		t.Fatal("expected no snapshot initially")
	}
	_ = m.WriteSnapshot(ctx, "r", 7, []byte("blob"))
	data, seq, ok, _ := m.ReadSnapshot(ctx, "r")
	if !ok || seq != 7 || string(data) != "blob" {
		t.Fatalf("snapshot = %q/%d/%v", data, seq, ok)
	}
}

// Stored events are isolated from caller mutation (defensive copy on Append/Read).
func TestAppendClonesEvent(t *testing.T) {
	ctx := context.Background()
	m := logstore.NewMemory()
	ev := kvEvent(1, "k", "original")
	_ = m.Append(ctx, "r", 1, ev)
	ev.GetBody().GetKvSet().Value = []byte("mutated") // mutate after append

	got, _ := m.Read(ctx, "r", 0)
	if v := string(got[0].GetBody().GetKvSet().GetValue()); v != "original" {
		t.Fatalf("stored event aliased caller mutation: got %q", v)
	}
}

// Property: appending a stream then folding it via roomcore.Replay reproduces the same
// state as folding the events directly — the log faithfully preserves order.
func TestProp_LogPreservesOrderForReplay(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		m := logstore.NewMemory()
		keys := []string{"a", "b", "c"}

		n := rapid.IntRange(0, 30).Draw(t, "n")
		direct := make([]*aetherv1.Event, 0, n)
		for i := 1; i <= n; i++ {
			ev := kvEvent(uint64(i),
				rapid.SampledFrom(keys).Draw(t, "key"),
				rapid.StringN(0, 8, -1).Draw(t, "val"),
			)
			if err := m.Append(ctx, "r", uint64(i), ev); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
			direct = append(direct, ev)
		}

		read, _ := m.Read(ctx, "r", 0)
		empty := &aetherv1.RoomState{Entries: map[string][]byte{}}
		fromLog := roomcore.Replay(empty, read)
		fromDirect := roomcore.Replay(empty, direct)

		if !proto.Equal(fromLog, fromDirect) {
			t.Fatalf("replay from log != replay direct:\n log=%v\n direct=%v", fromLog, fromDirect)
		}
	})
}
