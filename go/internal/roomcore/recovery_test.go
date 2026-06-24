package roomcore

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

func equalDedup(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// The dedup high-water marks survive snapshot+restore: a restored owner still rejects
// a commit that was already applied before the snapshot. This is what stops re-homing
// from double-applying a durable write.
func TestSnapshotRestorePreservesDedup(t *testing.T) {
	r := New()
	r.Apply("c1", 1, kvBody("a", "1"))
	r.Apply("c1", 2, kvBody("a", "2"))

	restored := Restore(r.Snapshot())

	if ev, ok := restored.Apply("c1", 2, kvBody("a", "MUTATED")); ok || ev != nil {
		t.Fatal("restored room should dedup an already-applied commit")
	}
	if got := string(restored.State().GetEntries()["a"]); got != "2" {
		t.Errorf("state changed after deduped replay on restored room: a=%q", got)
	}
}

// Re-homing reconstruction is exact: rebuilding from a snapshot taken at any split
// point plus the event tail reproduces the original room's state, seq, AND dedup.
func TestProp_RestoreAndReplayReconstructsRoom(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		clients := []string{"x", "y"}
		seqs := map[string]uint64{}

		orig := New()
		n := rapid.IntRange(0, 30).Draw(t, "n")
		events := make([]*aetherv1.Event, 0, n)
		for range make([]struct{}, n) {
			c := rapid.SampledFrom(clients).Draw(t, "client")
			seqs[c]++
			ev, ok := orig.Apply(
				c, seqs[c],
				kvBody(rapid.SampledFrom(keySpace).Draw(t, "key"), rapid.StringN(0, 8, -1).Draw(t, "val")),
			)
			if ok {
				events = append(events, ev)
			}
		}

		k := rapid.IntRange(0, len(events)).Draw(t, "split")

		// Build the room up to the split, snapshot it, then restore + replay the tail.
		partial := New()
		for _, e := range events[:k] {
			partial.applyEvent(e)
		}
		rebuilt := RestoreAndReplay(partial.Snapshot(), events[k:])

		if !proto.Equal(orig.State(), rebuilt.State()) {
			t.Fatalf("state mismatch at split=%d", k)
		}
		if orig.Seq() != rebuilt.Seq() {
			t.Fatalf("seq mismatch: orig=%d rebuilt=%d", orig.Seq(), rebuilt.Seq())
		}
		if !equalDedup(orig.dedup, rebuilt.dedup) {
			t.Fatalf("dedup mismatch: orig=%v rebuilt=%v", orig.dedup, rebuilt.dedup)
		}
	})
}
