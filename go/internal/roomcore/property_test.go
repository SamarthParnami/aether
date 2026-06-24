package roomcore

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// A small key space so generated event streams overwrite the same keys often,
// genuinely exercising last-write-wins ordering.
var keySpace = []string{"a", "b", "c"}

// genEvents produces a random ordered event stream of KeyValueSet bodies.
func genEvents(t *rapid.T) []*aetherv1.Event {
	n := rapid.IntRange(0, 30).Draw(t, "n")
	events := make([]*aetherv1.Event, n)
	for i := range events {
		key := rapid.SampledFrom(keySpace).Draw(t, "key")
		val := rapid.StringN(0, 8, -1).Draw(t, "val")
		events[i] = kvEvent(uint64(i+1), key, val)
	}
	return events
}

// Snapshot-equivalence: folding the whole log from genesis equals folding the tail
// on top of a snapshot taken at any split point. This is the property that makes
// re-homing / cold-start reconstruction provably correct.
func TestProp_SnapshotEquivalence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		events := genEvents(t)
		n := rapid.IntRange(0, len(events)).Draw(t, "split")

		full := Replay(emptyState(), events)
		snap := Replay(emptyState(), events[:n])
		split := Replay(snap, events[n:])

		if !proto.Equal(full, split) {
			t.Fatalf("snapshot-equivalence violated at split=%d:\n full=%v\n split=%v", n, full, split)
		}
	})
}

// Replay-determinism: folding the same events in the same order always yields the
// same state. (If this ever failed, clients could diverge.)
func TestProp_ReplayDeterminism(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		events := genEvents(t)
		a := Replay(emptyState(), events)
		b := Replay(emptyState(), events)
		if !proto.Equal(a, b) {
			t.Fatalf("replay not deterministic:\n a=%v\n b=%v", a, b)
		}
	})
}

// Dedup idempotency: replaying every commit a second time applies none of them and
// leaves state and seq unchanged — durable writes are exactly-once.
func TestProp_DedupIdempotent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		clients := []string{"x", "y"}
		seqs := map[string]uint64{}

		type commit struct {
			clientID  string
			clientSeq uint64
			key, val  string
		}
		n := rapid.IntRange(0, 30).Draw(t, "n")
		commits := make([]commit, n)
		for i := range commits {
			c := rapid.SampledFrom(clients).Draw(t, "client")
			seqs[c]++ // monotonic per client, as the SDK assigns
			commits[i] = commit{
				clientID:  c,
				clientSeq: seqs[c],
				key:       rapid.SampledFrom(keySpace).Draw(t, "key"),
				val:       rapid.StringN(0, 8, -1).Draw(t, "val"),
			}
		}

		r := New()
		for _, c := range commits {
			r.Apply(c.clientID, c.clientSeq, kvBody(c.key, c.val))
		}
		stateBefore := proto.Clone(r.State()).(*aetherv1.RoomState)
		seqBefore := r.Seq()

		for _, c := range commits { // replay them all
			if ev, ok := r.Apply(c.clientID, c.clientSeq, kvBody(c.key, "MUTATED")); ok || ev != nil {
				t.Fatalf("replay of (%s,%d) was applied", c.clientID, c.clientSeq)
			}
		}
		if !proto.Equal(stateBefore, r.State()) {
			t.Fatal("state changed on full replay")
		}
		if r.Seq() != seqBefore {
			t.Fatalf("seq advanced from %d to %d on replay", seqBefore, r.Seq())
		}
	})
}
