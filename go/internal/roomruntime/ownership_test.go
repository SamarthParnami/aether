package roomruntime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// fakeClock is a settable virtual clock shared by the test's runtimes and coordinator, so
// lease expiry is driven deterministically instead of by wall time.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const testTTL = 10 * time.Second

// twoNodes returns two runtimes (A, B) that contend for the same rooms: they share one durable
// log, one coordinator, and one virtual clock — the setup a real two-node cluster has.
func twoNodes() (a, b *roomruntime.Runtime, clk *fakeClock) {
	log := logstore.NewMemory()
	co := coord.NewMemory()
	clk = &fakeClock{t: time.Unix(1000, 0)}
	mk := func(id string) *roomruntime.Runtime {
		return roomruntime.New(log, fanout.NewMemory(),
			roomruntime.WithNodeID(id),
			roomruntime.WithCoordinator(co),
			roomruntime.WithClock(clk.now),
			roomruntime.WithLeaseTTL(testTTL),
		)
	}
	return mk("A"), mk("B"), clk
}

// While A holds the live lease, B is not the owner: it must refuse both writes and reads
// rather than serve a room it does not own (the soft guard).
func TestSecondNodeRefusesWhileLeaseHeld(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A commit: applied=%v err=%v", applied, err)
	}
	if _, _, err := b.Commit(ctx, "room", "y", 1, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B commit err = %v, want ErrNotOwner", err)
	}
	if _, err := b.Join(ctx, "room"); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B join err = %v, want ErrNotOwner", err)
	}
}

// When the owner dies (stops renewing) and its lease lapses, a survivor takes over: it rebuilds
// the room state from the durable log, continues the sequence, and the dead owner becomes a
// zombie whose writes are refused.
func TestFailoverAfterLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	a, b, clk := twoNodes()

	a.Commit(ctx, "room", "x", 1, kvBody("slide", "3"))
	a.Commit(ctx, "room", "x", 2, kvBody("slide", "7"))

	clk.advance(testTTL + time.Second) // A dies; its lease lapses

	ev, applied, err := b.Commit(ctx, "room", "z", 1, kvBody("slide", "9"))
	if err != nil || !applied {
		t.Fatalf("B takeover commit: applied=%v err=%v", applied, err)
	}
	if ev.GetRoomSeq() != 3 {
		t.Errorf("B commit room_seq = %d, want 3 (continues the log)", ev.GetRoomSeq())
	}

	joined, err := b.Join(ctx, "room")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(joined.GetSnapshot().GetState().GetEntries()["slide"]); got != "9" {
		t.Errorf("B reconstructed state slide=%q, want 9", got)
	}

	// A lost ownership while it was away: its write must be refused, not silently applied.
	if _, _, err := a.Commit(ctx, "room", "x", 3, kvBody("slide", "ZOMBIE")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("zombie A commit err = %v, want ErrNotOwner", err)
	}
}

// A graceful Release hands the room off immediately: the survivor takes over without waiting
// out the lease TTL.
func TestGracefulReleaseHandsOffImmediately(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	a.Commit(ctx, "room", "x", 1, kvBody("k", "v"))
	if _, _, err := b.Commit(ctx, "room", "y", 1, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B should be blocked while A owns; err=%v", err)
	}

	a.Release("room") // planned handoff — no TTL wait

	if _, applied, err := b.Commit(ctx, "room", "y", 1, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("B after A.Release: applied=%v err=%v", applied, err)
	}
}

// A single node re-claims its own room across calls (claim acts as renew) and keeps serving —
// the common no-contention path must not trip the ownership gate.
func TestSingleOwnerRenewsAcrossCommits(t *testing.T) {
	ctx := context.Background()
	a, _, clk := twoNodes()

	for i := uint64(1); i <= 3; i++ {
		clk.advance(testTTL / 2) // within TTL each time: renew, never lapse
		if _, applied, err := a.Commit(ctx, "room", "x", i, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("commit %d: applied=%v err=%v", i, applied, err)
		}
	}
	if joined, err := a.Join(ctx, "room"); err != nil || joined.GetCurrentSeq() != 3 {
		t.Fatalf("join: seq=%d err=%v, want 3", joined.GetCurrentSeq(), err)
	}
}
