package roomruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// shutdown asserts the graceful path actually completed. Shutdown now reports the releases it
// could not land, and a test that discarded that would keep passing while every lease leaked —
// the exact failure Shutdown exists to prevent.
//
// Deliberately context.Background() and NOT t.Context(): t.Context() is cancelled just before
// Cleanup functions run, and this helper is exactly the shape one registers in t.Cleanup. Against a
// ctx-aware coordinator that combination makes every release fail — a silent no-op that still
// passes, since the rooms are dropped locally either way. The production trap is the same shape
// (see Runtime.Shutdown).
func shutdown(t *testing.T, rt *roomruntime.Runtime) {
	t.Helper()
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// The point of Shutdown: a survivor takes over with NO time passing. The clock is never advanced
// in this test, so if the assertion holds it can only be because the lease was handed back —
// not because it lapsed. That is the difference between an instant handover and a TTL-long hole
// in which the room is unroutable.
func TestShutdownHandsRoomsOverWithoutWaitingOutTheTTL(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A must own the room first: applied=%v err=%v", applied, err)
	}
	if _, err := b.Join(ctx, "room"); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B must be locked out while A holds the lease, got %v", err)
	}

	shutdown(t, a)

	if _, err := b.Join(ctx, "room"); err != nil {
		t.Fatalf("B must take over immediately after A shuts down, got %v", err)
	}
}

// Shutdown must release EVERY owned room, not just the most recent one.
func TestShutdownReleasesEveryOwnedRoom(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	rooms := []string{"maths-1", "physics-2", "chem-3"}
	for _, room := range rooms {
		if _, applied, err := a.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("A must own %s: applied=%v err=%v", room, applied, err)
		}
	}

	shutdown(t, a)

	for _, room := range rooms {
		if _, err := b.Join(ctx, room); err != nil {
			t.Fatalf("B must take over %s immediately, got %v", room, err)
		}
	}
}

// The ordering guard. acquire CLAIMS, so a request arriving after Shutdown would re-take a room
// we just handed over — and the survivor, which may already be serving it, would lose it again.
// A shut-down runtime must refuse rather than re-claim.
func TestShutdownRuntimeNeverReclaims(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A must own the room first: applied=%v err=%v", applied, err)
	}
	shutdown(t, a)

	// A late request to the departing node must not resurrect its ownership…
	if _, _, err := a.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("a shut-down node must refuse commits, got %v", err)
	}
	if _, err := a.Join(ctx, "room"); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("a shut-down node must refuse joins, got %v", err)
	}
	// …and the room must still be free for the survivor afterwards.
	if _, err := b.Join(ctx, "room"); err != nil {
		t.Fatalf("B must still be able to take the room, got %v", err)
	}
}

// Shutdown is called from a signal handler; it must tolerate being called twice (SIGTERM then
// SIGINT, or a defer plus an explicit call) without panicking or revoking anything it re-owns.
func TestShutdownIsIdempotent(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A must own the room first: applied=%v err=%v", applied, err)
	}

	shutdown(t, a)
	shutdown(t, a)

	if _, err := b.Join(ctx, "room"); err != nil {
		t.Fatalf("B must own the room after a repeated shutdown, got %v", err)
	}
}

// Safety: a departing node must never revoke a lease that has already moved on. A releases a room
// it no longer owns — coord.Release is guarded by holder identity, so B keeps it.
func TestShutdownDoesNotRevokeALeaseAlreadyTakenOver(t *testing.T) {
	ctx := context.Background()
	a, b, clk := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A must own the room first: applied=%v err=%v", applied, err)
	}

	// A stalls long enough to lose the lease, and B takes over.
	clk.advance(testTTL + time.Second)
	if _, err := b.Join(ctx, "room"); err != nil {
		t.Fatalf("B must take over the expired lease, got %v", err)
	}

	// A now shuts down and tries to release a room it no longer owns. B must be unaffected.
	shutdown(t, a)

	if _, applied, err := b.Commit(ctx, "room", "y", 1, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("B must still own the room after A's late shutdown: applied=%v err=%v", applied, err)
	}
}
