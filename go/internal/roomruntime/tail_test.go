package roomruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// recvSeq receives one tailed room_seq, failing the test if none arrives promptly.
func recvSeq(t *testing.T, ch <-chan uint64) uint64 {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a tailed event")
		return 0
	}
}

// Tail first replays the log (catch-up), then delivers live commits — all in room_seq order.
func TestTailCatchUpThenLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())

	rt.Commit(ctx, "room", "A", 1, kvBody("k", "1"))
	rt.Commit(ctx, "room", "A", 2, kvBody("k", "2"))

	got := make(chan uint64, 16)
	go func() {
		_ = rt.Tail(ctx, "room", 0, func(ev *aetherv1.Event) error {
			got <- ev.GetRoomSeq()
			return nil
		})
	}()

	if s := recvSeq(t, got); s != 1 {
		t.Fatalf("catch-up[0] = %d, want 1", s)
	}
	if s := recvSeq(t, got); s != 2 {
		t.Fatalf("catch-up[1] = %d, want 2", s)
	}

	rt.Commit(ctx, "room", "A", 3, kvBody("k", "3"))
	rt.Commit(ctx, "room", "A", 4, kvBody("k", "4"))
	if s := recvSeq(t, got); s != 3 {
		t.Fatalf("live[0] = %d, want 3", s)
	}
	if s := recvSeq(t, got); s != 4 {
		t.Fatalf("live[1] = %d, want 4", s)
	}
}

// Tail(fromSeq) delivers only events after the cursor — the resume path.
func TestTailResumesFromCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	for i := uint64(1); i <= 3; i++ {
		rt.Commit(ctx, "room", "A", i, kvBody("k", "v"))
	}

	got := make(chan uint64, 16)
	go func() {
		_ = rt.Tail(ctx, "room", 2, func(ev *aetherv1.Event) error {
			got <- ev.GetRoomSeq()
			return nil
		})
	}()

	if s := recvSeq(t, got); s != 3 { // only room_seq 3 is > fromSeq 2
		t.Fatalf("resume delivered %d, want 3", s)
	}
	select {
	case s := <-got:
		t.Fatalf("delivered %d at/before the resume cursor", s)
	case <-time.After(100 * time.Millisecond):
	}
}

// A non-owner refuses to tail (ErrNotOwner) so the gateway re-resolves the real owner.
func TestTailRefusedByNonOwner(t *testing.T) {
	ctx := context.Background()
	a, b, _ := twoNodes()

	if _, applied, err := a.Commit(ctx, "room", "A", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A commit: applied=%v err=%v", applied, err)
	}
	err := b.Tail(ctx, "room", 0, func(*aetherv1.Event) error { return nil })
	if !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("non-owner Tail err = %v, want ErrNotOwner", err)
	}
}

// Cancelling the context stops the tail with context.Canceled.
func TestTailStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	rt.Commit(ctx, "room", "A", 1, kvBody("k", "v"))

	got := make(chan uint64, 4)
	done := make(chan error, 1)
	go func() {
		done <- rt.Tail(ctx, "room", 0, func(ev *aetherv1.Event) error {
			got <- ev.GetRoomSeq()
			return nil
		})
	}()

	recvSeq(t, got) // wait until the catch-up is delivered and Tail is parked on its wakeup
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Tail returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Tail did not return after ctx cancel")
	}
}

// A reader doesn't freeze when the room re-homes: even though the new owner's commits fire ITS
// fan-out (never this node's), the poll re-reads the shared log and delivers them within an
// interval — the correctness floor beneath the wake buses. (Short poll interval keeps the test fast.)
func TestTailPollSurvivesReHome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := logstore.NewMemory()
	co := coord.NewMemory()
	clk := &fakeClock{t: time.Unix(1000, 0)}
	mk := func(id string) *roomruntime.Runtime {
		return roomruntime.New(log, fanout.NewMemory(),
			roomruntime.WithNodeID(id),
			roomruntime.WithCoordinator(co),
			roomruntime.WithClock(clk.now),
			roomruntime.WithLeaseTTL(testTTL),
			roomruntime.WithTailPollInterval(20*time.Millisecond),
		)
	}
	a, b := mk("A"), mk("B")

	if _, applied, err := a.Commit(ctx, "room", "A", 1, kvBody("k", "1")); err != nil || !applied {
		t.Fatalf("A commit: applied=%v err=%v", applied, err)
	}

	got := make(chan uint64, 16)
	go func() {
		_ = a.Tail(ctx, "room", 0, func(ev *aetherv1.Event) error {
			got <- ev.GetRoomSeq()
			return nil
		})
	}()
	if s := recvSeq(t, got); s != 1 {
		t.Fatalf("catch-up = %d, want 1", s)
	}

	// A's lease lapses; B takes over and commits — firing B's fan-out, which A's Tail never sees.
	clk.advance(testTTL + time.Second)
	if _, applied, err := b.Commit(ctx, "room", "B", 1, kvBody("k", "2")); err != nil || !applied {
		t.Fatalf("B takeover commit: applied=%v err=%v", applied, err)
	}

	// The poll re-reads the shared log and delivers B's write — the reader does not freeze.
	if s := recvSeq(t, got); s != 2 {
		t.Fatalf("after re-home the poll delivered %d, want 2 (the new owner's write)", s)
	}
}

// A send error ends the tail and is returned to the caller (the RPC handler ends the stream).
func TestTailStopsOnSendError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	rt.Commit(ctx, "room", "A", 1, kvBody("k", "v"))

	boom := errors.New("send failed")
	err := rt.Tail(ctx, "room", 0, func(*aetherv1.Event) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Tail returned %v, want the send error", err)
	}
}
