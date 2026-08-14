package roomruntime_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/coord/coordtest"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// node returns a single runtime over its own coordinator, plus the coordinator so a test can check
// the directory directly — "did the node actually take the lease" is not answerable from the
// runtime alone.
func node(t *testing.T, opts ...roomruntime.Option) (*roomruntime.Runtime, *coord.Memory) {
	t.Helper()
	co := coord.NewMemory()
	opts = append([]roomruntime.Option{
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
	}, opts...)
	return roomruntime.New(logstore.NewMemory(), fanout.NewMemory(), opts...), co
}

// leased reports whether the directory currently names an owner for the room.
func leased(t *testing.T, co *coord.Memory, roomID string) bool {
	t.Helper()
	_, ok, err := co.Current(t.Context(), roomID, time.Now())
	if err != nil {
		t.Fatalf("coord.Current(%q): %v", roomID, err)
	}
	return ok
}

// The exit criterion for draining: a node that has released a room must not take it back. Without
// this, preStop is a loop that fights itself — Release hands the room over, and the next in-flight
// request (or the relay's own Tail re-acquiring) claims it straight back, so the pod exits still
// holding a live lease that names an address about to go dark for a full TTL.
func TestDrainingNodeNeverRetakesAReleasedRoom(t *testing.T) {
	ctx := context.Background()
	rt, co := node(t)

	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("warm-up commit: applied=%v err=%v", applied, err)
	}

	rt.SetDraining(true)
	if err := rt.Release(ctx, "room"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Every path that acquires must refuse, not just the write path — the relay's Tail is the one
	// that actually caused this race in practice.
	if _, _, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrDraining) {
		t.Errorf("Commit after drain+release = %v, want ErrDraining", err)
	}
	if _, err := rt.Join(ctx, "room"); !errors.Is(err, roomruntime.ErrDraining) {
		t.Errorf("Join after drain+release = %v, want ErrDraining", err)
	}
	tailErr := rt.Tail(ctx, "room", 0, func(*aetherv1.Event) error { return nil })
	if !errors.Is(tailErr, roomruntime.ErrDraining) {
		t.Errorf("Tail after drain+release = %v, want ErrDraining", tailErr)
	}

	if leased(t, co, "room") {
		t.Fatal("the released room is leased again — a draining node re-took it")
	}
}

// Draining gates ADMISSION only. A node mid-drain still owns rooms it hasn't handed over yet, and
// cutting them off at the moment preStop fires would drop every live session instead of migrating
// them one at a time.
func TestDrainingKeepsServingRoomsItStillOwns(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t)

	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("warm-up commit: applied=%v err=%v", applied, err)
	}

	rt.SetDraining(true)

	if _, applied, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("commit to an owned room while draining: applied=%v err=%v", applied, err)
	}
	// A room it does NOT own is refused at the same moment.
	if _, _, err := rt.Commit(ctx, "other", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrDraining) {
		t.Fatalf("commit to a new room while draining = %v, want ErrDraining", err)
	}
}

// Draining is reversible, unlike Shutdown. An aborted rolling deploy or a health check that
// recovers must be able to put the node back in service without restarting the process.
func TestDrainingIsReversible(t *testing.T) {
	ctx := context.Background()
	rt, co := node(t)

	rt.SetDraining(true)
	if _, _, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrDraining) {
		t.Fatalf("commit while draining = %v, want ErrDraining", err)
	}

	rt.SetDraining(false)
	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("commit after drain cleared: applied=%v err=%v", applied, err)
	}
	if !leased(t, co, "room") {
		t.Fatal("node served the room without taking its lease")
	}
}

// A draining refusal must stay distinguishable from an ownership verdict. Collapsing them would
// send the gateway back to the directory, which would name this same node again — a refusal loop
// rather than a handover.
func TestDrainingIsNotErrNotOwner(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t)
	rt.SetDraining(true)

	_, _, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v"))
	if errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("draining reported as ErrNotOwner (%v) — the gateway would re-resolve straight "+
			"back to this node instead of trying another", err)
	}
	if !errors.Is(err, roomruntime.ErrDraining) {
		t.Fatalf("commit while draining = %v, want ErrDraining", err)
	}
}

// The capacity cap bounds how much placement can pile onto one node. The hash distributes by id and
// knows nothing about load, so without a cap a node has no way to decline.
func TestMaxRoomsRefusesBeyondTheCap(t *testing.T) {
	ctx := context.Background()
	rt, co := node(t, roomruntime.WithMaxRooms(2))

	for _, room := range []string{"room-1", "room-2"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
	}

	if _, _, err := rt.Commit(ctx, "room-3", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrAtCapacity) {
		t.Fatalf("commit beyond the cap = %v, want ErrAtCapacity", err)
	}
	if leased(t, co, "room-3") {
		t.Fatal("a refused room was leased anyway — the gate ran after the claim")
	}
}

// At capacity, the rooms already owned keep working. A cap that shed existing rooms would make
// every node's load oscillate instead of settle.
func TestMaxRoomsDoesNotEvictOwnedRooms(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t, roomruntime.WithMaxRooms(1))

	if _, applied, err := rt.Commit(ctx, "room-1", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("room-1 commit: applied=%v err=%v", applied, err)
	}
	if _, _, err := rt.Commit(ctx, "room-2", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrAtCapacity) {
		t.Fatalf("room-2 commit = %v, want ErrAtCapacity", err)
	}
	// room-1 is unaffected by the node being full.
	if _, applied, err := rt.Commit(ctx, "room-1", "x", 2, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("room-1 commit while at capacity: applied=%v err=%v", applied, err)
	}
}

// Releasing a room frees a capacity slot — the cap tracks what is owned NOW, not a high-water mark.
func TestMaxRoomsFreesASlotOnRelease(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t, roomruntime.WithMaxRooms(1))

	if _, applied, err := rt.Commit(ctx, "room-1", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("room-1 commit: applied=%v err=%v", applied, err)
	}
	if _, _, err := rt.Commit(ctx, "room-2", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrAtCapacity) {
		t.Fatalf("room-2 commit = %v, want ErrAtCapacity", err)
	}

	if err := rt.Release(ctx, "room-1"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, applied, err := rt.Commit(ctx, "room-2", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("room-2 commit after a slot freed: applied=%v err=%v", applied, err)
	}
}

// Zero (the default) means unlimited — the Phase-1 behaviour must be unchanged for anyone who does
// not opt in.
func TestMaxRoomsZeroIsUnlimited(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t)

	for _, room := range []string{"r1", "r2", "r3", "r4", "r5"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
	}
}

// The capacity cap must count rooms this node CURRENTLY owns, not rooms it has ever touched.
//
// owned is only pruned by an explicit Release or a lost Claim, so a lease that lapses from
// inactivity leaves its entry behind. As a lifetime counter the knob does not bound concurrent load
// at all — it bounds how long a pod may stay up. For this product that is the normal case rather
// than a corner: rooms are classes, classes end, and nothing releases them when they do. A node
// configured for 500 rooms would serve 500 classes one after another, never more than one at a
// time, then refuse everything permanently while holding zero live leases — a silent failure that
// arrives a week late.
func TestMaxRoomsCountsLiveLeasesNotLifetime(t *testing.T) {
	ctx := context.Background()
	co := coord.NewMemory()
	clk := &fakeClock{t: time.Unix(1000, 0)}
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
		roomruntime.WithClock(clk.now),
		roomruntime.WithLeaseTTL(testTTL),
		roomruntime.WithMaxRooms(2),
	)

	// Serve rooms strictly one after another, letting each lease lapse before the next. Concurrency
	// never exceeds one, so a cap of 2 must never be reached.
	for i := range 8 {
		room := fmt.Sprintf("class-%d", i)
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s (room %d served sequentially, cap=2): applied=%v err=%v — the cap is "+
				"counting rooms ever owned rather than rooms owned now", room, i+1, applied, err)
		}
		clk.advance(testTTL + time.Second) // the lease lapses; the room is over
	}
}

// The cap still binds on CONCURRENT rooms, so the fix above cannot be satisfied by never refusing.
func TestMaxRoomsStillBindsWhileLeasesAreLive(t *testing.T) {
	ctx := context.Background()
	co := coord.NewMemory()
	clk := &fakeClock{t: time.Unix(1000, 0)}
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
		roomruntime.WithClock(clk.now),
		roomruntime.WithLeaseTTL(testTTL),
		roomruntime.WithMaxRooms(2),
	)

	for _, room := range []string{"room-1", "room-2"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
		clk.advance(time.Second) // well inside the TTL: both leases stay live
	}

	if _, _, err := rt.Commit(ctx, "room-3", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrAtCapacity) {
		t.Fatalf("third concurrent room = %v, want ErrAtCapacity", err)
	}
}

// A lapsed lease is not ownership, so a draining node must not treat it as a room it already holds
// and quietly re-take it.
func TestDrainingRefusesARoomWhoseLeaseLapsed(t *testing.T) {
	ctx := context.Background()
	co := coord.NewMemory()
	clk := &fakeClock{t: time.Unix(1000, 0)}
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
		roomruntime.WithClock(clk.now),
		roomruntime.WithLeaseTTL(testTTL),
	)

	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("warm-up commit: applied=%v err=%v", applied, err)
	}
	clk.advance(testTTL + time.Second) // the lease lapses on its own
	rt.SetDraining(true)

	if _, _, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrDraining) {
		t.Fatalf("commit to a lapsed room while draining = %v, want ErrDraining — a lapsed lease "+
			"is not ownership, so the admission gate must apply", err)
	}
}

// The cap must see rooms that are being READ, not only rooms being written.
//
// Ownership was claim-on-serve for writes alone, so a room with readers and no writers — a class
// where everyone is subscribed and nobody is committing — lost its lease after one TTL while the
// node kept streaming it. Counting live leases then UNDER-counts, which is quieter and worse than
// the lifetime over-count it replaced: instead of a node that refuses everything (visible, and
// traceable in a day), a node that accepts everything and dies of memory pressure with no
// connection to the knob meant to prevent it.
func TestMaxRoomsCountsRoomsHeldByReadersAlone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	co := coord.NewMemory()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
		roomruntime.WithLeaseTTL(200*time.Millisecond),
		roomruntime.WithTailPollInterval(20*time.Millisecond), // renewal interval, well under the TTL
		roomruntime.WithMaxRooms(2),
	)

	// Two rooms held by readers only — no commits after the seed, so nothing but Tail renews them.
	for _, room := range []string{"room-1", "room-2"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s seed commit: applied=%v err=%v", room, applied, err)
		}
		go func() { _ = rt.Tail(ctx, room, 0, func(*aetherv1.Event) error { return nil }) }()
	}

	// Outlive the lease several times over. Only the readers keep these rooms alive.
	time.Sleep(600 * time.Millisecond)

	if _, _, err := rt.Commit(ctx, "room-3", "x", 1, kvBody("k", "v")); !errors.Is(err, roomruntime.ErrAtCapacity) {
		t.Fatalf("third room while two are held by readers = %v, want ErrAtCapacity — the cap "+
			"cannot see rooms this node is streaming, so it does not bound what the node holds", err)
	}
}

// startReader begins a Tail and returns only once the stream is ESTABLISHED — i.e. after its first
// delivered event, which means the initial acquire has already succeeded. Without that barrier the
// goroutine can race the test's own state changes and take the admission gate on its FIRST acquire,
// which proves nothing about established streams.
func startReader(ctx context.Context, t *testing.T, rt *roomruntime.Runtime, room string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	first := make(chan struct{})
	var once sync.Once
	go func() {
		done <- rt.Tail(ctx, room, 0, func(*aetherv1.Event) error {
			once.Do(func() { close(first) })
			return nil
		})
	}()
	select {
	case <-first:
	case err := <-done:
		t.Fatalf("reader ended before it was established: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("reader never established")
	}
	return done
}

// A transient coord brownout must not disconnect readers that are already streaming.
//
// Renewing on Tail's tick put every established stream on the node behind the same store within the
// same interval, so a blip shorter than one poll interval could drop all of them at once. That
// inverts the thesis this layer is built on: ambiguity freezes, it does not disconnect. Not knowing
// whether we still hold the lease is not evidence that we do not — the same reason acquire refuses
// to render an unanswered claim as ErrNotOwner — and reads come from the shared log, so they stay
// correct while ownership is unknown.
func TestCoordBrownoutDoesNotDisconnectEstablishedReaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	co := coordtest.New(coord.NewMemory())
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithCoordinator(co),
		roomruntime.WithLeaseTTL(200*time.Millisecond),
		roomruntime.WithTailPollInterval(20*time.Millisecond),
	)
	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("seed commit: applied=%v err=%v", applied, err)
	}

	readers := make([]<-chan error, 8)
	for i := range readers {
		readers[i] = startReader(ctx, t, rt, "room")
	}

	co.FailClaim(errors.New("dynamodb: throttled")) // a blip spanning several poll ticks
	time.Sleep(120 * time.Millisecond)
	co.FailClaim(nil)
	time.Sleep(60 * time.Millisecond)

	for i, done := range readers {
		select {
		case err := <-done:
			t.Fatalf("reader %d was disconnected by a transient coord brownout: %v", i, err)
		default: // still streaming, which is the point
		}
	}
}

// preStop must not cut readers that are mid-stream. This is the promise gating admission-only was
// made to keep — a drain migrates sessions, it does not drop them — and routing Tail's renewal
// through those same gates would break it one poll interval after SetDraining.
func TestDrainDoesNotCutEstablishedReaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithLeaseTTL(200*time.Millisecond),
		roomruntime.WithTailPollInterval(20*time.Millisecond),
	)
	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("seed commit: applied=%v err=%v", applied, err)
	}
	done := startReader(ctx, t, rt, "room")

	// The documented preStop sequence, which drops the room from owned.
	rt.SetDraining(true)
	if err := rt.Release(ctx, "room"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // several poll ticks

	select {
	case err := <-done:
		t.Fatalf("preStop cut a live reader session: %v", err)
	default:
	}
}

// A negative cap is unlimited, not "refuse everything" — the gate is n > 0. Pinned because a
// computed cap (capacity minus reserved, say) can come out negative, and the other reading would
// take the node out of service silently.
func TestMaxRoomsNegativeIsUnlimited(t *testing.T) {
	ctx := context.Background()
	rt, _ := node(t, roomruntime.WithMaxRooms(-1))

	for _, room := range []string{"r1", "r2", "r3"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit with a negative cap: applied=%v err=%v", room, applied, err)
		}
	}
}
