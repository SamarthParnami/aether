package roomruntime_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/coord/coordtest"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// errStore stands in for a coord adapter that cannot answer — a DynamoDB throttle or timeout.
var errStore = errors.New("coord store unreachable")

// oneNode returns a single-node runtime over a faultable coordinator and a Read-counting log.
func oneNode(t *testing.T) (*roomruntime.Runtime, *coordtest.Faulty, *countingLog) {
	t.Helper()
	log := &countingLog{LogStore: logstore.NewMemory()}
	co := coordtest.New(coord.NewMemory())
	rt := roomruntime.New(log, fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
	)
	return rt, co, log
}

// The distinction this whole change exists to make, on the owner side: a coord that cannot answer
// is NOT "some other node owns this room".
//
// ErrNotOwner is an instruction — it tells the gateway to go find the real owner and retry there.
// Returning it when the store merely timed out would send every request for every room hunting for
// a new owner, i.e. a re-home storm aimed at a coord that is already failing. The error must
// surface as itself.
func TestAcquireCoordErrorIsNotErrNotOwner(t *testing.T) {
	ctx := context.Background()
	rt, co, _ := oneNode(t)

	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("warm-up commit: applied=%v err=%v", applied, err)
	}

	co.FailClaim(errStore)
	_, _, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2"))
	if err == nil {
		t.Fatal("commit succeeded while coord was unreachable")
	}
	if errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("coord failure reported as ErrNotOwner (%v) — the gateway would go re-home a room "+
			"that never changed owner", err)
	}
	if !errors.Is(err, errStore) {
		t.Fatalf("commit err = %v, want it to wrap %v", err, errStore)
	}
}

// A coord brownout must not cost the room its materialized state. We do not know we lost the room,
// so dropping it would turn every failed lookup into a full log replay on recovery — the expensive
// half of a re-home, paid for nothing.
func TestAcquireCoordErrorKeepsTheRoomMaterialized(t *testing.T) {
	ctx := context.Background()
	rt, co, log := oneNode(t)

	if _, applied, err := rt.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("warm-up commit: applied=%v err=%v", applied, err)
	}
	afterWarmUp := log.reads.Load()

	co.FailClaim(errStore)
	if _, _, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); err == nil {
		t.Fatal("commit succeeded while coord was unreachable")
	}
	co.FailClaim(nil) // the store recovers

	if _, applied, err := rt.Commit(ctx, "room", "x", 2, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("commit after coord recovered: applied=%v err=%v", applied, err)
	}
	if got := log.reads.Load(); got != afterWarmUp {
		t.Fatalf("full-log reads = %d, want %d — the room was dropped and rebuilt on a coord "+
			"error, which is the behaviour of a lost lease, not an unanswered one", got, afterWarmUp)
	}
}

// The other side of the boundary: a lease genuinely lost still yields the definite ErrNotOwner, so
// the test above cannot be satisfied by never reporting ErrNotOwner at all.
func TestAcquireLostLeaseIsErrNotOwner(t *testing.T) {
	ctx := context.Background()
	log := &countingLog{LogStore: logstore.NewMemory()}
	co := coord.NewMemory()
	shared := func(nodeID, addr string) *roomruntime.Runtime {
		return roomruntime.New(log, fanout.NewMemory(),
			roomruntime.WithNodeID(nodeID),
			roomruntime.WithAddr(addr),
			roomruntime.WithCoordinator(co),
		)
	}
	a, b := shared("A", "a.example:7001"), shared("B", "b.example:7002")

	if _, applied, err := a.Commit(ctx, "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A warm-up commit: applied=%v err=%v", applied, err)
	}
	if err := a.Release(ctx, "room"); err != nil {
		t.Fatalf("A Release: %v", err)
	}
	if _, applied, err := b.Commit(ctx, "room", "y", 1, kvBody("k", "v2")); err != nil || !applied {
		t.Fatalf("B takeover commit: applied=%v err=%v", applied, err)
	}

	// A is now a stale owner. Its next attempt must be refused as ErrNotOwner — the definite answer.
	if _, _, err := a.Commit(ctx, "room", "x", 2, kvBody("k", "v3")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("stale owner commit = %v, want ErrNotOwner", err)
	}
}

// Shutdown's whole purpose is that a survivor need not wait out the TTL. When a release does not
// land that guarantee is silently gone, so the failure has to reach the caller — this is what
// makes the drain sequencing in cmd/aether-node able to tell a clean exit from a lossy one.
func TestShutdownReportsFailedReleases(t *testing.T) {
	ctx := context.Background()
	rt, co, _ := oneNode(t)

	for _, room := range []string{"room-1", "room-2"} {
		if _, applied, err := rt.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
	}

	co.FailRelease(errStore)
	err := rt.Shutdown(ctx)
	if err == nil {
		t.Fatal("Shutdown reported success while every release failed")
	}
	if !errors.Is(err, errStore) {
		t.Fatalf("Shutdown err = %v, want it to wrap %v", err, errStore)
	}

	// Failing to release must not leave the node willing to serve: closed is set regardless, or a
	// request arriving mid-drain would re-claim a room the node is walking away from.
	if _, _, err := rt.Commit(ctx, "room-1", "x", 2, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("commit after failed Shutdown = %v, want ErrNotOwner", err)
	}
}

// Pins the trap in Shutdown's doc, because no amount of documentation stops the canonical wiring:
//
//	ctx, _ := signal.NotifyContext(ctx, syscall.SIGTERM); <-ctx.Done(); rt.Shutdown(ctx)
//
// Against coord.Memory this looks perfect — it ignores ctx entirely, so every release "succeeds"
// and the handover appears instant. Against anything that honours ctx (every durable store) each
// release fails, the rooms are dropped locally anyway, and every lease strands for a full TTL.
// That is #49's whole guarantee gone, silently, on every rolling deploy.
//
// The test exists because the failure is invisible to the rest of the suite: it takes a coordinator
// that reads ctx, which is exactly what coordtest.Faulty now provides.
func TestShutdownWithACancelledContextStrandsEveryLease(t *testing.T) {
	rt, _, _ := oneNode(t)
	rooms := []string{"room-1", "room-2"}
	for _, room := range rooms {
		if _, applied, err := rt.Commit(context.Background(), room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	err := rt.Shutdown(cancelled)
	if err == nil {
		t.Fatal("Shutdown reported success with a cancelled context — the caller has no way to " +
			"learn that every lease was stranded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown err = %v, want it to wrap context.Canceled", err)
	}
}

// The same trap on the DEFAULT coordinator — no coordtest.Faulty, no options, exactly what
// roomruntime.New() gives you and what twoNodes() uses.
//
// This is the test that makes the guard structural rather than opt-in. The version above only
// fires for someone who already reached for Faulty, and knowing to reach for it requires already
// suspecting the bug — which is the "remembering" the guard was supposed to remove. Whoever writes
// P9's preStop test gets the default coordinator, so the default coordinator is where this has to
// bite.
func TestShutdownWithACancelledContextFailsOnTheDefaultCoordinator(t *testing.T) {
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	if _, applied, err := rt.Commit(context.Background(), "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("commit: applied=%v err=%v", applied, err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if err := rt.Shutdown(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown on the default coordinator = %v, want it to wrap context.Canceled — "+
			"the default path is still blind to a dead context, so the trap only shows up for "+
			"someone who already suspected it", err)
	}
}

// perRoomFaultyRelease fails Release for chosen rooms and records every room it was asked to
// release — enough to tell "never attempted" apart from "attempted and failed", which is the whole
// question when one room's failure could abort the loop.
type perRoomFaultyRelease struct {
	coord.Coordinator
	fail map[string]error

	mu       sync.Mutex
	attempts []string
}

func (p *perRoomFaultyRelease) Release(ctx context.Context, roomID, owner string) error {
	p.mu.Lock()
	p.attempts = append(p.attempts, roomID)
	p.mu.Unlock()
	if err := p.fail[roomID]; err != nil {
		return err
	}
	return p.Coordinator.Release(ctx, roomID, owner)
}

func (p *perRoomFaultyRelease) attempted() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	for _, r := range p.attempts {
		seen[r] = true
	}
	return seen
}

// One room whose release fails must not strand the node's OTHER leases. Iterating rooms and
// returning on the first error would leave a rolling deploy holding leases it meant to hand back,
// and the rooms behind them unroutable for a full TTL — the exact hole Shutdown exists to close.
func TestShutdownReleasesEveryRoomDespiteOneFailure(t *testing.T) {
	ctx := context.Background()
	rooms := []string{"room-1", "room-2", "room-3"}
	co := &perRoomFaultyRelease{
		Coordinator: coord.NewMemory(),
		fail:        map[string]error{"room-2": errStore},
	}
	a := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("A"),
		roomruntime.WithAddr("a.example:7001"),
		roomruntime.WithCoordinator(co),
	)

	for _, room := range rooms {
		if _, applied, err := a.Commit(ctx, room, "x", 1, kvBody("k", "v")); err != nil || !applied {
			t.Fatalf("%s commit: applied=%v err=%v", room, applied, err)
		}
	}

	if err := a.Shutdown(ctx); !errors.Is(err, errStore) {
		t.Fatalf("Shutdown err = %v, want it to wrap %v", err, errStore)
	}

	attempted := co.attempted()
	for _, room := range rooms {
		if !attempted[room] {
			t.Errorf("%s was never released — one room's failure aborted the drain", room)
		}
	}

	// The rooms that DID release are free at once, with no clock advanced: a survivor sharing the
	// coordinator takes them immediately. room-2 is the one that legitimately waits out its TTL.
	b := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID("B"),
		roomruntime.WithAddr("b.example:7002"),
		roomruntime.WithCoordinator(co),
	)
	for _, room := range []string{"room-1", "room-3"} {
		if _, applied, err := b.Commit(ctx, room, "y", 1, kvBody("k", "v2")); err != nil || !applied {
			t.Fatalf("B claim of %s after A's partial drain: applied=%v err=%v", room, applied, err)
		}
	}
	if _, _, err := b.Commit(ctx, "room-2", "y", 1, kvBody("k", "v2")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B claim of room-2 = %v, want ErrNotOwner — its release failed, so A still holds "+
			"the lease until the TTL lapses", err)
	}
}
