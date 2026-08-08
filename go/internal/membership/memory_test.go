package membership_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamarthParnami/aether/go/internal/membership"
)

// t0 is an arbitrary, fixed base instant for the unit tests.
var t0 = time.Unix(1000, 0)

func mustView(t *testing.T, r *membership.Memory, now time.Time) membership.View {
	t.Helper()
	v, err := r.View(context.Background(), now)
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	return v
}

func TestHeartbeatIsVisibleUntilItExpires(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()
	if err := r.Heartbeat(ctx, membership.Node{ID: "a", Addr: "10.0.0.1:7001"}, t0, 10*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if got := mustView(t, r, t0.Add(9*time.Second)).Len(); got != 1 {
		t.Errorf("view within ttl holds %d nodes, want 1", got)
	}
	// Expiry is evaluated caller-side against `now`, exactly as coord.Current does.
	if got := mustView(t, r, t0.Add(10*time.Second)).Len(); got != 0 {
		t.Errorf("view at expiry holds %d nodes, want 0", got)
	}
}

// TestHeartbeatRejectsUnroutableNode — the enforcement point for the hazard NewView also
// defends against. A node with no address must never enter the fleet view.
func TestHeartbeatRejectsUnroutableNode(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()

	for _, n := range []membership.Node{
		{ID: "a", Addr: ""},
		{ID: "", Addr: "10.0.0.1:7001"},
	} {
		err := r.Heartbeat(ctx, n, t0, 10*time.Second)
		if !errors.Is(err, membership.ErrInvalidNode) {
			t.Errorf("Heartbeat(%+v) error = %v, want ErrInvalidNode", n, err)
		}
	}

	if got := mustView(t, r, t0).Len(); got != 0 {
		t.Errorf("rejected nodes still landed in the view (%d nodes)", got)
	}
}

// TestDeregisterTakesEffectImmediately is the property the whole drain ordering rests on: a
// departing node must leave the view without waiting out its heartbeat ttl, or gateways keep
// placing rooms onto a pod that is already exiting.
func TestDeregisterTakesEffectImmediately(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()
	_ = r.Heartbeat(ctx, membership.Node{ID: "a", Addr: "10.0.0.1:7001"}, t0, 10*time.Second)
	_ = r.Heartbeat(ctx, membership.Node{ID: "b", Addr: "10.0.0.2:7001"}, t0, 10*time.Second)

	if err := r.Deregister(ctx, "a", t0.Add(time.Second)); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// Well inside a's ttl, which is the point.
	v := mustView(t, r, t0.Add(2*time.Second))
	if v.Len() != 1 {
		t.Fatalf("view holds %d nodes, want 1: %+v", v.Len(), v.Nodes())
	}
	if got := v.Nodes()[0].ID; got != "b" {
		t.Errorf("surviving node = %q, want \"b\"", got)
	}
}

func TestDeregisterUnknownNodeIsANoOp(t *testing.T) {
	r := membership.NewMemory()
	if err := r.Deregister(context.Background(), "never-registered", t0); err != nil {
		t.Errorf("Deregister of an unknown node = %v, want nil", err)
	}
}

// TestHeartbeatAfterDeregisterRejoins — an aborted drain must not need operator intervention to
// clear.
func TestHeartbeatAfterDeregisterRejoins(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()
	n := membership.Node{ID: "a", Addr: "10.0.0.1:7001"}

	_ = r.Heartbeat(ctx, n, t0, 10*time.Second)
	_ = r.Deregister(ctx, "a", t0)
	if got := mustView(t, r, t0).Len(); got != 0 {
		t.Fatalf("view after deregister holds %d nodes, want 0", got)
	}

	_ = r.Heartbeat(ctx, n, t0.Add(time.Second), 10*time.Second)
	if got := mustView(t, r, t0.Add(2*time.Second)).Len(); got != 1 {
		t.Errorf("view after re-heartbeat holds %d nodes, want 1 (draining mark not cleared)", got)
	}
}

// TestViewIsSortedAndReflectsTheLatestAddress — a node that restarts on a new address must be
// dialable at the new one, and the view must not leak map iteration order.
func TestViewIsSortedAndReflectsTheLatestAddress(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()
	for _, id := range []string{"c", "a", "b"} {
		_ = r.Heartbeat(ctx, membership.Node{ID: id, Addr: "10.0.0.9:7001"}, t0, 10*time.Second)
	}
	_ = r.Heartbeat(ctx, membership.Node{ID: "b", Addr: "10.0.0.2:7002"}, t0, 10*time.Second)

	nodes := mustView(t, r, t0).Nodes()
	if len(nodes) != 3 {
		t.Fatalf("view holds %d nodes, want 3", len(nodes))
	}
	for i, want := range []string{"a", "b", "c"} {
		if nodes[i].ID != want {
			t.Fatalf("view not sorted by ID: %+v", nodes)
		}
	}
	if got := nodes[1].Addr; got != "10.0.0.2:7002" {
		t.Errorf("node b addr = %q, want the re-heartbeated address", got)
	}
}

// TestRegistryFeedsPlacement is the small end-to-end this package owes: register a fleet, take a
// view, and get a stable placement target — with a drained node never selected.
func TestRegistryFeedsPlacement(t *testing.T) {
	r := membership.NewMemory()
	ctx := context.Background()
	for _, id := range []string{"owner-0", "owner-1", "owner-2"} {
		_ = r.Heartbeat(ctx, membership.Node{ID: id, Addr: id + ":7001"}, t0, 10*time.Second)
	}

	const room = "class-721363"
	primary, ok := mustView(t, r, t0).Primary(room)
	if !ok {
		t.Fatal("no primary for a three-node fleet")
	}
	if primary.Addr == "" {
		t.Fatal("placement target has no dialable address")
	}

	// Drain the chosen node: placement must move, and must not pick it again.
	if err := r.Deregister(ctx, primary.ID, t0); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	next, ok := mustView(t, r, t0).Primary(room)
	if !ok {
		t.Fatal("no primary after draining one of three nodes")
	}
	if next.ID == primary.ID {
		t.Errorf("placement still selects the drained node %q", primary.ID)
	}
}
