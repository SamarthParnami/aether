package membership_test

import (
	"fmt"
	"testing"

	"github.com/SamarthParnami/aether/go/internal/membership"
)

// buildFleet returns n filler nodes in DESCENDING id order (so the input is far from sorted and
// the sort really has to move things), with the given duplicate pair placed first.
func buildFleet(n int, dup ...membership.Node) []membership.Node {
	nodes := make([]membership.Node, 0, n+len(dup))
	nodes = append(nodes, dup...)
	for i := n; i > 0; i-- {
		nodes = append(nodes, membership.Node{
			ID:   fmt.Sprintf("node-%03d", i),
			Addr: fmt.Sprintf("10.0.1.%d:7001", i),
		})
	}
	return nodes
}

// On duplicate IDs the LOWEST Addr wins, whatever order the rows arrived in.
//
// The duplicate pair is deliberately ordered with the lower address SECOND, which separates the
// three possible behaviours: an unstable sort picks unpredictably, a stable sort picks the first
// INPUT entry (10.0.0.9), and only a total order over (ID, Addr) picks the lowest address. The
// fleet is wide enough to clear the sort's insertion-sort cutoff, below which equal elements never
// move and none of this is observable.
func TestNewViewDuplicateWinnerIsTheLowestAddr(t *testing.T) {
	nodes := buildFleet(64,
		membership.Node{ID: "dup", Addr: "10.0.0.9:7001"},
		membership.Node{ID: "dup", Addr: "10.0.0.1:7001"},
	)

	var got string
	for _, node := range membership.NewView(nodes).Nodes() {
		if node.ID == "dup" {
			got = node.Addr
		}
	}
	if got != "10.0.0.1:7001" {
		t.Fatalf("duplicate id kept Addr %q, want 10.0.0.1:7001 (the lowest) — the survivor must "+
			"be a function of the node SET, not of which row happened to arrive first", got)
	}
}

// The view a gateway computes must depend only on the SET of nodes, never on the order the registry
// returned them in — a DynamoDB Scan guarantees no ordering across pages, so two gateways reading
// one table legitimately receive the same rows in different orders.
//
// This compares Nodes() in full, INCLUDING Addr, and compares forward against reversed. Both matter:
// comparing only ids is blind to precisely the divergence that hurts (placement dials Node.Addr, so
// two gateways can agree on the set and still route to different processes), and comparing an input
// against itself is a tautology that any deterministic sort satisfies, stable or not.
func TestNewViewIsIndependentOfInputOrder(t *testing.T) {
	forward := buildFleet(64,
		membership.Node{ID: "dup", Addr: "10.0.0.9:7001"},
		membership.Node{ID: "dup", Addr: "10.0.0.1:7001"},
	)
	reversed := make([]membership.Node, len(forward))
	for i, node := range forward {
		reversed[len(forward)-1-i] = node
	}

	a := membership.NewView(forward).Nodes()
	b := membership.NewView(reversed).Nodes()
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("view depends on input order:\n forward  = %v\n reversed = %v", a, b)
	}
}

// Rank must agree across gateways down to the address, not just the identity. This is the
// end-to-end version of the two tests above: the ranked list is what placement actually walks.
func TestRankAgreesOnAddrAcrossInputOrders(t *testing.T) {
	forward := buildFleet(64,
		membership.Node{ID: "dup", Addr: "10.0.0.9:7001"},
		membership.Node{ID: "dup", Addr: "10.0.0.1:7001"},
	)
	reversed := make([]membership.Node, len(forward))
	for i, node := range forward {
		reversed[len(forward)-1-i] = node
	}

	for _, room := range []string{"", "room", "class-721363", "\x00"} {
		a := membership.NewView(forward).Rank(room)
		b := membership.NewView(reversed).Rank(room)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Errorf("Rank(%q) differs by input order:\n forward  = %v\n reversed = %v", room, a, b)
		}
	}
}
