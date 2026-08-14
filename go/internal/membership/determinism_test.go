package membership_test

import (
	"fmt"
	"testing"

	"github.com/SamarthParnami/aether/go/internal/membership"
)

// dupHi and dupLo are one duplicate id carrying two addresses. Every assertion below runs with the
// pair in BOTH input orders, which is what makes these tests independent of fleet size.
//
// Asserting one order only is how the ORIGINAL defect slipped past the test written to catch it: a
// stable sort keeps the first input row, so ordering the pair higher-Addr-first makes a stable sort
// pick wrong — but an UNSTABLE sort's permutation is arbitrary, and at many sizes (n=64 among them)
// it happens to leave the lower address first, which is exactly what the assertion wanted. That is
// the same "passes only by accident of size" this PR diagnosed one round earlier, reproduced in its
// own replacement at a different constant.
//
// Running both orders removes size from the answer: a total order over (ID, Addr) yields the lowest
// Addr either way, while any ID-only sort — stable or not — must disagree with itself on at least
// one of the two. It also guards the likeliest future regression, which is not someone
// reintroducing a stable sort but someone simplifying the comparator back to a one-line
// strings.Compare(a.ID, b.ID).
var (
	dupHi = membership.Node{ID: "dup", Addr: "10.0.0.9:7001"}
	dupLo = membership.Node{ID: "dup", Addr: "10.0.0.1:7001"}
)

// dupOrders is the duplicate pair both ways round.
func dupOrders() [][]membership.Node {
	return [][]membership.Node{{dupHi, dupLo}, {dupLo, dupHi}}
}

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

// On duplicate IDs the LOWEST Addr wins, whatever order the rows arrived in. The fleet is wide
// enough to clear the sort's insertion-sort cutoff, below which equal elements never move and none
// of this is observable at all.
func TestNewViewDuplicateWinnerIsTheLowestAddr(t *testing.T) {
	for _, dup := range dupOrders() {
		nodes := buildFleet(64, dup...)

		var got string
		for _, node := range membership.NewView(nodes).Nodes() {
			if node.ID == "dup" {
				got = node.Addr
			}
		}
		if got != dupLo.Addr {
			t.Errorf("input order [%s %s]: duplicate kept Addr %q, want %q (the lowest) — the "+
				"survivor must be a function of the node SET, not of which row arrived first",
				dup[0].Addr, dup[1].Addr, got, dupLo.Addr)
		}
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
	for _, dup := range dupOrders() {
		forward := buildFleet(64, dup...)
		reversed := make([]membership.Node, len(forward))
		for i, node := range forward {
			reversed[len(forward)-1-i] = node
		}

		a := membership.NewView(forward).Nodes()
		b := membership.NewView(reversed).Nodes()
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Errorf("input order [%s %s]: view depends on input order:\n forward  = %v\n reversed = %v",
				dup[0].Addr, dup[1].Addr, a, b)
		}
	}
}

// Rank must agree across gateways down to the address, not just the identity. This is the
// end-to-end version of the two tests above: the ranked list is what placement actually walks.
func TestRankAgreesOnAddrAcrossInputOrders(t *testing.T) {
	for _, dup := range dupOrders() {
		forward := buildFleet(64, dup...)
		reversed := make([]membership.Node, len(forward))
		for i, node := range forward {
			reversed[len(forward)-1-i] = node
		}

		for _, room := range []string{"", "room", "class-721363", "\x00"} {
			a := membership.NewView(forward).Rank(room)
			b := membership.NewView(reversed).Rank(room)
			if fmt.Sprint(a) != fmt.Sprint(b) {
				t.Errorf("input order [%s %s]: Rank(%q) differs:\n forward  = %v\n reversed = %v",
					dup[0].Addr, dup[1].Addr, room, a, b)
			}
		}
	}
}
