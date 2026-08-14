package membership_test

import (
	"fmt"
	"testing"

	"github.com/SamarthParnami/aether/go/internal/membership"
)

// NewView promises that on duplicate IDs the FIRST entry wins, and every gateway must resolve that
// the same way — two gateways that keep different Addrs for one node id dial different addresses
// for the same node, which is placement divergence of exactly the kind the maphash ban exists to
// prevent.
//
// The existing duplicate test uses three nodes. Go's sort falls back to insertion sort (which is
// incidentally stable) below a size threshold, so a three-node case cannot observe an unstable
// sort at all. This one is deliberately wide enough to reach pdqsort proper, where an unstable
// sort is free to swap the equal-ID pair and hand back the second entry's address.
func TestNewViewDuplicateWinnerIsStableAtScale(t *testing.T) {
	const n = 64
	nodes := make([]membership.Node, 0, n+1)

	// The duplicate pair goes in first, so "first entry wins" is unambiguous about which Addr the
	// view must keep.
	nodes = append(nodes,
		membership.Node{ID: "dup", Addr: "10.0.0.1:7001"},
		membership.Node{ID: "dup", Addr: "10.0.0.2:7001"},
	)
	// Descending filler IDs so the input is far from sorted and the sort really has to move things.
	for i := n; i > 0; i-- {
		nodes = append(nodes, membership.Node{
			ID:   fmt.Sprintf("node-%03d", i),
			Addr: fmt.Sprintf("10.0.1.%d:7001", i),
		})
	}

	view := membership.NewView(nodes)

	var got string
	for _, node := range view.Nodes() {
		if node.ID == "dup" {
			got = node.Addr
		}
	}
	if got != "10.0.0.1:7001" {
		t.Fatalf("duplicate id kept Addr %q, want 10.0.0.1:7001 (the first entry) — an unstable "+
			"sort makes which duplicate survives unspecified, so two gateways given the same "+
			"nodes in different order can dial different addresses for one node", got)
	}
}

// The view a gateway computes must depend only on the SET of nodes, never on the order the
// registry happened to return them in. A DynamoDB scan gives no order guarantee across pages, so
// two gateways reading the same table can legitimately see different orderings.
func TestNewViewIsIndependentOfInputOrder(t *testing.T) {
	const n = 64
	forward := make([]membership.Node, 0, n+1)
	for i := range n {
		forward = append(forward, membership.Node{
			ID:   fmt.Sprintf("node-%03d", i),
			Addr: fmt.Sprintf("10.0.1.%d:7001", i),
		})
	}
	forward = append(forward, membership.Node{ID: "node-000", Addr: "10.9.9.9:7001"}) // a late duplicate

	reversed := make([]membership.Node, len(forward))
	for i, node := range forward {
		reversed[len(forward)-1-i] = node
	}

	a, b := membership.NewView(forward), membership.NewView(forward)
	if fmt.Sprint(a.Nodes()) != fmt.Sprint(b.Nodes()) {
		t.Fatal("NewView is not deterministic for identical input")
	}

	// Reversing moves the duplicate ahead of its twin, so the two views legitimately differ in
	// which Addr wins — what must NOT differ is the node SET.
	ids := func(v membership.View) []string {
		out := make([]string, 0, v.Len())
		for _, node := range v.Nodes() {
			out = append(out, node.ID)
		}
		return out
	}
	if fmt.Sprint(ids(a)) != fmt.Sprint(ids(membership.NewView(reversed))) {
		t.Fatalf("node set depends on input order: %v vs %v", ids(a), ids(membership.NewView(reversed)))
	}
}
