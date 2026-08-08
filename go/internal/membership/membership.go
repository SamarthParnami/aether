// Package membership is the live-node registry and the placement function computed over it.
//
// It answers one question — "which nodes are serving right now, and which of them should be
// asked to own room R?" — and it answers it as a pure function of an immutable snapshot, so
// every gateway holding the same snapshot picks the same node with no coordination between
// them.
//
// This package decides who to ASK, never who OWNS. Ownership is settled by coord's lease (the
// soft guard) and logstore's conditional Append (the hard one); nothing here writes to either,
// and there is no path from a View to a second successful claim. A stale or divergent view
// therefore costs at most one wasted RPC — the loser gets ErrNotOwner and re-resolves onto the
// winner's lease — which is what keeps membership off the load-bearing path entirely: an empty
// or stale view degrades routing, never correctness.
//
// Time (`now`) is passed in explicitly and never read from a hidden clock — the same
// convention coord states, and what lets the single-threaded sim drive this synchronously.
package membership

import (
	"context"
	"slices"
	"strings"
	"time"
)

// Node is one serving process: a stable identity plus the dialable RPC address it publishes.
// Addr is the same value coord.Lease.Addr carries, because placement dials it directly — so a
// node that cannot be dialed must never appear in a View (see NewView).
type Node struct {
	ID   string
	Addr string
}

// View is an immutable snapshot of the live fleet, sorted by ID. Construct it with NewView.
//
// The zero View is valid and empty: Rank returns nothing, Primary reports false. Callers must
// read that as "no placement possible" and fall back to their existing no-owner behaviour
// rather than guessing at a target.
type View struct {
	nodes []Node
}

// NewView returns an immutable snapshot of nodes, sorted by ID, with duplicates and
// unroutable entries removed.
//
// Dropping nodes with an empty ID or Addr is load-bearing, not defensive tidiness. coord.Claim
// does not validate addr, and a lease carrying an empty Addr reads as "no owner" to the gateway
// locator while still refusing every other node's claim — so a node that forgot to set its
// address could win a placement and pin a room both unroutable and unclaimable for a full lease
// TTL, repeatedly, with no runtime recovery. Registry implementations reject such a node at
// registration; dropping it here as well means a malformed row in the store cannot reintroduce
// the hazard behind their back.
//
// On duplicate IDs the first entry in ID order wins.
func NewView(nodes []Node) View {
	routable := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.ID == "" || n.Addr == "" {
			continue
		}
		routable = append(routable, n)
	}
	slices.SortFunc(routable, func(a, b Node) int { return strings.Compare(a.ID, b.ID) })
	return View{nodes: slices.CompactFunc(routable, func(a, b Node) bool { return a.ID == b.ID })}
}

// Len reports how many nodes the view holds.
func (v View) Len() int { return len(v.nodes) }

// Nodes returns the snapshot's nodes in ID order. The result is a copy, so a caller that sorts
// or appends to it cannot corrupt a View shared across goroutines.
func (v View) Nodes() []Node { return slices.Clone(v.nodes) }

// Rank orders the view for roomID by descending rendezvous weight: the node placement should
// ask first, followed by the fallbacks to try when that node refuses (draining, at capacity) or
// is penalized for a recent dial failure.
//
// The order is total and depends only on roomID and the set of node IDs — equal weights break
// on ID — so two gateways holding the same view always produce the same list, and permuting the
// input cannot change the output. Cost is O(n log n) over the fleet, paid only when the
// directory has no live lease.
func (v View) Rank(roomID string) []Node {
	scored := make([]scoredNode, len(v.nodes))
	for i, n := range v.nodes {
		scored[i] = scoredNode{node: n, w: weight(n.ID, roomID)}
	}
	slices.SortFunc(scored, compareScored)

	ranked := make([]Node, len(scored))
	for i, s := range scored {
		ranked[i] = s.node
	}
	return ranked
}

// Primary is Rank(roomID)[0] without the sort or the allocation — the common case, since
// placement asks the top-ranked node and only walks further down on refusal.
func (v View) Primary(roomID string) (Node, bool) {
	if len(v.nodes) == 0 {
		return Node{}, false
	}
	best := scoredNode{node: v.nodes[0], w: weight(v.nodes[0].ID, roomID)}
	for _, n := range v.nodes[1:] {
		if cand := (scoredNode{node: n, w: weight(n.ID, roomID)}); compareScored(cand, best) < 0 {
			best = cand
		}
	}
	return best.node, true
}

type scoredNode struct {
	node Node
	w    uint64
}

// compareScored is the single ordering Rank and Primary share, so the two cannot disagree about
// which node is first.
//
// Higher weight sorts first; equal weights break on ascending node id. That tie-break is what
// makes the result a TOTAL order rather than merely sorted — without it, two nodes of equal
// weight could come back in either order depending on the input permutation, and two gateways
// would place the same room differently. 64-bit collisions are vanishingly rare, so the branch
// is effectively unreachable through Rank in production; it is pinned directly by test instead.
func compareScored(a, b scoredNode) int {
	if a.w != b.w {
		if a.w > b.w {
			return -1 // descending: the highest scorer is asked first
		}
		return 1
	}
	return strings.Compare(a.node.ID, b.node.ID)
}

// weight is the rendezvous (highest-random-weight) score of nodeID for roomID: FNV-1a over the
// two strings, run through the splitmix64 finalizer.
//
// It must stay a pure function of its inputs, identical in every process and across restarts,
// because every gateway computes placement independently and they have to agree. That rules out
// hash/maphash, whose seed is randomized per process: using it would give each gateway a
// different placement function, and a room would re-home on every resolve. A golden vector pins
// this function in CI so the constraint cannot be broken silently — if you change the hash, you
// re-place the entire fleet at once, and the failing test is the point.
//
// The finalizer is not decoration. FNV-1a avalanches poorly on short, similar strings, so raw
// FNV over ids like "owner-0"/"owner-1" clusters badly and skews the distribution.
func weight(nodeID, roomID string) uint64 {
	return splitmix64(fnv1a(nodeID, roomID))
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// fnv1a hashes nodeID || 0x00 || roomID. The separator keeps ("ab", "c") from colliding with
// ("a", "bc").
func fnv1a(nodeID, roomID string) uint64 {
	h := uint64(fnvOffset64)
	mix := func(s string) {
		for i := range len(s) {
			h ^= uint64(s[i])
			h *= fnvPrime64
		}
	}

	mix(nodeID)
	// The FNV-1a step for the 0x00 separator byte. Its XOR is an identity by definition, so
	// only the multiply is written out — that multiply is what separates the two strings.
	h *= fnvPrime64
	mix(roomID)
	return h
}

// splitmix64 is the SplitMix64 finalizer: xor-shift/multiply rounds that avalanche a weakly
// mixed hash, with the reference constants.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Registry is the live-node directory: nodes self-report into it, readers take snapshots out.
//
// Self-reporting is the point. A node heartbeats only while it can actually serve, so
// membership means "I am serving" rather than "my pod is Ready" — and those two diverge exactly
// when it matters most, on a wedged runtime or a pod mid-drain.
//
// Pod readiness must NOT be gated on this registry. Coupling every pod's readiness to one
// shared store means a single store blip marks the whole fleet unready at once and stalls a
// rollout. A caller that gets an empty or stale view is required to degrade — place locally, or
// freeze — never to fail.
type Registry interface {
	// Heartbeat records n as live until now.Add(ttl); callers re-send well inside ttl. It
	// rejects a node with an empty ID or Addr — see NewView for why that one matters.
	Heartbeat(ctx context.Context, n Node, now time.Time, ttl time.Duration) error

	// Deregister marks nodeID as gone immediately, without waiting out its ttl. It is the first
	// step of a graceful drain and must run BEFORE the node stops accepting rooms: reversed, a
	// reader holding a slightly stale view places rooms straight back onto the departing node.
	Deregister(ctx context.Context, nodeID string, now time.Time) error

	// View returns the nodes live at now.
	View(ctx context.Context, now time.Time) (View, error)
}
