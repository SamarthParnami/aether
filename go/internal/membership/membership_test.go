package membership_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/SamarthParnami/aether/go/internal/membership"
)

// viewOf builds a view from node ids, giving each a distinct synthetic address so none is
// dropped as unroutable.
func viewOf(ids ...string) membership.View {
	nodes := make([]membership.Node, len(ids))
	for i, id := range ids {
		nodes[i] = membership.Node{ID: id, Addr: "10.0.0." + strconv.Itoa(i+1) + ":7001"}
	}
	return membership.NewView(nodes)
}

func rankIDs(v membership.View, roomID string) []string {
	ranked := v.Rank(roomID)
	ids := make([]string, len(ranked))
	for i, n := range ranked {
		ids[i] = n.ID
	}
	return ids
}

// goldenNodes is the fixed fleet the golden vector is pinned against.
var goldenNodes = []string{"owner-0", "owner-1", "owner-2", "node-a", "node-b"}

// TestRankGoldenVector pins the placement function itself.
//
// Every gateway computes placement independently and they must agree, so the ranking has to be
// byte-identical across processes, machines and restarts. This test is the guard: changing the
// hash, the separator, the finalizer or the tie-break silently re-places the entire fleet at
// once — every room moving to a new owner simultaneously — and this is what makes that
// impossible to do by accident. If it fails, the question is not "update the golden" but "did I
// mean to re-place every room in production".
//
// The room ids include the empty string and a lone NUL, plus the "ab"/"a" pair that would
// collide if the node id's length prefix were ever dropped.
//
// These values were re-goldened once, deliberately, when the 0x00 separator was replaced by that
// length prefix (a NUL in the input aliased the separator). Doing it before there is a cmd/ or
// anything deployed cost one golden update against zero production rooms; the same change later
// would re-place every live room simultaneously.
func TestRankGoldenVector(t *testing.T) {
	golden := map[string][]string{
		"":             {"node-b", "owner-1", "owner-2", "node-a", "owner-0"},
		"room":         {"owner-1", "node-b", "owner-0", "node-a", "owner-2"},
		"class-721363": {"owner-1", "owner-0", "node-a", "owner-2", "node-b"},
		"r-1":          {"owner-0", "node-b", "node-a", "owner-1", "owner-2"},
		"r-2":          {"owner-2", "owner-1", "node-b", "node-a", "owner-0"},
		"\x00":         {"node-b", "owner-2", "node-a", "owner-0", "owner-1"},
		"ab":           {"owner-0", "owner-2", "owner-1", "node-a", "node-b"},
		"a":            {"node-a", "owner-1", "node-b", "owner-0", "owner-2"},
	}

	v := viewOf(goldenNodes...)
	for room, want := range golden {
		if got := rankIDs(v, room); !slices.Equal(got, want) {
			t.Errorf("Rank(%q) = %v, want %v", room, got, want)
		}
	}
}

// TestRankIsIndependentOfInputOrder is the other half of cross-process agreement: two gateways
// that learned the same nodes in a different order (map iteration, a differently-ordered store
// response) must still rank identically.
func TestRankIsIndependentOfInputOrder(t *testing.T) {
	forward := viewOf(goldenNodes...)

	reversed := slices.Clone(goldenNodes)
	slices.Reverse(reversed)
	backward := viewOf(reversed...)

	for _, room := range []string{"", "room", "r-1", "class-721363"} {
		a, b := rankIDs(forward, room), rankIDs(backward, room)
		if !slices.Equal(a, b) {
			t.Errorf("Rank(%q) depends on input order: %v vs %v", room, a, b)
		}
	}
}

// TestRankIsATotalOrder — every node appears exactly once, so a caller walking the ranking on
// refusal can never loop forever or skip a candidate.
func TestRankIsATotalOrder(t *testing.T) {
	v := viewOf(goldenNodes...)
	for i := range 500 {
		ranked := rankIDs(v, "room-"+strconv.Itoa(i))
		if len(ranked) != len(goldenNodes) {
			t.Fatalf("Rank returned %d nodes, want %d", len(ranked), len(goldenNodes))
		}
		sorted := slices.Clone(ranked)
		slices.Sort(sorted)
		if !slices.Equal(sorted, slices.Sorted(slices.Values(goldenNodes))) {
			t.Fatalf("Rank is not a permutation of the view: %v", ranked)
		}
	}
}

func TestPrimaryMatchesRankHead(t *testing.T) {
	v := viewOf(goldenNodes...)
	for i := range 500 {
		room := "room-" + strconv.Itoa(i)
		p, ok := v.Primary(room)
		if !ok {
			t.Fatalf("Primary(%q) reported no node for a non-empty view", room)
		}
		if head := v.Rank(room)[0]; p.ID != head.ID {
			t.Fatalf("Primary(%q) = %q, Rank head = %q", room, p.ID, head.ID)
		}
	}
}

// TestNewViewDropsUnroutableNodes covers the hazard NewView exists to close: a node with no
// address that wins a placement pins the room unroutable AND unclaimable for a full lease TTL.
func TestNewViewDropsUnroutableNodes(t *testing.T) {
	v := membership.NewView([]membership.Node{
		{ID: "good", Addr: "10.0.0.1:7001"},
		{ID: "no-addr", Addr: ""},
		{ID: "", Addr: "10.0.0.2:7001"},
	})

	if v.Len() != 1 {
		t.Fatalf("view holds %d nodes, want 1: %+v", v.Len(), v.Nodes())
	}
	if got := v.Nodes()[0].ID; got != "good" {
		t.Errorf("surviving node = %q, want \"good\"", got)
	}
}

func TestNewViewDeduplicatesAndSorts(t *testing.T) {
	v := membership.NewView([]membership.Node{
		{ID: "b", Addr: "10.0.0.2:7001"},
		{ID: "a", Addr: "10.0.0.1:7001"},
		{ID: "b", Addr: "10.0.0.9:7001"}, // duplicate id, later address
	})

	ids := make([]string, 0, v.Len())
	for _, n := range v.Nodes() {
		ids = append(ids, n.ID)
	}
	if want := []string{"a", "b"}; !slices.Equal(ids, want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	if got := v.Nodes()[1].Addr; got != "10.0.0.2:7001" {
		t.Errorf("duplicate resolution kept %q, want the first entry in ID order", got)
	}
}

// TestZeroViewPlacesNothing — an empty or unconfigured view must report "no placement possible"
// rather than panicking or inventing a target, because that is the degraded path every caller
// falls back to when the registry is empty or stale.
func TestZeroViewPlacesNothing(t *testing.T) {
	var zero membership.View

	if zero.Len() != 0 {
		t.Errorf("zero view Len = %d, want 0", zero.Len())
	}
	if got := zero.Rank("room"); len(got) != 0 {
		t.Errorf("zero view Rank = %v, want empty", got)
	}
	if _, ok := zero.Primary("room"); ok {
		t.Error("zero view Primary reported a node")
	}
}

// TestNodesReturnsACopy — a View is shared across gateway goroutines, so handing out the
// backing array would let one caller's sort corrupt everyone else's placement.
func TestNodesReturnsACopy(t *testing.T) {
	v := viewOf("a", "b", "c")

	got := v.Nodes()
	slices.Reverse(got)
	got[0] = membership.Node{ID: "clobbered", Addr: "x"}

	if after := v.Nodes()[0].ID; after != "a" {
		t.Fatalf("mutating the result of Nodes() changed the View: head is now %q", after)
	}
}

// TestProp_AddingANodeOnlyMovesRoomsToIt is the rendezvous-hashing guarantee that makes a scale
// -up safe: a room either stays exactly where it was or moves to the node that just joined.
// Nothing shuffles between incumbents, so growing the fleet costs the minimum possible rebuild.
func TestProp_AddingANodeOnlyMovesRoomsToIt(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ids := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z0-9-]{1,8}`), 2, 9,
			func(s string) string { return s },
		).Draw(t, "ids")

		added := ids[len(ids)-1]
		before, after := viewOf(ids[:len(ids)-1]...), viewOf(ids...)

		for i := range 200 {
			room := "room-" + strconv.Itoa(i)
			was, ok := before.Primary(room)
			if !ok {
				t.Fatalf("no primary for %q in a non-empty view", room)
			}
			now, _ := after.Primary(room)
			if now.ID != was.ID && now.ID != added {
				t.Fatalf("room %q moved %q -> %q; adding %q must only pull rooms to itself",
					room, was.ID, now.ID, added)
			}
		}
	})
}

// TestProp_RemovingANodeOnlyMovesItsOwnRooms is the same guarantee for the case that actually
// hurts: when a node dies holding rooms, only ITS rooms re-home. Every other room stays put, so
// one failure cannot cascade into a fleet-wide rebuild.
func TestProp_RemovingANodeOnlyMovesItsOwnRooms(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ids := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z0-9-]{1,8}`), 2, 9,
			func(s string) string { return s },
		).Draw(t, "ids")
		idx := rapid.IntRange(0, len(ids)-1).Draw(t, "removed")
		removed := ids[idx]

		before := viewOf(ids...)
		after := viewOf(slices.Concat(ids[:idx], ids[idx+1:])...)

		for i := range 200 {
			room := "room-" + strconv.Itoa(i)
			was, _ := before.Primary(room)
			now, ok := after.Primary(room)
			if !ok {
				t.Fatalf("no primary for %q after removing %q", room, removed)
			}
			if was.ID != removed && now.ID != was.ID {
				t.Fatalf("room %q moved %q -> %q, but its owner %q was not the removed node %q",
					room, was.ID, now.ID, was.ID, removed)
			}
		}
	})
}

// TestRankSpreadsRoomsEvenly guards the finalizer. Raw FNV-1a over short, similar ids like
// "owner-0"/"owner-1" clusters badly; without the splitmix64 avalanche one node would take a
// wildly disproportionate share of rooms and the whole point of hashing would be lost.
//
// Fully deterministic — fixed nodes, fixed room ids — so this either passes or fails, never
// flakes.
func TestRankSpreadsRoomsEvenly(t *testing.T) {
	const (
		rooms     = 20000
		tolerance = 0.10
	)
	v := viewOf("owner-0", "owner-1", "owner-2", "owner-3", "owner-4", "owner-5", "owner-6", "owner-7")

	counts := map[string]int{}
	for i := range rooms {
		p, _ := v.Primary("room-" + strconv.Itoa(i))
		counts[p.ID]++
	}

	ideal := float64(rooms) / float64(v.Len())
	for _, n := range v.Nodes() {
		got := float64(counts[n.ID])
		if drift := (got - ideal) / ideal; drift > tolerance || drift < -tolerance {
			t.Errorf("node %s took %d rooms, want %.0f ± %.0f%% (drift %+.1f%%)",
				n.ID, counts[n.ID], ideal, tolerance*100, drift*100)
		}
	}
}

// TestPackageDoesNotImportMaphash is a lint with teeth. hash/maphash is the obvious stdlib
// choice here and it is the one thing that must never be used: its seed is randomized per
// process, so each gateway would compute a different placement function and a room would
// re-home on every resolve. The bug would not show up in any single-process test — including
// every other test in this file — which is exactly why it is pinned structurally instead.
func TestPackageDoesNotImportMaphash(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no source files found — this guard would silently pass")
	}

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, imp := range parsed.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", f, err)
			}
			if path == "hash/maphash" {
				t.Errorf("%s imports hash/maphash — its per-process random seed would give every "+
					"gateway a different placement function; see the weight() doc comment", f)
			}
		}
	}
}
