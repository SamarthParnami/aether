package membership

import "testing"

// TestSeparatorPreventsConcatenationCollisions pins the 0x00 byte between node id and room id.
//
// Without it the hash is taken over a plain concatenation, so node "ab" + room "c" and node "a"
// + room "bc" produce the same weight. The consequence is not academic: two different nodes
// would score identically for two different rooms, and the tie-break would then place both by
// node id rather than by hash — quietly correlating placements that are supposed to be
// independent. The external golden vector would not catch a regression here on its own.
func TestSeparatorPreventsConcatenationCollisions(t *testing.T) {
	pairs := [][2][2]string{
		{{"ab", "c"}, {"a", "bc"}},
		{{"owner-1", "0"}, {"owner-", "10"}},
		{{"n", ""}, {"", "n"}},
	}

	for _, p := range pairs {
		left, right := weight(p[0][0], p[0][1]), weight(p[1][0], p[1][1])
		if left == right {
			t.Errorf("weight(%q,%q) == weight(%q,%q) == %d — the separator is not being mixed in",
				p[0][0], p[0][1], p[1][0], p[1][1], left)
		}
	}
}

// TestWeightIsPinned locks the exact arithmetic. The exported golden vector pins the resulting
// ORDER; this pins the values it is derived from, so a change to the offset basis, the prime,
// the finalizer constants or the mixing order is caught at its source rather than as a puzzling
// reordering one layer up.
func TestWeightIsPinned(t *testing.T) {
	cases := []struct {
		node, room string
		want       uint64
	}{
		{"owner-0", "room", 0xe7007560283a9902},
		{"owner-1", "room", 0x275866e58b7c79c0},
		{"", "", 0x71b8262bb6e2e086},
	}

	for _, c := range cases {
		if got := weight(c.node, c.room); got != c.want {
			t.Errorf("weight(%q, %q) = %#016x, want %#016x", c.node, c.room, got, c.want)
		}
	}
}

// TestTieBreakIsByNodeID reaches the branch no end-to-end test can. A 64-bit weight collision
// effectively never happens on real ids, so Rank alone cannot exercise the tie-break — yet
// without it the ordering stops being total and two gateways could place the same room
// differently. Testing the comparator directly is the only way to hold it.
func TestTieBreakIsByNodeID(t *testing.T) {
	tied := func(id string) scoredNode { return scoredNode{node: Node{ID: id}, w: 42} }
	a, b := tied("a"), tied("b")

	if got := compareScored(a, b); got >= 0 {
		t.Errorf("compareScored(a, b) = %d on equal weights, want < 0 (ascending id)", got)
	}
	if got := compareScored(b, a); got <= 0 {
		t.Errorf("compareScored(b, a) = %d on equal weights, want > 0", got)
	}
	if got := compareScored(a, a); got != 0 {
		t.Errorf("compareScored(a, a) = %d, want 0", got)
	}

	// Weight must dominate the id, not the other way round.
	heavier := scoredNode{node: Node{ID: "z"}, w: 43}
	if got := compareScored(heavier, a); got >= 0 {
		t.Errorf("compareScored(heavier-but-later-id, a) = %d, want < 0", got)
	}
}

// TestSplitmix64Avalanches is a cheap sanity check on the finalizer: flipping one input bit
// should change roughly half the output bits. Raw FNV-1a fails this badly on short similar
// strings, which is the whole reason the finalizer is there.
func TestSplitmix64Avalanches(t *testing.T) {
	const trials = 64
	total := 0
	for i := range trials {
		a, b := splitmix64(uint64(i)), splitmix64(uint64(i)^1)
		diff := a ^ b
		bits := 0
		for ; diff != 0; diff &= diff - 1 {
			bits++
		}
		total += bits
	}

	if avg := float64(total) / trials; avg < 24 || avg > 40 {
		t.Errorf("average bit flips = %.1f, want ~32 (±8) — the finalizer is not avalanching", avg)
	}
}
