package roomcore

import (
	"testing"

	"pgregory.net/rapid"
)

func TestCursorClassifies(t *testing.T) {
	c := NewCursor(415)
	if d := c.Offer(416); d != DecisionApply {
		t.Errorf("contiguous: got %v, want apply", d)
	}
	if c.Last() != 416 {
		t.Errorf("Last() = %d, want 416", c.Last())
	}
	if d := c.Offer(416); d != DecisionSkip {
		t.Errorf("duplicate: got %v, want skip", d)
	}
	if d := c.Offer(420); d != DecisionGap {
		t.Errorf("gap: got %v, want gap", d)
	}
	if c.Last() != 416 {
		t.Errorf("Last() advanced past 416 on skip/gap: %d", c.Last())
	}
}

// A contiguous stream always applies and advances the cursor to the last seq.
func TestProp_CursorContiguousApplies(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		from := uint64(rapid.IntRange(0, 1000).Draw(t, "from"))
		n := rapid.IntRange(0, 50).Draw(t, "n")
		c := NewCursor(from)
		for i := 1; i <= n; i++ {
			seq := from + uint64(i)
			if d := c.Offer(seq); d != DecisionApply {
				t.Fatalf("contiguous seq %d: got %v, want apply", seq, d)
			}
		}
		if want := from + uint64(n); c.Last() != want {
			t.Fatalf("Last() = %d, want %d", c.Last(), want)
		}
	})
}

// Duplicates and gaps are detected and never advance the cursor.
func TestProp_CursorDetectsSkipAndGap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		last := uint64(rapid.IntRange(1, 1000).Draw(t, "last"))
		c := NewCursor(last)

		dup := uint64(rapid.IntRange(0, int(last)).Draw(t, "dup")) // <= last
		if d := c.Offer(dup); d != DecisionSkip {
			t.Fatalf("dup %d: got %v, want skip", dup, d)
		}

		ahead := uint64(rapid.IntRange(2, 1000).Draw(t, "ahead"))
		if d := c.Offer(last + ahead); d != DecisionGap {
			t.Fatalf("gap %d: got %v, want gap", last+ahead, d)
		}

		if c.Last() != last {
			t.Fatalf("Last() changed from %d to %d on skip/gap", last, c.Last())
		}
	})
}
