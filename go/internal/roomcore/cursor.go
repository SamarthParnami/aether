package roomcore

// Decision is how a consumer should treat an incoming event given its cursor.
type Decision int

const (
	// DecisionApply means the event is next in sequence — apply it; the cursor advances.
	DecisionApply Decision = iota
	// DecisionSkip means the event was already seen (room_seq <= cursor) — ignore it.
	DecisionSkip
	// DecisionGap means events were missed (room_seq > cursor+1) — resume from the cursor.
	DecisionGap
)

func (d Decision) String() string {
	switch d {
	case DecisionApply:
		return "apply"
	case DecisionSkip:
		return "skip"
	case DecisionGap:
		return "gap"
	default:
		return "unknown"
	}
}

// Cursor tracks the highest contiguous room_seq a consumer has applied (its lastSeq)
// and classifies incoming events as apply / skip / gap. It is the gap-detection logic
// the SDK and gateway use to decide whether to apply an event, ignore a duplicate, or
// trigger a resume. Pure, no I/O.
type Cursor struct{ last uint64 }

// NewCursor starts a cursor at `from` — the last applied room_seq (0 for a fresh room).
func NewCursor(from uint64) *Cursor { return &Cursor{last: from} }

// Last returns the highest contiguously-applied room_seq.
func (c *Cursor) Last() uint64 { return c.last }

// Offer classifies an event by its room_seq relative to the cursor. On DecisionApply
// the cursor advances to room_seq; DecisionSkip and DecisionGap leave it unchanged.
func (c *Cursor) Offer(roomSeq uint64) Decision {
	switch {
	case roomSeq <= c.last:
		return DecisionSkip
	case roomSeq == c.last+1:
		c.last = roomSeq
		return DecisionApply
	default:
		return DecisionGap
	}
}
