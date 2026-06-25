// Package sim is a deterministic discrete-event simulator.
//
// All time, randomness, and message delivery flow through it, so any run replays
// bit-for-bit from its seed. It is the engine the chaos tests use to inject faults
// (drop / delay / reorder / duplicate / partition) and reproduce failures exactly —
// a failing run carries a seed you can replay forever.
//
// Time is real time.Duration / time.Time, not abstract ticks. That means domain code can
// express durations the same way in tests and in production (e.g. a 6s lease TTL); only
// the clock *source* is swapped (this Sim under test, the wall clock in prod). No unit
// conversion is ever needed.
package sim

import (
	"container/heap"
	"math/rand"
	"time"
)

// epoch is the deterministic start time of every simulation.
var epoch = time.Unix(0, 0).UTC()

// Sim is a single-threaded discrete-event simulator driven by a seeded RNG and a virtual
// clock. Callers schedule work with Schedule and drive time forward with Run.
type Sim struct {
	rng   *rand.Rand
	clock time.Time
	seq   uint64
	queue eventQueue
}

// New returns a Sim seeded for reproducibility. The same seed always yields the same run.
// The virtual clock starts at a fixed epoch.
func New(seed int64) *Sim {
	s := &Sim{
		rng:   rand.New(rand.NewSource(seed)), //nolint:gosec // determinism, not security
		clock: epoch,
	}
	heap.Init(&s.queue)
	return s
}

// Now returns the current virtual time.
func (s *Sim) Now() time.Time { return s.clock }

// Rand returns the simulator's deterministic RNG. Anything needing randomness MUST use
// this (never the global rand or the wall clock) or runs stop being reproducible.
func (s *Sim) Rand() *rand.Rand { return s.rng }

// Schedule runs fn after delay from the current virtual time.
func (s *Sim) Schedule(delay time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.queue, &event{at: s.clock.Add(delay), seq: s.seq, fn: fn})
}

// Run processes scheduled events in (time, insertion) order, advancing the clock to each
// event's time, until the queue drains or maxSteps events have run. It returns the number
// of events processed.
func (s *Sim) Run(maxSteps int) int {
	steps := 0
	for s.queue.Len() > 0 && steps < maxSteps {
		e := heap.Pop(&s.queue).(*event)
		s.clock = e.at
		e.fn()
		steps++
	}
	return steps
}

// event is one scheduled callback. seq breaks ties so equal-time events run in insertion
// order, making the schedule a total, deterministic order.
type event struct {
	at  time.Time
	seq uint64
	fn  func()
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if !q[i].at.Equal(q[j].at) {
		return q[i].at.Before(q[j].at)
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return e
}
