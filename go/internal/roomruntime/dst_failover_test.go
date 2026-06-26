package roomruntime_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
	"github.com/SamarthParnami/aether/go/internal/sim"
)

// Deterministic-simulation failover test.
//
// A cluster of room owners shares one durable log and one coordinator, all driven by the
// simulator's virtual clock. A stream of client commits flows in over (virtual) time while
// nodes are killed and revived: the current owner can die mid-session, and a previously
// paused node revives holding a STALE in-memory room. The seeded RNG drives the schedule,
// so a failing seed replays bit-for-bit forever.
//
// What this exercises now: ownership failover under churn — a dead owner's lease lapses and a
// survivor re-homes the room from the log, and a revived owner with stale memory is caught by
// the conditional-append backstop and rebuilds. Network-level faults on the client↔gateway
// path (drop / reorder / duplicate, partitions) land with the gateway/SDK DST once that path
// exists; here every node talks to the shared log/coord directly.
//
// Invariants asserted per seed:
//   - exactly-once: each (origin_client_id, origin_client_seq) appears at most once in the log;
//   - no loss: every commit the cluster acknowledged (applied) is in the log;
//   - total order: room_seqs are exactly 1..N, contiguous, no gaps or overwrites;
//   - convergence: a fresh node re-homing from the log alone rebuilds head == N.

const (
	dstNodes   = 3
	dstClients = 4
	dstKeys    = 5
	dstOps     = 50
	dstTTL     = 5 * time.Second
	dstTick    = 50 * time.Millisecond // client retry/backoff granularity (must be << TTL)
)

func TestDST_FailoverConvergence(t *testing.T) {
	for seed := int64(1); seed <= 120; seed++ {
		runFailoverScenario(t, seed)
	}
}

type dstOp struct {
	client string
	seq    uint64
	body   *aetherv1.EventBody
}

// ack records one commit the cluster acknowledged (Commit returned applied).
type ack struct {
	roomSeq uint64
	client  string
	seq     uint64
}

func runFailoverScenario(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	s := sim.New(seed)
	rng := s.Rand()
	log := logstore.NewMemory()
	co := coord.NewMemory()

	nodeIDs := make([]string, dstNodes)
	nodes := make(map[string]*roomruntime.Runtime, dstNodes)
	alive := make(map[string]bool, dstNodes)
	for i := range nodeIDs {
		id := fmt.Sprintf("node-%d", i)
		nodeIDs[i] = id
		nodes[id] = roomruntime.New(log, fanout.NewMemory(),
			roomruntime.WithNodeID(id),
			roomruntime.WithCoordinator(co),
			roomruntime.WithClock(s.Now),
			roomruntime.WithLeaseTTL(dstTTL),
		)
		alive[id] = true
	}

	const room = "R"

	lowestAlive := func() string {
		for _, id := range nodeIDs { // nodeIDs is sorted → deterministic owner selection
			if alive[id] {
				return id
			}
		}
		return ""
	}

	// applied accumulates every commit the cluster acknowledged, in the order it acknowledged
	// them (which is room_seq order, since the log is appended strictly increasing).
	var applied []ack
	remaining := dstOps

	// attempt drives one client op to completion, rescheduling itself on the sim timeline when
	// it must wait out a dead owner's lease or retry after a conflict — exactly what a real
	// client behind a gateway does (route to the owner, back off, retry).
	var attempt func(op dstOp)
	attempt = func(op dstOp) {
		now := s.Now()

		var target string
		if cur, ok := co.Current(room, now); ok {
			if alive[cur.Owner] {
				target = cur.Owner
			} else {
				// Dead owner still holds a live lease: the room can't be served until the lease
				// lapses, then a survivor takes over. Back off until just past expiry.
				s.Schedule(cur.Expiry.Sub(now)+dstTick, func() { attempt(op) })
				return
			}
		} else {
			target = lowestAlive() // free room: a survivor claims and re-homes it from the log
		}
		if target == "" {
			s.Schedule(dstTick, func() { attempt(op) }) // nobody available (shouldn't happen)
			return
		}

		ev, ok, err := nodes[target].Commit(ctx, room, op.client, op.seq, op.body)
		switch {
		case errors.Is(err, roomruntime.ErrNotOwner), errors.Is(err, logstore.ErrConflict):
			// Lost ownership or a stale-memory conflict (rebuilt by the runtime) — retry shortly.
			s.Schedule(dstTick, func() { attempt(op) })
		case err != nil:
			t.Fatalf("seed %d: unexpected commit error: %v", seed, err)
		case ok:
			applied = append(applied, ack{ev.GetRoomSeq(), ev.GetOriginClientId(), ev.GetOriginClientSeq()})
			remaining--
		default:
			remaining-- // duplicate (already applied) — exactly-once, treat as done
		}
	}

	// Schedule the client ops over virtual time, spaced so the run spans many lease lifetimes.
	nextSeq := map[string]uint64{}
	for i := 0; i < dstOps; i++ {
		client := fmt.Sprintf("client-%d", rng.Intn(dstClients))
		nextSeq[client]++
		op := dstOp{
			client: client,
			seq:    nextSeq[client],
			body:   kvBody(fmt.Sprintf("key-%d", rng.Intn(dstKeys)), fmt.Sprintf("v%d", i)),
		}
		// Spacing in [0, ~1.5×TTL): adjacent ops sometimes straddle a lease lifetime.
		gap := time.Duration(rng.Int63n(int64(dstTTL+dstTTL/2) + 1))
		at := time.Duration(i) * (dstTTL / 4)
		s.Schedule(at+gap, func() { attempt(op) })
	}

	// Chaos schedule: at most one node dead at a time (so ≥2 of 3 stay alive). Each toggle
	// revives the prior casualty and kills a fresh random node — which may be the live owner.
	dead := ""
	for tk := 1; ; tk++ {
		at := time.Duration(tk) * (dstTTL * 3 / 2)
		if at > time.Duration(dstOps)*(dstTTL/4)+dstTTL*4 {
			break // stop seeding chaos a little after the last op's nominal time
		}
		victim := nodeIDs[rng.Intn(dstNodes)]
		s.Schedule(at, func() {
			if dead != "" {
				alive[dead] = true // revive the prior casualty (keeps its stale in-memory room)
			}
			alive[victim] = false // hard kill: lease is left to lapse (no graceful Release)
			dead = victim
		})
	}

	if steps := s.Run(2_000_000); steps == 2_000_000 {
		t.Fatalf("seed %d: simulation hit the step cap (likely stuck)", seed)
	}
	if remaining != 0 {
		t.Fatalf("seed %d: %d/%d ops never committed (sim drained early)", seed, remaining, dstOps)
	}

	assertLogIntegrity(t, seed, log, room, applied)
}

// assertLogIntegrity checks the durable log against the acknowledged commits: exactly-once,
// no loss, contiguous total order, and that a fresh node re-homes to the same head.
func assertLogIntegrity(
	t *testing.T, seed int64, log *logstore.Memory, room string, applied []ack,
) {
	t.Helper()
	ctx := context.Background()

	events, err := log.Read(ctx, room, 0)
	if err != nil {
		t.Fatalf("seed %d: read log: %v", seed, err)
	}

	if len(events) != len(applied) {
		t.Fatalf("seed %d: log has %d events but %d commits were acked (loss or dup)",
			seed, len(events), len(applied))
	}

	seen := map[string]bool{}
	for i, e := range events {
		if got := e.GetRoomSeq(); got != uint64(i+1) {
			t.Fatalf("seed %d: room_seq at index %d = %d, want %d (gap/overwrite)", seed, i, got, i+1)
		}
		dedupKey := fmt.Sprintf("%s/%d", e.GetOriginClientId(), e.GetOriginClientSeq())
		if seen[dedupKey] {
			t.Fatalf("seed %d: duplicate commit in log: %s", seed, dedupKey)
		}
		seen[dedupKey] = true
	}

	// Every acknowledged commit is present (no acked-but-lost).
	for _, a := range applied {
		if !seen[fmt.Sprintf("%s/%d", a.client, a.seq)] {
			t.Fatalf("seed %d: acked commit %s/%d missing from log", seed, a.client, a.seq)
		}
	}

	// applied was collected in ack order; it must already be sorted by room_seq.
	if !sort.SliceIsSorted(applied, func(i, j int) bool { return applied[i].roomSeq < applied[j].roomSeq }) {
		t.Fatalf("seed %d: acks not in room_seq order", seed)
	}

	// Convergence: a brand-new node, sharing only the durable log, re-homes to the same head.
	observer := roomruntime.New(log, fanout.NewMemory(),
		roomruntime.WithNodeID("observer"),
		roomruntime.WithCoordinator(coord.NewMemory()), // its own coord ⇒ it can always claim
	)
	joined, err := observer.Join(ctx, room)
	if err != nil {
		t.Fatalf("seed %d: observer join: %v", seed, err)
	}
	if joined.GetCurrentSeq() != uint64(len(events)) {
		t.Fatalf("seed %d: observer head = %d, want %d", seed, joined.GetCurrentSeq(), len(events))
	}
}
