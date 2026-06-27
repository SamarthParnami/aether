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
// the conditional-append backstop and rebuilds. It also models the LOST ACK: with some
// probability a client never sees its commit fanned back (its owner died right after persisting)
// and retries the same (client_id, client_seq); dedup must reject the replay so it never lands a
// second time. Network-level faults on the client↔gateway path (drop / reorder / duplicate,
// partitions) land with the gateway/SDK DST once that path exists; here every node talks to the
// shared log/coord directly.
//
// Invariants asserted per seed:
//   - exactly-once: each (origin_client_id, origin_client_seq) appears at most once in the log,
//     AND a lost-ack retry of an already-persisted op is deduplicated (never a second entry);
//   - no loss: every commit the cluster acknowledged (applied) is in the log;
//   - total order: room_seqs are exactly 1..N, contiguous, no gaps or overwrites;
//   - convergence: a fresh node re-homing from the log alone rebuilds head == N.
//
// And asserted ONCE across the whole sweep (so the chaos can't silently degrade to a green
// happy-path run): every failure path — stale-reclaim conflict, survivor takeover, dead-owner
// backoff, lost-ack dedup — fired at least once.

const (
	dstNodes    = 3
	dstClients  = 4
	dstKeys     = 5
	dstOps      = 50
	dstTTL      = 5 * time.Second
	dstTick     = 50 * time.Millisecond // client retry/backoff granularity (must be << TTL)
	dstToggles  = 24                    // number of kill/revive chaos events (spans well past the ops)
	lostAckProb = 0.4                   // chance a committed op's ack is "lost" and the client retries it
)

func TestDST_FailoverConvergence(t *testing.T) {
	var total chaosCounts
	for seed := int64(1); seed <= 120; seed++ {
		total.add(runFailoverScenario(t, seed))
	}

	// Teeth check: fail if any failure path stopped firing across the sweep. Without this a
	// routing/timing tweak (or a subtle bug that stops nodes ever conflicting) could quietly
	// turn this into a green happy-path run that proves nothing.
	if total.conflict == 0 {
		t.Error("no stale-reclaim conflicts in the sweep — the conditional-append backstop was never exercised")
	}
	if total.takeover == 0 {
		t.Error("no survivor takeovers in the sweep — failover was never exercised")
	}
	if total.wait == 0 {
		t.Error("no dead-owner backoffs in the sweep — owner death was never exercised")
	}
	if total.dedup == 0 {
		t.Error("no lost-ack replays deduplicated in the sweep — exactly-once was never exercised")
	}
	t.Logf("teeth over 120 seeds: conflict=%d takeover=%d wait=%d dedup=%d",
		total.conflict, total.takeover, total.wait, total.dedup)
}

// chaosCounts tallies the interesting transitions a scenario actually exercised, so the test
// can prove the chaos still has teeth instead of trusting it does.
type chaosCounts struct {
	conflict int // stale-reclaim hit logstore.ErrConflict and rebuilt
	takeover int // a survivor claimed a free/lapsed room and re-homed it
	wait     int // a client backed off because the owner was dead but still leased
	dedup    int // a lost-ack retry was rejected by dedup (no second log entry)
}

func (c *chaosCounts) add(o chaosCounts) {
	c.conflict += o.conflict
	c.takeover += o.takeover
	c.wait += o.wait
	c.dedup += o.dedup
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

func runFailoverScenario(t *testing.T, seed int64) chaosCounts {
	t.Helper()
	ctx := context.Background()
	var c chaosCounts

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

	// Per-client op plans: client -> bodies, where seq i+1 is bodies[i]. Each client submits its
	// ops strictly in order, one in flight at a time (advance only after the current op is
	// confirmed) — the single-connection, monotonic-client_seq contract the high-water dedup
	// relies on. Submitting a client's seqs out of order would itself be the bug, not a fault.
	plans := map[string][]*aetherv1.EventBody{}
	var clientList []string
	for i := 0; i < dstOps; i++ {
		client := fmt.Sprintf("client-%d", rng.Intn(dstClients))
		if _, ok := plans[client]; !ok {
			clientList = append(clientList, client) // first-seen order ⇒ deterministic, no map ranging
		}
		plans[client] = append(plans[client], kvBody(fmt.Sprintf("key-%d", rng.Intn(dstKeys)), fmt.Sprintf("v%d", i)))
	}

	var attempt func(op dstOp, isRetry bool)

	// advance marks op (client, seq) fully done and submits the client's next op in order, if any.
	advance := func(client string, seq uint64) {
		remaining--
		if int(seq) < len(plans[client]) { // a seq+1 exists (bodies index seq)
			next := dstOp{client: client, seq: seq + 1, body: plans[client][seq]}
			s.Schedule(time.Duration(rng.Int63n(int64(dstTTL)+1))+dstTick, func() { attempt(next, false) })
		}
	}

	// attempt drives one client op, rescheduling itself on the sim timeline when it must wait out
	// a dead owner's lease or retry after a conflict — exactly what a real client behind a gateway
	// does (route to the owner, back off, retry). isRetry marks a lost-ack replay: a resend of an
	// op that already persisted, which dedup must reject.
	attempt = func(op dstOp, isRetry bool) {
		now := s.Now()

		var target string
		if cur, ok := co.Current(room, now); ok {
			if alive[cur.Owner] {
				target = cur.Owner
			} else {
				// Dead owner still holds a live lease: the room can't be served until the lease
				// lapses, then a survivor takes over. Back off until just past expiry.
				c.wait++
				s.Schedule(cur.Expiry.Sub(now)+dstTick, func() { attempt(op, isRetry) })
				return
			}
		} else {
			target = lowestAlive() // free room: a survivor claims and re-homes it from the log
			c.takeover++
		}
		if target == "" {
			s.Schedule(dstTick, func() { attempt(op, isRetry) }) // nobody available (shouldn't happen)
			return
		}

		ev, ok, err := nodes[target].Commit(ctx, room, op.client, op.seq, op.body)
		switch {
		case errors.Is(err, roomruntime.ErrNotOwner), errors.Is(err, logstore.ErrConflict):
			// Lost ownership or a stale-memory conflict (rebuilt by the runtime) — retry shortly.
			if errors.Is(err, logstore.ErrConflict) {
				c.conflict++
			}
			s.Schedule(dstTick, func() { attempt(op, isRetry) })
		case err != nil:
			t.Fatalf("seed %d: unexpected commit error: %v", seed, err)
		case ok:
			if isRetry {
				// A resend of an already-persisted op was applied AGAIN — exactly-once is broken.
				t.Fatalf("seed %d: lost-ack retry of %s/%d landed a second log entry", seed, op.client, op.seq)
			}
			applied = append(applied, ack{ev.GetRoomSeq(), ev.GetOriginClientId(), ev.GetOriginClientSeq()})
			// Model a lost ack: the owner persisted but the client never saw the fanned-back event
			// (its owner may have died right after) and retries the SAME seq after a delay that can
			// straddle a failover, before advancing. dedup must reject it — the retry's dedup is
			// what marks the op done; on no lost ack we advance immediately.
			if rng.Float64() < lostAckProb {
				s.Schedule(time.Duration(rng.Int63n(int64(dstTTL)+1))+dstTick, func() { attempt(op, true) })
			} else {
				advance(op.client, op.seq)
			}
		default:
			// Duplicate (already applied). Only a lost-ack retry should ever land here — a first,
			// in-order submission can't dedup (its high-water is exactly seq-1), so a dup here for a
			// non-retry would be a real bug.
			if !isRetry {
				t.Fatalf("seed %d: first submission of %s/%d was unexpectedly deduplicated", seed, op.client, op.seq)
			}
			c.dedup++
			advance(op.client, op.seq) // lost-ack replay correctly rejected — op is truly done
		}
	}

	// Kick off each client's first op, staggered over virtual time so chains overlap and the run
	// spans many lease lifetimes.
	for idx, client := range clientList {
		first := dstOp{client: client, seq: 1, body: plans[client][0]}
		start := time.Duration(idx)*(dstTTL/4) + time.Duration(rng.Int63n(int64(dstTTL)+1))
		s.Schedule(start, func() { attempt(first, false) })
	}

	// Chaos schedule: at most one node dead at a time (so ≥2 of 3 stay alive). Each toggle
	// revives the prior casualty and kills a fresh random node — which may be the live owner.
	// A fixed cadence spanning well past the ops; extra toggles after the run drains are no-ops.
	dead := ""
	for tk := 1; tk <= dstToggles; tk++ {
		at := time.Duration(tk) * (dstTTL * 3 / 2)
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
	return c
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
