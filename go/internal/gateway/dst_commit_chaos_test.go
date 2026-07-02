package gateway

import (
	"context"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// presenter is a gateway CLIENT that commits with recovery — a model of the TS SDK's commit path.
// It draws acks from the fan-out (an Event echoed back with its own origin_client_id/seq is the ack),
// buffers while FROZEN, and re-sends any un-acked client_seq once LIVE. Exactly-once is the owner's
// job: a re-send of a commit that already applied (before an owner died) dedups on (client_id, seq)
// and is acked via the log replay the relay resumes into — never a double-apply.
type presenter struct {
	mu    sync.Mutex
	acked map[uint64]bool
}

func newPresenter() *presenter { return &presenter{acked: map[uint64]bool{}} }

// read drains frames in the background: an Event echoed back for our client_id is the ack for that
// client_seq (fan-out is the ack). RoomStatus is ignored — the presenter resends on its own cadence,
// not gated on LIVE (see pump).
func (p *presenter) read(ctx context.Context, pipe frameConn, myID string) {
	go func() {
		for {
			data, err := pipe.ReadFrame(ctx)
			if err != nil {
				return
			}
			var m aetherv1.ServerMessage
			if proto.Unmarshal(data, &m) != nil {
				continue
			}
			if ev := m.GetEvent(); ev != nil && ev.GetOriginClientId() == myID {
				p.mu.Lock()
				p.acked[ev.GetOriginClientSeq()] = true
				p.mu.Unlock()
			}
		}
	}()
}

func (p *presenter) numAcked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.acked)
}

func (p *presenter) ackedSet() map[uint64]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	a := make(map[uint64]bool, len(p.acked))
	for k := range p.acked {
		a[k] = true
	}
	return a
}

func writeCommit(ctx context.Context, t *testing.T, pipe frameConn, room string, seq uint64) {
	t.Helper()
	writePipe(ctx, t, pipe, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Commit{Commit: &aetherv1.Commit{
		RoomId: room, ClientSeq: seq, Body: eventBody("p", fmt.Sprint(seq)),
	}}})
}

// TestDST_ChaosCommitExactlyOnce is the write-path exit gate: a presenter commits THROUGH a gateway
// while its room's owner is killed and re-homed, and every commit must land in the log EXACTLY ONCE —
// a commit that applied before the owner died must not double-apply when the presenter resubmits
// (dedup across the re-home), and none may be lost (Nack UNAVAILABLE → resubmit on LIVE). Placement is
// external, as in every DST test (the gateway can't route a room with no live owner — the missing
// owner-placement mechanism, noted for the deployability work).
func TestDST_ChaosCommitExactlyOnce(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runCommitChaosSeed(t, seed)
		})
	}
}

func runCommitChaosSeed(t *testing.T, seed uint64) {
	synctest.Test(t, func(t *testing.T) {
		rng := mathrand.New(mathrand.NewPCG(seed, seed))
		cl, gws := newDSTCluster(seed, 3, 1) // 3 owners, 1 gateway (the presenter's)
		defer cl.stopAll()
		bg := context.Background()
		ctx, cancel := context.WithCancel(bg)
		defer cancel()

		const room = "room"
		// Bootstrap: owner-0 claims the room (room_seq 1).
		if _, applied, err := cl.owners[0].Commit(bg, room, "bootstrap", 1, eventBody("k", "0")); err != nil || !applied {
			t.Fatalf("bootstrap: applied=%v err=%v", applied, err)
		}

		pipe, stop := cl.dial(ctx, gws[0], "presenter")
		defer stop()
		joinRoom(ctx, t, pipe, room, "np")
		clientID := readPipe(ctx, t, pipe).GetJoined().GetClientId()
		p := newPresenter()
		p.read(ctx, pipe, clientID)

		n := uint64(4 + rng.Int64N(5))                          // 4..8 commits
		killAfter := uint64(1) + uint64(rng.Int64N(int64(n-1))) // ack this many to owner-0 before kill

		// pump re-sends every un-acked seq in [1,limit], advancing the fake clock, until cond holds or
		// the budget is spent. It re-sends REGARDLESS of FROZEN/LIVE on purpose: the commit path
		// (handleCommit → the room's current owner) works the moment a new owner is reachable, and that
		// resent commit is itself the first post-recovery event that unblocks the relay's Subscribe and
		// drives LIVE + the ack. Gating resends on LIVE would deadlock (no LIVE until an event, no event
		// until a commit). A commit that already applied dedups — so liberal resends stay exactly-once.
		pump := func(limit uint64, cond func() bool) bool {
			for i := 0; i < 400; i++ {
				if cond() {
					return true
				}
				acked := p.ackedSet()
				for seq := uint64(1); seq <= limit; seq++ {
					if !acked[seq] {
						writeCommit(ctx, t, pipe, room, seq)
					}
				}
				time.Sleep(200 * time.Millisecond)
				synctest.Wait()
			}
			return cond()
		}

		// Phase 1: commit 1..killAfter to owner-0.
		if !pump(killAfter, func() bool { return uint64(p.numAcked()) >= killAfter }) {
			t.Fatalf("phase 1 stalled: %d/%d acked", p.numAcked(), killAfter)
		}

		// FAILOVER: kill owner-0, expire its lease, and (external placement) put the room on owner-1.
		cl.killOwner(0)
		time.Sleep(11 * time.Second) // past the 10s lease TTL
		if _, err := cl.owners[1].Join(bg, room); err != nil {
			t.Fatalf("owner-1 claim: %v", err)
		}

		// Recovery: the presenter resends up to n; owner-1 applies the new ones and dedups any that
		// already applied at owner-0, acking the latter via the relay's resumed log replay.
		if !pump(n, func() bool { return uint64(p.numAcked()) >= n }) {
			t.Fatalf("recovery stalled: %d/%d acked", p.numAcked(), n)
		}

		cancel()
		synctest.Wait()

		// Exactly-once, ground truth: the presenter's client_seqs are exactly {1..n} in the shared log,
		// each once — no double-apply from a resend, none lost.
		events, err := cl.log.Read(bg, room, 0)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[uint64]int{}
		for _, ev := range events {
			if ev.GetOriginClientId() == clientID {
				seen[ev.GetOriginClientSeq()]++
			}
		}
		if len(seen) != int(n) {
			t.Fatalf("log has %d distinct presenter client_seqs, want %d", len(seen), n)
		}
		for seq := uint64(1); seq <= n; seq++ {
			if seen[seq] != 1 {
				t.Fatalf("client_seq %d appears %d times in the log, want exactly 1", seq, seen[seq])
			}
		}
	})
}
