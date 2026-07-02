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

// watcher drains a joined client's frames in the background, recording the room_seqs it receives (in
// arrival order) and whether it saw FROZEN/LIVE — so the chaos sweep can assert no-loss /
// exactly-once / order / convergence / recovery-not-reload once the scenario quiesces. Guarded by a
// mutex because the draining goroutine writes while the test goroutine snapshots.
type watcher struct {
	mu     sync.Mutex
	events []uint64
	frozen bool
	live   bool
}

func newWatcher(ctx context.Context, pipe frameConn) *watcher {
	w := &watcher{}
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
			w.mu.Lock()
			switch {
			case m.GetEvent() != nil:
				w.events = append(w.events, m.GetEvent().GetRoomSeq())
			case m.GetRoomStatus().GetStatus() == aetherv1.RoomStatus_STATUS_FROZEN:
				w.frozen = true
			case m.GetRoomStatus().GetStatus() == aetherv1.RoomStatus_STATUS_LIVE:
				w.live = true
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *watcher) snapshot() (events []uint64, frozen, live bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]uint64(nil), w.events...), w.frozen, w.live
}

func equalSeqs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDST_ChaosFailoverConvergence is the Phase-1 exit gate for the client↔gateway↔owner path: over
// a seed sweep, a room's owner is KILLED mid-session and re-homed to a survivor (off the shared log),
// and every watcher — spread across gateways — must still converge on the exact committed stream with
// no loss, no duplicate, in order, and recover transparently (FROZEN→LIVE), never reloading from
// zero. The connection-level owner-death fault is injected at the byte-stream seam; the fake clock
// drives lease expiry + relay backoff, so each seed runs instantly and the invariants are checked
// every seed. (Message-level faults via a Connect interceptor, and the commit path under chaos, layer
// on next — see 06-design-dst.md.)
func TestDST_ChaosFailoverConvergence(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runChaosSeed(t, seed)
		})
	}
}

func runChaosSeed(t *testing.T, seed uint64) {
	synctest.Test(t, func(t *testing.T) {
		rng := mathrand.New(mathrand.NewPCG(seed, seed))
		cl, gws := newDSTCluster(3, 2) // 3 owners (2 survivors), 2 gateways
		defer cl.stopAll()
		bg := context.Background()
		ctx, cancel := context.WithCancel(bg)
		defer cancel()

		const room = "room"
		var seq uint64
		commit := func(owner int) {
			seq++
			if _, applied, err := cl.owners[owner].Commit(bg, room, "presenter", seq, eventBody("v", fmt.Sprint(seq))); err != nil || !applied {
				t.Fatalf("commit seq %d to owner %d: applied=%v err=%v", seq, owner, applied, err)
			}
		}
		commit(0) // seq 1 — owner-0 claims the room

		// Watchers (seeded count) join across the gateways, all caught up to seq 1 via the snapshot.
		ws := make([]*watcher, 2+int(rng.Int64N(3))) // 2..4
		for i := range ws {
			pipe, stop := cl.dial(ctx, gws[i%len(gws)], fmt.Sprintf("w%d", i))
			defer stop()
			joinRoom(ctx, t, pipe, room, fmt.Sprintf("n%d", i))
			if s := readPipe(ctx, t, pipe).GetJoined().GetCurrentSeq(); s != 1 {
				t.Fatalf("watcher %d joined seq = %d, want 1", i, s)
			}
			ws[i] = newWatcher(ctx, pipe)
		}

		// Phase 1: a seeded burst of commits to owner-0, delivered to every watcher.
		for n := 1 + int(rng.Int64N(4)); n > 0; n-- {
			commit(0)
		}
		synctest.Wait()

		// FAULT: kill owner-0; its lease lapses on the fake clock; a survivor takes over.
		cl.killOwner(0)
		time.Sleep(11 * time.Second) // past roomruntime's 10s lease TTL → owner-0's lease expires

		// Phase 2: commits to the survivor (owner-1), which re-homes the room from the shared log.
		for n := 1 + int(rng.Int64N(4)); n > 0; n-- {
			commit(1)
		}
		// Let the relays re-resolve to the survivor and resume from their cursors (backoff caps at 5s).
		time.Sleep(2 * relayRetryCap)
		synctest.Wait()

		cancel()        // stop the watcher/conn goroutines
		synctest.Wait() // …and let them exit before we snapshot

		// Invariants. Watchers join at seq 1, so they must receive exactly 2..seq, contiguous & ordered.
		want := make([]uint64, 0, seq-1)
		for s := uint64(2); s <= seq; s++ {
			want = append(want, s)
		}
		for i, w := range ws {
			got, frozen, live := w.snapshot()
			if !equalSeqs(got, want) { // no-loss + exactly-once + total order
				t.Fatalf("watcher %d events = %v, want %v", i, got, want)
			}
			if !frozen || !live { // recovery, not reload: the kill surfaced FROZEN and recovered LIVE
				t.Fatalf("watcher %d frozen=%v live=%v, want both", i, frozen, live)
			}
		}
	})
}
