package gateway_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/gateway"
	"github.com/SamarthParnami/aether/go/internal/logstore"
)

// When the owner a client is relayed from dies and the room re-homes to a survivor, the gateway
// recovers the live feed transparently: it re-resolves the new owner and re-subscribes from the
// cursor (gap-free, off the shared log), signalling FROZEN then LIVE — the client never re-Joins.
func TestRelayRecoversAfterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bg := context.Background()

	// A and B share one durable log + coordinator, so B can re-home a room A owned.
	co := coord.NewMemory()
	log := logstore.NewMemory()
	a, stopA := startOwnerWithLog(t, co, log, "A")
	b, _ := startOwnerWithLog(t, co, log, "B")

	if _, applied, err := a.Commit(bg, "room", "x", 1, kvBody("k", "1")); err != nil || !applied {
		t.Fatalf("A seed commit: applied=%v err=%v", applied, err)
	}

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", SessionNonce: "n",
	}}})
	if joined := readFrame(ctx, t, ws).GetJoined(); joined == nil || joined.GetCurrentSeq() != 1 {
		t.Fatalf("joined = %v, want current_seq 1", joined)
	}

	// Deliver one live event so the relay's Tail has definitely subscribed to A before the handoff.
	// This is what makes the test exercise RE-subscription rather than a first subscription that
	// happens to land on B — it is setup, not a workaround, so it stays.
	if _, applied, err := a.Commit(bg, "room", "x", 2, kvBody("k", "2")); err != nil || !applied {
		t.Fatalf("A warm-up commit: applied=%v err=%v", applied, err)
	}
	if ev := readFrame(ctx, t, ws).GetEvent(); ev == nil || ev.GetRoomSeq() != 2 {
		t.Fatalf("relay warm-up event = %v, want room_seq 2", ev)
	}

	// Failover: A hands off and dies; B takes over with a post-failover commit (same shared log).
	//
	// Draining before releasing is the real fix for the re-acquire race this test used to dodge by
	// timing alone: Release hands the room back, but A's relay Tail is still running and its
	// claim-on-serve would re-take the lease, leaving A the owner so B could never win. A draining
	// node refuses to re-acquire, which is exactly the ordering preStop uses.
	a.SetDraining(true)
	if err := a.Release(bg, "room"); err != nil {
		t.Fatalf("A Release: %v", err)
	}
	stopA()
	if _, applied, err := b.Commit(bg, "room", "x", 3, kvBody("k", "3")); err != nil || !applied {
		t.Fatalf("B failover commit: applied=%v err=%v", applied, err)
	}

	// The relay recovers on its own: FROZEN (feed dropped) → LIVE (re-subscribed to B) → the
	// post-failover event (room_seq 3), gap-free.
	sawFrozen, sawLive := false, false
	for {
		m := readFrame(ctx, t, ws)
		if st := m.GetRoomStatus(); st != nil {
			switch st.GetStatus() {
			case aetherv1.RoomStatus_STATUS_FROZEN:
				sawFrozen = true
			case aetherv1.RoomStatus_STATUS_LIVE:
				sawLive = true
			}
			continue
		}
		if ev := m.GetEvent(); ev != nil && ev.GetRoomSeq() == 3 {
			if !sawFrozen {
				t.Fatal("recovered without ever signalling FROZEN")
			}
			if !sawLive {
				t.Fatal("recovered without ever signalling LIVE")
			}
			return
		}
	}
}
