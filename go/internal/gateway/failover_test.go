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

	// Failover: A hands off and dies; B takes over with a post-failover commit (same shared log).
	a.Release("room")
	stopA()
	if _, applied, err := b.Commit(bg, "room", "x", 2, kvBody("k", "2")); err != nil || !applied {
		t.Fatalf("B failover commit: applied=%v err=%v", applied, err)
	}

	// The relay recovers on its own: FROZEN (feed dropped) → LIVE (re-subscribed to B) → the
	// post-failover event (room_seq 2), gap-free.
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
		if ev := m.GetEvent(); ev != nil && ev.GetRoomSeq() == 2 {
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
