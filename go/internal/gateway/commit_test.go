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
)

// The client write path: a joined client's Commit goes to the owner and the committed Event comes
// back to that same client via its relay — fan-out is the ack (no separate success frame).
func TestCommitEchoesViaRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	co := coord.NewMemory()
	owner, _ := startOwner(t, co, "owner")
	if _, applied, err := owner.Commit(context.Background(), "room", "seed", 1, kvBody("k", "seed")); err != nil || !applied {
		t.Fatalf("seed commit: applied=%v err=%v", applied, err)
	}

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", FromSeq: 0, SessionNonce: "n",
	}}})
	readFrame(ctx, t, ws) // Joined (current_seq 1)

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Commit{Commit: &aetherv1.Commit{
		RoomId: "room", ClientSeq: 1, Body: kvBody("slide", "9"),
	}}})

	ev := readFrame(ctx, t, ws).GetEvent()
	if ev == nil || ev.GetOriginClientSeq() != 1 || ev.GetRoomSeq() != 2 {
		t.Fatalf("relayed ack = %v, want Event room_seq 2 origin_client_seq 1", ev)
	}
}

// A commit to a room the client never joined is refused with NOT_JOINED (checked before routing,
// so it doesn't even need a live owner).
func TestCommitNotJoinedNacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(coord.NewMemory()),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Commit{Commit: &aetherv1.Commit{
		RoomId: "room", ClientSeq: 1, Body: kvBody("k", "v"),
	}}})

	nack := readFrame(ctx, t, ws).GetNack()
	if nack == nil || nack.GetReason() != aetherv1.NackReason_NACK_REASON_NOT_JOINED {
		t.Fatalf("nack = %v, want NOT_JOINED", nack)
	}
}

// A commit that can't reach an owner (re-home / owner death) is Nacked for the SPECIFIC commit with
// NACK_REASON_UNAVAILABLE — a transient, client_seq-correlated signal the SDK can buffer and
// resubmit on LIVE — not an uncorrelated Error. The write-side mirror of the relay's FROZEN.
func TestCommitUnavailableNacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	co := coord.NewMemory()
	owner, stopOwner := startOwner(t, co, "owner")
	owner.Commit(context.Background(), "room", "seed", 1, kvBody("k", "1"))

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
	readFrame(ctx, t, ws) // Joined

	stopOwner() // owner gone — a commit now can't be routed

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Commit{Commit: &aetherv1.Commit{
		RoomId: "room", ClientSeq: 5, Body: kvBody("k", "2"),
	}}})

	// Read past any RoomStatus the relay emits; the commit must surface a Nack we can correlate.
	for {
		m := readFrame(ctx, t, ws)
		if nack := m.GetNack(); nack != nil {
			if nack.GetReason() != aetherv1.NackReason_NACK_REASON_UNAVAILABLE {
				t.Fatalf("nack reason = %v, want UNAVAILABLE", nack.GetReason())
			}
			if nack.GetClientSeq() != 5 {
				t.Fatalf("nack client_seq = %d, want 5 (correlated to the commit)", nack.GetClientSeq())
			}
			return
		}
	}
}
