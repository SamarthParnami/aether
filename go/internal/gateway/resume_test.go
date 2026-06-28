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

// A Join with from_seq > 0 is a resume: the client keeps its state, so the gateway sends NO snapshot
// and the relay replays only the gap (events after the cursor) then live — the cheap reconnect path.
func TestResumeReplaysGapWithoutSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bg := context.Background()

	co := coord.NewMemory()
	owner, _ := startOwner(t, co, "owner")
	owner.Commit(bg, "room", "A", 1, kvBody("k", "1"))
	owner.Commit(bg, "room", "A", 2, kvBody("k", "2"))
	owner.Commit(bg, "room", "A", 3, kvBody("k", "3"))

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	// Resume from cursor 1: the client already has seq 1 and wants the gap (2, 3) then live.
	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", FromSeq: 1, SessionNonce: "n",
	}}})

	joined := readFrame(ctx, t, ws).GetJoined()
	if joined == nil {
		t.Fatal("expected a Joined frame")
	}
	if joined.GetCurrentSeq() != 1 {
		t.Fatalf("current_seq = %d, want 1 (the resume cursor)", joined.GetCurrentSeq())
	}
	if joined.GetSnapshot() != nil {
		t.Fatal("resume must not carry a snapshot — the client keeps its state")
	}

	// The gap replays in order: events 2 then 3.
	if ev := readFrame(ctx, t, ws).GetEvent(); ev == nil || ev.GetRoomSeq() != 2 {
		t.Fatalf("gap[0] = %v, want Event room_seq 2", ev)
	}
	if ev := readFrame(ctx, t, ws).GetEvent(); ev == nil || ev.GetRoomSeq() != 3 {
		t.Fatalf("gap[1] = %v, want Event room_seq 3", ev)
	}

	// A live commit (seq 4) continues to arrive after the gap.
	if _, applied, err := owner.Commit(bg, "room", "A", 4, kvBody("k", "4")); err != nil || !applied {
		t.Fatalf("live commit: applied=%v err=%v", applied, err)
	}
	if ev := readFrame(ctx, t, ws).GetEvent(); ev == nil || ev.GetRoomSeq() != 4 {
		t.Fatalf("live = %v, want Event room_seq 4", ev)
	}
}
