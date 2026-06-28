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

// The first full client↔gateway↔owner path: a WS client Joins, the gateway derives the client's id,
// resolves the room's owner via the locator, fetches the snapshot over RPC, and replies Joined.
func TestJoinReturnsIdentityAndSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	co := coord.NewMemory()
	owner := startOwner(t, co, "owner") // owner node serving RoomService (from locator_test.go)
	if _, applied, err := owner.Commit(context.Background(), "room", "A", 1, kvBody("slide", "7")); err != nil || !applied {
		t.Fatalf("owner seed commit: applied=%v err=%v", applied, err)
	}

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", FromSeq: 0, SessionNonce: "nonce-1",
	}}})

	joined := readFrame(ctx, t, ws).GetJoined()
	if joined == nil {
		t.Fatal("expected a Joined frame")
	}
	if joined.GetRoomId() != "room" {
		t.Fatalf("room_id = %q, want room", joined.GetRoomId())
	}
	if joined.GetClientId() == "" {
		t.Fatal("Joined.client_id is empty")
	}
	if joined.GetCurrentSeq() != 1 {
		t.Fatalf("current_seq = %d, want 1", joined.GetCurrentSeq())
	}
	if got := string(joined.GetSnapshot().GetState().GetEntries()["slide"]); got != "7" {
		t.Fatalf("snapshot slide = %q, want 7", got)
	}
}

// A Join without a session_nonce is rejected — an empty nonce would collapse a principal's sessions
// onto one client_id and silently drop their commits, so the requirement is enforced server-side.
func TestJoinEmptyNonceRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(coord.NewMemory()),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", // no SessionNonce
	}}})

	if code := readFrame(ctx, t, ws).GetError().GetCode(); code != "INVALID" {
		t.Fatalf("error code = %q, want INVALID", code)
	}
}

// A Join to a room with no reachable owner yields an error frame (FROZEN/retry lands with routing).
func TestJoinNoOwnerErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(coord.NewMemory()), // empty directory — no owners
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "nope", SessionNonce: "n",
	}}})

	if code := readFrame(ctx, t, ws).GetError().GetCode(); code != "UNAVAILABLE" {
		t.Fatalf("error code = %q, want UNAVAILABLE", code)
	}
}
