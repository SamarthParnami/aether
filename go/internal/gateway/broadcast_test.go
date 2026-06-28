package gateway_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/gateway"
)

func ephBody(key, val string) *aetherv1.EphemeralBody {
	return &aetherv1.EphemeralBody{
		Kind: &aetherv1.EphemeralBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

// A client's Broadcast frame is forwarded to the owner and fanned to the room's ephemeral
// subscribers — including the sender's own paired ephemeral relay (set up at Join). So a broadcast
// echoes back as an Ephemeral frame carrying the sender's client_id: the full forward + relay path.
// Ephemeral delivery is live-only and the relay subscribes asynchronously after Join, so the test
// re-broadcasts until one echoes back.
func TestBroadcastEchoesAsEphemeral(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	co := coord.NewMemory()
	owner, _ := startOwner(t, co, "owner")
	// Seed a commit so the owner claims "room" (publishes its addr) before the gateway resolves it.
	owner.Commit(context.Background(), "room", "A", 1, kvBody("k", "1"))

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
	joined := readFrame(ctx, t, ws).GetJoined()
	if joined == nil {
		t.Fatal("expected a Joined frame")
	}
	clientID := joined.GetClientId()

	// Ephemeral delivery is live-only and the relay subscribes asynchronously after Join, so keep
	// broadcasting (concurrently, throttled) until one echoes back. The broadcaster uses a raw write
	// off the test goroutine (no t.Fatal there) and ignores errors during teardown; the test goroutine
	// does a blocking read on the outer ctx — never a short per-read timeout, which would close the WS.
	bcast, err := proto.Marshal(&aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Broadcast{Broadcast: &aetherv1.Broadcast{
		RoomId: "room", Body: ephBody("cursor", "10,20"),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = ws.Write(context.Background(), websocket.MessageBinary, bcast)
			}
		}
	}()

	for {
		m := readFrame(ctx, t, ws)
		if e := m.GetEphemeral(); e != nil {
			if e.GetOriginClientId() != clientID {
				t.Fatalf("ephemeral origin = %q, want %q", e.GetOriginClientId(), clientID)
			}
			if v := string(e.GetBody().GetKvSet().GetValue()); v != "10,20" {
				t.Fatalf("ephemeral body = %q, want 10,20", v)
			}
			return
		}
		// any other frame (e.g. a stray Event) — keep reading
	}
}

// A Broadcast to a room the client hasn't joined is a usage error, answered with an Error frame
// (the ephemeral tier has no Nack), rather than silently forwarded under an unset identity.
func TestBroadcastNotJoinedErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(coord.NewMemory()),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Broadcast{Broadcast: &aetherv1.Broadcast{
		RoomId: "room", Body: ephBody("cursor", "1,1"),
	}}})
	if code := readFrame(ctx, t, ws).GetError().GetCode(); code != "INVALID" {
		t.Fatalf("error code = %q, want INVALID", code)
	}
}
