package gateway_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/coord/coordtest"
	"github.com/SamarthParnami/aether/go/internal/gateway"
)

// errDirectory stands in for a coord adapter that cannot answer — a DynamoDB throttle or timeout.
var errDirectory = errors.New("coord store unreachable")

// The distinction this change exists to make, on the routing side.
//
// ErrNoOwner is a definite answer: the directory was read and the room has no live lease. A store
// that failed to answer is not evidence of anything. Collapsing the two is what a Coordinator
// without an error return FORCES a durable adapter to do, and it is why placement cannot be built
// on the old interface: the resolve would read "unowned" and pick a node by hash, turning a coord
// brownout into a fleet-wide claim storm aimed at the store that is already failing.
func TestLocatorDirectoryErrorIsNotNoOwner(t *testing.T) {
	co := coordtest.New(coord.NewMemory())
	mustClaim(t, co, "room", "owner", "127.0.0.1:9999", time.Now(), 10*time.Second)

	loc := gateway.NewOwnerLocator(co)
	co.FailCurrent(errDirectory)

	_, _, err := loc.Owner(t.Context(), "room")
	if err == nil {
		t.Fatal("Owner succeeded while the directory was unreadable")
	}
	if errors.Is(err, gateway.ErrNoOwner) {
		t.Fatalf("unreadable directory reported as ErrNoOwner (%v) — that is a definite "+
			"'this room is unowned', asserted on no evidence", err)
	}
	if !errors.Is(err, gateway.ErrDirectoryUnavailable) {
		t.Fatalf("Owner err = %v, want ErrDirectoryUnavailable", err)
	}
	// The store's own error must survive the wrap, or an operator sees "directory unavailable"
	// with nothing to say WHY.
	if !errors.Is(err, errDirectory) {
		t.Fatalf("Owner err = %v, want it to wrap %v", err, errDirectory)
	}
}

// The locator holds no poisoned state: once the store answers again the very next resolve works.
// A brownout must not need a gateway restart to clear.
func TestLocatorRecoversWhenDirectoryRecovers(t *testing.T) {
	co := coordtest.New(coord.NewMemory())
	mustClaim(t, co, "room", "owner", "127.0.0.1:9999", time.Now(), 10*time.Second)
	loc := gateway.NewOwnerLocator(co)

	co.FailCurrent(errDirectory)
	if _, _, err := loc.Owner(t.Context(), "room"); !errors.Is(err, gateway.ErrDirectoryUnavailable) {
		t.Fatalf("Owner during brownout = %v, want ErrDirectoryUnavailable", err)
	}

	co.FailCurrent(nil)
	if _, addr, err := loc.Owner(t.Context(), "room"); err != nil || addr != "127.0.0.1:9999" {
		t.Fatalf("Owner after recovery = %q, %v; want the live owner", addr, err)
	}
}

// A definite "no live lease" still resolves to ErrNoOwner. Pinned so the change above cannot be
// satisfied by reporting everything as unavailable — which would suppress the one signal that will
// later be allowed to authorize placement.
func TestLocatorUnownedRoomIsStillNoOwner(t *testing.T) {
	co := coordtest.New(coord.NewMemory())
	loc := gateway.NewOwnerLocator(co)

	if _, _, err := loc.Owner(t.Context(), "never-owned"); !errors.Is(err, gateway.ErrNoOwner) {
		t.Fatalf("Owner of an unowned room = %v, want ErrNoOwner", err)
	}
}

// Zero behaviour change at the wire: a client joining while the directory is down sees exactly what
// it saw before this PR — one UNAVAILABLE frame, not a new error and not a hang. The distinction
// added here is visible to the gateway's own code and to nothing else yet.
func TestJoinDuringDirectoryBrownoutIsUnchangedAtTheWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	co := coordtest.New(coord.NewMemory())
	rt, _ := startOwner(t, co, "owner")
	if _, applied, err := rt.Commit(context.Background(), "room", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("owner seed commit: applied=%v err=%v", applied, err)
	}

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	co.FailCurrent(errDirectory)
	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", SessionNonce: "n",
	}}})

	got := readFrame(ctx, t, ws)
	if e := got.GetError(); e == nil || e.GetCode() != "UNAVAILABLE" {
		t.Fatalf("join during brownout = %v, want an UNAVAILABLE error frame", got)
	}
}
