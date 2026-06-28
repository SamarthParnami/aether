package gateway_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/gateway"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/ownerrpc"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

func kvBody(key, val string) *aetherv1.EventBody {
	return &aetherv1.EventBody{
		Kind: &aetherv1.EventBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

// startOwner brings up an owner node — a roomruntime.Runtime serving the RoomService RPC on a real
// loopback listener — and returns its runtime and dialable addr. Binding the listener first lets the
// runtime publish its own addr (WithAddr) into the shared coordinator on claim.
func startOwner(t *testing.T, co coord.Coordinator, nodeID string) *roomruntime.Runtime {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
		roomruntime.WithNodeID(nodeID),
		roomruntime.WithAddr(addr),
		roomruntime.WithCoordinator(co),
	)
	mux := http.NewServeMux()
	mux.Handle(aetherv1connect.NewRoomServiceHandler(ownerrpc.NewServer(rt)))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return rt
}

// The locator resolves a room to its owner via the coord directory and dials it — an end-to-end
// gateway-client → owner-server round-trip over real HTTP.
func TestLocatorResolvesAndDialsOwner(t *testing.T) {
	ctx := context.Background()
	co := coord.NewMemory()
	rt := startOwner(t, co, "owner")

	// The owner claims (publishing its addr into the directory) by serving a commit.
	if _, applied, err := rt.Commit(ctx, "room", "A", 1, kvBody("slide", "7")); err != nil || !applied {
		t.Fatalf("owner commit: applied=%v err=%v", applied, err)
	}

	loc := gateway.NewOwnerLocator(co)
	client, err := loc.Owner("room")
	if err != nil {
		t.Fatalf("Owner(room): %v", err)
	}

	resp, err := client.GetSnapshot(ctx, connect.NewRequest(&aetherv1.GetSnapshotRequest{RoomId: "room"}))
	if err != nil {
		t.Fatalf("GetSnapshot via dialed owner: %v", err)
	}
	if resp.Msg.GetRoomSeq() != 1 {
		t.Fatalf("room_seq = %d, want 1", resp.Msg.GetRoomSeq())
	}
	if got := string(resp.Msg.GetState().GetEntries()["slide"]); got != "7" {
		t.Fatalf("state slide = %q, want 7", got)
	}
}

// A room with no live lease has no owner to dial.
func TestLocatorNoOwner(t *testing.T) {
	loc := gateway.NewOwnerLocator(coord.NewMemory())
	if _, err := loc.Owner("nope"); !errors.Is(err, gateway.ErrNoOwner) {
		t.Fatalf("Owner of an unowned room = %v, want ErrNoOwner", err)
	}
}

// A lease with an empty Addr is a non-routable owner — treated as no owner so the gateway
// re-resolves rather than dialing a black hole.
func TestLocatorEmptyAddrIsNoOwner(t *testing.T) {
	co := coord.NewMemory()
	co.Claim("room", "owner-without-addr", "", time.Now(), 10*time.Second) // empty addr

	loc := gateway.NewOwnerLocator(co)
	if _, err := loc.Owner("room"); !errors.Is(err, gateway.ErrNoOwner) {
		t.Fatalf("Owner with empty addr = %v, want ErrNoOwner", err)
	}
}

// A lapsed lease has no current owner — the locator honors expiry via its clock.
func TestLocatorExpiredLeaseIsNoOwner(t *testing.T) {
	co := coord.NewMemory()
	t0 := time.Unix(1000, 0)
	co.Claim("room", "owner", "127.0.0.1:9999", t0, 10*time.Second) // expires at t0+10s

	loc := gateway.NewOwnerLocator(co, gateway.WithLocatorClock(func() time.Time {
		return t0.Add(11 * time.Second) // past expiry
	}))
	if _, err := loc.Owner("room"); !errors.Is(err, gateway.ErrNoOwner) {
		t.Fatalf("Owner of a lapsed lease = %v, want ErrNoOwner", err)
	}
}

// The locator pools one client per owner address rather than dialing afresh each call.
func TestLocatorPoolsClientPerOwner(t *testing.T) {
	co := coord.NewMemory()
	co.Claim("room", "owner", "127.0.0.1:9999", time.Now(), 10*time.Second)

	loc := gateway.NewOwnerLocator(co)
	a, err := loc.Owner("room")
	if err != nil {
		t.Fatal(err)
	}
	b, err := loc.Owner("room")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("locator dialed a fresh client instead of pooling the owner's")
	}
}
