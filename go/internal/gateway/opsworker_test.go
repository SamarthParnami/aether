package gateway_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/gateway"
)

// blockingOwner is a RoomService whose GetSnapshot blocks until released — used to hold a Join in
// flight on the ops worker so the test can prove the read loop stays responsive meanwhile.
type blockingOwner struct {
	aetherv1connect.UnimplementedRoomServiceHandler
	release chan struct{}
}

func (b *blockingOwner) GetSnapshot(
	ctx context.Context, _ *connect.Request[aetherv1.GetSnapshotRequest],
) (*connect.Response[aetherv1.GetSnapshotResponse], error) {
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return connect.NewResponse(&aetherv1.GetSnapshotResponse{}), nil
}

// A slow owner op (a Join blocked in GetSnapshot) runs on the ops worker, so the read loop keeps
// draining the socket and answers Ping promptly — the decoupling the ops worker exists for. Before
// it (synchronous dispatch on the read loop), the Pong would not arrive until GetSnapshot returned,
// and this read would time out.
func TestReadLoopStaysResponsiveDuringSlowOp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	release := make(chan struct{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(aetherv1connect.NewRoomServiceHandler(&blockingOwner{release: release}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	co := coord.NewMemory()
	co.Claim("room", "slow", ln.Addr().String(), time.Now(), time.Minute)

	gw := httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(co),
	))
	defer gw.Close()

	ws := dial(ctx, t, gw, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	// Join blocks the worker in GetSnapshot...
	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: "room", SessionNonce: "n",
	}}})
	// ...but a Ping must still be answered promptly, off the read loop's critical path.
	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Ping{Ping: &aetherv1.Ping{Id: "live"}}})
	if got := readFrame(ctx, t, ws).GetPong().GetId(); got != "live" {
		t.Fatalf("pong id = %q, want live (read loop blocked by a slow worker op)", got)
	}

	close(release) // let the Join finish so teardown is clean
}
