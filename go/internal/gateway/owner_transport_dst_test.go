package gateway

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"

	"connectrpc.com/connect"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/ownerrpc"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// memListener is a net.Listener backed by net.Pipe: each dial hands a fresh in-memory conn pair to
// Accept. net.Pipe reads/writes are channel operations — durably blocking under testing/synctest —
// so a real Connect-RPC client/server pair over it can run inside a bubble (a real socket cannot).
type memListener struct {
	accept chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func newMemListener() *memListener {
	return &memListener{accept: make(chan net.Conn), done: make(chan struct{})}
}

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *memListener) Addr() net.Addr { return memAddr{} }

func (l *memListener) dial(ctx context.Context) (net.Conn, error) {
	cli, srv := net.Pipe()
	select {
	case l.accept <- srv:
		return cli, nil
	case <-l.done:
		_ = cli.Close()
		_ = srv.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = cli.Close()
		_ = srv.Close()
		return nil, ctx.Err()
	}
}

type memAddr struct{}

func (memAddr) Network() string { return "mem" }
func (memAddr) String() string  { return "mem" }

// serveInMemOwner runs the ownerrpc handler for rt over in-memory net.Pipe connections and returns
// the http.Client the gateway locator dials it through (WithLocatorHTTPClient), plus a stop func.
// This is the gateway→owner transport for DST — the real Connect stack, but over pipes instead of
// TCP so it lives in a synctest bubble. Single-owner: every dial routes to this owner regardless of
// addr (multi-owner addr routing lands with the G11c cluster harness).
func serveInMemOwner(rt *roomruntime.Runtime) (*http.Client, func()) {
	ln := newMemListener()
	mux := http.NewServeMux()
	mux.Handle(aetherv1connect.NewRoomServiceHandler(ownerrpc.NewServer(rt)))
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return ln.dial(ctx) },
	}}
	return httpClient, func() { _ = srv.Close(); _ = ln.Close() }
}

func eventBody(key, val string) *aetherv1.EventBody {
	return &aetherv1.EventBody{
		Kind: &aetherv1.EventBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

// The de-risk for G11b: a REAL Connect-RPC client and server (net/http transport + server
// goroutines included) talking over net.Pipe INSIDE a testing/synctest bubble reaches quiescence —
// both a unary call (GetSnapshot) and a server stream (Subscribe, incl. a live event) work, and
// synctest.Wait returns (every goroutine, including net/http's internal ones, is durably blocked).
// If this hangs, Connect-over-pipe is not bubble-compatible and G11b needs a different owner seam.
func TestSynctestOwnerRPCOverPipeSettles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
		if _, applied, err := rt.Commit(context.Background(), "room", "A", 1, eventBody("k", "1")); err != nil || !applied {
			t.Fatalf("seed commit: applied=%v err=%v", applied, err)
		}

		httpClient, stop := serveInMemOwner(rt)
		defer stop()
		client := aetherv1connect.NewRoomServiceClient(httpClient, "http://mem")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Unary.
		snap, err := client.GetSnapshot(ctx, connect.NewRequest(&aetherv1.GetSnapshotRequest{RoomId: "room"}))
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}
		if snap.Msg.GetRoomSeq() != 1 {
			t.Fatalf("room_seq = %d, want 1", snap.Msg.GetRoomSeq())
		}

		// Server stream: catch-up then a live event.
		stream, err := client.Subscribe(ctx, connect.NewRequest(&aetherv1.SubscribeRequest{RoomId: "room", FromSeq: 0}))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if !stream.Receive() || stream.Msg().GetEvent().GetRoomSeq() != 1 {
			t.Fatalf("catch-up receive: msg=%v err=%v", stream.Msg(), stream.Err())
		}
		if _, applied, err := rt.Commit(context.Background(), "room", "A", 2, eventBody("k", "2")); err != nil || !applied {
			t.Fatalf("live commit: applied=%v err=%v", applied, err)
		}
		if !stream.Receive() || stream.Msg().GetEvent().GetRoomSeq() != 2 {
			t.Fatalf("live receive: msg=%v err=%v", stream.Msg(), stream.Err())
		}

		// THE de-risk: with a unary call done and a stream open, does the bubble go idle?
		synctest.Wait()

		_ = stream.Close()
		cancel()
	})
}

// The core G11b de-risk: the RELAY — the 5th per-conn goroutine — brought into the synctest bubble.
// A real gateway connection (over the frame pipe) joins a room served by an in-memory owner (Connect
// over net.Pipe), its relay subscribes to that owner, and a live commit at the owner is delivered to
// the client through the relay. Then synctest.Wait returns: the relay, net/http's transport/server
// goroutines, the owner's Tail, and the conn's loops are ALL durably blocked — the whole
// client↔gateway↔owner path is bubble-drivable, which is what G11c's chaos suite needs.
func TestSynctestRelayDeliversInBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bg := context.Background()
		co := coord.NewMemory()
		rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(),
			roomruntime.WithNodeID("owner"), roomruntime.WithAddr("owner"), roomruntime.WithCoordinator(co))
		httpClient, stop := serveInMemOwner(rt)
		defer stop()

		// Seed a commit so the owner claims "owner" (publishes its addr) before the gateway resolves it.
		if _, applied, err := rt.Commit(bg, "room", "A", 1, eventBody("k", "1")); err != nil || !applied {
			t.Fatalf("seed commit: applied=%v err=%v", applied, err)
		}

		srv := NewServer(DevAuthenticator{Header: "X-Aether-Principal"},
			NewOwnerLocator(co, WithLocatorHTTPClient(httpClient)))
		client, server := newFramePipe()

		ctx, cancel := context.WithCancel(bg)
		done := make(chan struct{})
		go func() {
			defer close(done)
			newConn(srv, Principal{ID: "user-1"}, server).run(ctx)
		}()

		// Join → GetSnapshot over the in-mem transport → Joined → the relay subscribes to the owner.
		writePipe(ctx, t, client, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
			RoomId: "room", SessionNonce: "n",
		}}})
		if joined := readPipe(ctx, t, client).GetJoined(); joined == nil || joined.GetCurrentSeq() != 1 {
			t.Fatalf("joined = %v, want current_seq 1", joined)
		}

		// A live commit at the owner must reach the client via the relay.
		if _, applied, err := rt.Commit(bg, "room", "A", 2, eventBody("k", "2")); err != nil || !applied {
			t.Fatalf("live commit: applied=%v err=%v", applied, err)
		}
		if ev := readPipe(ctx, t, client).GetEvent(); ev == nil || ev.GetRoomSeq() != 2 {
			t.Fatalf("relayed event = %v, want room_seq 2", ev)
		}

		synctest.Wait() // relay + net/http + owner Tail + conn loops all durably blocked

		cancel()
		<-done
	})
}
