package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/ownerrpc"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// dstNet is the in-memory network for a DST cluster: each owner's ownerrpc handler is registered
// under an addr and served over net.Pipe (memListener), and the gateway locator dials owners through
// an http.Client that routes each dial to the addressed owner. Every conn is net.Pipe → durably
// blocking, so the whole cluster runs in one synctest bubble. (Message-level fault injection layers
// onto the dials in G11d.)
type dstNet struct {
	mu        sync.Mutex
	listeners map[string]*memListener
}

func newDSTNet() *dstNet { return &dstNet{listeners: map[string]*memListener{}} }

// serve runs rt's ownerrpc handler reachable at addr, returning a stop func that kills that owner.
func (n *dstNet) serve(addr string, rt *roomruntime.Runtime) func() {
	ln := newMemListener()
	mux := http.NewServeMux()
	mux.Handle(aetherv1connect.NewRoomServiceHandler(ownerrpc.NewServer(rt)))
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	n.mu.Lock()
	n.listeners[addr] = ln
	n.mu.Unlock()
	return func() { _ = srv.Close(); _ = ln.Close() }
}

// httpClient returns the transport the gateway locator dials owners through; DialContext routes the
// addr (host of "http://<addr>") to that owner's listener.
func (n *dstNet) httpClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, hostport string) (net.Conn, error) {
			host := hostport
			if h, _, err := net.SplitHostPort(hostport); err == nil {
				host = h
			}
			n.mu.Lock()
			ln := n.listeners[host]
			n.mu.Unlock()
			if ln == nil {
				return nil, fmt.Errorf("dstNet: no owner at %q", host)
			}
			return ln.dial(ctx)
		},
	}}
}

// dstCluster wires a DST scenario in one synctest bubble: a shared coordinator + shared durable log,
// M owner runtimes over the in-mem network, and N gateway Servers (each with a seeded rng). Owners
// SHARE the log so a survivor re-homes a room by replaying it; each keeps its own fan-out (cross-node
// event delivery goes through the shared log via Tail's poll, not a shared bus).
type dstCluster struct {
	co     coord.Coordinator
	log    logstore.LogStore
	net    *dstNet
	owners []*roomruntime.Runtime
	stops  []func()
}

func newDSTCluster(owners, gateways int) (*dstCluster, []*Server) {
	c := &dstCluster{co: coord.NewMemory(), log: logstore.NewMemory(), net: newDSTNet()}
	for i := 0; i < owners; i++ {
		addr := fmt.Sprintf("owner-%d", i)
		rt := roomruntime.New(c.log, fanout.NewMemory(),
			roomruntime.WithNodeID(addr), roomruntime.WithAddr(addr), roomruntime.WithCoordinator(c.co))
		c.owners = append(c.owners, rt)
		c.stops = append(c.stops, c.net.serve(addr, rt))
	}
	gws := make([]*Server, gateways)
	for i := range gws {
		gws[i] = NewServer(DevAuthenticator{Header: "X-Aether-Principal"},
			NewOwnerLocator(c.co, WithLocatorHTTPClient(c.net.httpClient())),
			WithRand(newLockedRand(uint64(i+1))))
	}
	return c, gws
}

func (c *dstCluster) stopAll() {
	for _, s := range c.stops {
		s()
	}
}

// killOwner stops owner i's server + listener, so its open streams break and new dials to it fail —
// the connection-level "owner death" fault, injected at the byte-stream seam (per the #40 review,
// per-message drop/dup/reorder have no analogue on a single ordered stream and belong on a Connect
// interceptor instead). Idempotent, so it composes with stopAll.
func (c *dstCluster) killOwner(i int) { c.stops[i]() }

// dial connects a client to gateway g over a frame pipe, runs the conn, and returns the client end
// plus a stop func that tears it down and waits for the conn's goroutines to exit.
func (c *dstCluster) dial(ctx context.Context, g *Server, principalID string) (*framePipe, func()) {
	client, server := newFramePipe()
	connCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		newConn(g, Principal{ID: principalID}, server).run(connCtx)
	}()
	return client, func() { cancel(); <-done }
}

func joinRoom(ctx context.Context, t *testing.T, c frameConn, room, nonce string) {
	t.Helper()
	writePipe(ctx, t, c, &aetherv1.ClientMessage{Body: &aetherv1.ClientMessage_Join{Join: &aetherv1.Join{
		RoomId: room, SessionNonce: nonce,
	}}})
}

// The cluster harness end to end: one room owned by owner-0, two clients on TWO DIFFERENT gateways.
// Both resolve the owner independently through their gateways' locators over the in-mem network,
// both join, and both converge on the owner's commit — the multi-gateway/multi-owner wiring the
// chaos suite (G11d) will stress. synctest.Wait confirms the whole cluster parks.
func TestSynctestClusterMultiGatewayConverges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cl, gws := newDSTCluster(2, 2) // 2 owners, 2 gateways
		defer cl.stopAll()
		bg := context.Background()

		// owner-0 claims "room" (publishes its addr into coord) by serving a seed commit.
		if _, applied, err := cl.owners[0].Commit(bg, "room", "seed", 1, eventBody("k", "1")); err != nil || !applied {
			t.Fatalf("seed commit: applied=%v err=%v", applied, err)
		}

		ctx, cancel := context.WithCancel(bg)
		defer cancel()
		a, stopA := cl.dial(ctx, gws[0], "A")
		defer stopA()
		b, stopB := cl.dial(ctx, gws[1], "B")
		defer stopB()

		joinRoom(ctx, t, a, "room", "na")
		if seq := readPipe(ctx, t, a).GetJoined().GetCurrentSeq(); seq != 1 {
			t.Fatalf("A joined current_seq = %d, want 1", seq)
		}
		joinRoom(ctx, t, b, "room", "nb")
		if seq := readPipe(ctx, t, b).GetJoined().GetCurrentSeq(); seq != 1 {
			t.Fatalf("B joined current_seq = %d, want 1", seq)
		}

		// A commit at owner-0 must reach BOTH clients, each via its own gateway + relay.
		if _, applied, err := cl.owners[0].Commit(bg, "room", "seed", 2, eventBody("k", "2")); err != nil || !applied {
			t.Fatalf("commit: applied=%v err=%v", applied, err)
		}
		if ev := readPipe(ctx, t, a).GetEvent(); ev == nil || ev.GetRoomSeq() != 2 {
			t.Fatalf("A relayed event = %v, want room_seq 2", ev)
		}
		if ev := readPipe(ctx, t, b).GetEvent(); ev == nil || ev.GetRoomSeq() != 2 {
			t.Fatalf("B relayed event = %v, want room_seq 2", ev)
		}

		synctest.Wait()
	})
}
