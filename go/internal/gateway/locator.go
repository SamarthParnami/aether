package gateway

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
)

// ErrNoOwner is returned by OwnerLocator.Owner when a room has no reachable owner: either no live
// lease, or a lease whose Addr is empty (a deliberately non-routable owner — see
// roomruntime.WithAddr). The gateway treats it as "re-resolve / back off and retry", not a hard
// failure — turning a momentary gap or a node that forgot WithAddr into a fast re-resolve rather
// than a silent black hole.
var ErrNoOwner = errors.New("gateway: no reachable owner for room")

// OwnerLocator resolves a room to its current owner's RoomService client via the coord directory,
// pooling one Connect client per owner address. Owner addresses are dialed as host:port over HTTP.
type OwnerLocator struct {
	coord      coord.Coordinator
	now        func() time.Time
	httpClient connect.HTTPClient

	mu      sync.Mutex
	clients map[string]aetherv1connect.RoomServiceClient // owner addr -> pooled client
}

// LocatorOption configures an OwnerLocator.
type LocatorOption func(*OwnerLocator)

// WithLocatorHTTPClient injects the HTTP client used to dial owners (shared transport for pooling).
// Defaults to a zero-timeout *http.Client — a per-request timeout would kill long-lived Subscribe
// streams; unary calls bound themselves with the request context instead.
//
// The default transport speaks HTTP/1.1, so each concurrent Subscribe stream holds its own TCP
// connection gateway→owner, and http.DefaultTransport's MaxIdleConnsPerHost=2 then starves unary
// Commit/GetSnapshot reuse to the same owner. That does NOT scale to the watcher counts this design
// targets — h2c multiplexing (thousands of streams over a handful of connections) is load-bearing
// for the streaming tier, not cosmetic. G6/G11 inject an h2c client through this seam once real
// stream load lands: a golang.org/x/net/http2.Transport{AllowHTTP: true, DialTLSContext: <plaintext
// dial>}.
func WithLocatorHTTPClient(h connect.HTTPClient) LocatorOption {
	return func(l *OwnerLocator) { l.httpClient = h }
}

// WithLocatorClock injects the clock used to evaluate lease expiry. Defaults to time.Now.
func WithLocatorClock(now func() time.Time) LocatorOption {
	return func(l *OwnerLocator) { l.now = now }
}

// NewOwnerLocator returns a locator over the given coordinator.
func NewOwnerLocator(co coord.Coordinator, opts ...LocatorOption) *OwnerLocator {
	l := &OwnerLocator{
		coord:      co,
		now:        time.Now,
		httpClient: &http.Client{},
		clients:    map[string]aetherv1connect.RoomServiceClient{},
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Owner returns the RoomService client for roomID's current owner and the owner's address, or
// ErrNoOwner when there is no reachable owner — no live lease, or a non-routable (empty Addr) one.
// The caller passes the returned address back to Invalidate after a dial/transport failure so the
// dead client is dropped and the next Owner re-creates against the live (possibly re-homed) owner.
func (l *OwnerLocator) Owner(roomID string) (aetherv1connect.RoomServiceClient, string, error) {
	lease, ok := l.coord.Current(roomID, l.now())
	if !ok || lease.Addr == "" {
		return nil, "", ErrNoOwner
	}
	return l.clientFor(lease.Addr), lease.Addr, nil
}

// Invalidate drops the pooled client for an owner address — the invalidation half of the pool the
// design specifies (§2: "invalidated on failover / dial errors"). The gateway calls it after a
// dial/RPC transport failure to that owner (the pod is gone, or the room re-homed), so the next
// Owner re-creates against the live address rather than reusing a dead client and leaking its idle
// connections. A no-op if the address isn't pooled.
func (l *OwnerLocator) Invalidate(addr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.clients, addr)
}

// clientFor returns a pooled RoomService client for an owner address, creating one on first use.
func (l *OwnerLocator) clientFor(addr string) aetherv1connect.RoomServiceClient {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c, ok := l.clients[addr]; ok {
		return c
	}
	// addr is a host:port; the internal gateway↔owner RPC is plain HTTP (h2c/https configurable
	// later). Connect appends the service path to this base URL.
	c := aetherv1connect.NewRoomServiceClient(l.httpClient, "http://"+addr)
	l.clients[addr] = c
	return c
}
