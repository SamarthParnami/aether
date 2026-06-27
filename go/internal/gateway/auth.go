package gateway

import (
	"context"
	"errors"
	"net/http"
)

// ErrUnauthenticated is returned by an Authenticator when a handshake carries no valid identity.
var ErrUnauthenticated = errors.New("gateway: unauthenticated")

// Principal is the authenticated identity behind a connection. ID is the stable basis from which a
// client_id is derived (principal + session nonce) — see 05-design-gateway.md §5. (Derivation
// itself lands with the join/resume PRs.)
type Principal struct {
	ID string
}

// Authenticator verifies a WebSocket handshake and returns the principal behind it. Real JWT
// verification slots in behind this interface at app integration; Phase 1 uses DevAuthenticator.
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (Principal, error)
}

// DevAuthenticator is the Phase-1 stub: it trusts a principal id passed in a request header. It
// does NO signature verification and must never be used in production.
type DevAuthenticator struct {
	Header string // request header carrying the principal id (e.g. "X-Aether-Principal")
}

// Authenticate implements Authenticator.
func (d DevAuthenticator) Authenticate(_ context.Context, r *http.Request) (Principal, error) {
	id := r.Header.Get(d.Header)
	if id == "" {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{ID: id}, nil
}

var _ Authenticator = DevAuthenticator{}
