package gateway

import "testing"

// client_id derivation is stable for the same (principal, nonce) — so a reconnect keeps the id and
// dedup survives — and changes with either input, so it can't be forged across principals.
func TestDeriveClientID(t *testing.T) {
	secret := []byte("cluster-secret")

	id := deriveClientID(secret, "principal-1", "nonce-1")
	if id == "" {
		t.Fatal("empty client_id")
	}
	if id != deriveClientID(secret, "principal-1", "nonce-1") {
		t.Fatal("not stable for the same principal+nonce")
	}
	if id == deriveClientID(secret, "principal-1", "nonce-2") {
		t.Fatal("a different nonce must yield a different id (distinct sessions)")
	}
	if id == deriveClientID(secret, "principal-2", "nonce-1") {
		t.Fatal("a different principal must yield a different id (no cross-principal forgery)")
	}
	if id == deriveClientID([]byte("other-secret"), "principal-1", "nonce-1") {
		t.Fatal("a different cluster secret must yield a different id")
	}
}
