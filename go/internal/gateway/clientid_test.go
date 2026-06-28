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

// Field boundaries are unconditionally unambiguous: two (principal, nonce) pairs that share the
// same byte concatenation but split differently must still yield different ids — otherwise a
// crafted nonce could collide with another principal's id. (Length-prefixing the principal is what
// guarantees this even when principalID contains the NUL byte a single delimiter would rely on.)
func TestDeriveClientIDFieldsArePrefixFree(t *testing.T) {
	secret := []byte("cluster-secret")
	// "a"‖"\x00bc" and "a\x00"‖"bc" are both the bytes a,0,b,c — they must not collide.
	if deriveClientID(secret, "a", "\x00bc") == deriveClientID(secret, "a\x00", "bc") {
		t.Fatal("ambiguous (principal, nonce) encoding — ids collide across field boundaries")
	}
}
