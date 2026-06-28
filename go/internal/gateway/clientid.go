package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// deriveClientID computes a stable, unforgeable client_id from the authenticated principal and the
// client-supplied session nonce (05-design-gateway.md §5).
//
// It is HMAC(secret, len(principalID) ‖ principalID ‖ nonce): any gateway holding the same cluster
// secret re-derives the same id with zero shared state (so a reconnect to a different gateway keeps
// the id, and dedup survives), while binding the principal means a client can only ever mint ids
// within its own authenticated identity — it cannot forge another client's id to poison that
// client's (client_id, client_seq) dedup space. The nonce separates concurrent sessions of one
// principal (e.g. multiple tabs).
//
// The principal is LENGTH-PREFIXED so the (principalID, nonce) encoding is unconditionally
// unambiguous — domain separation that holds for any bytes the auth layer may ever produce (a
// single delimiter byte would only be injective if principalID could never contain that byte).
func deriveClientID(secret []byte, principalID, nonce string) string {
	mac := hmac.New(sha256.New, secret)
	var plen [4]byte
	binary.BigEndian.PutUint32(plen[:], uint32(len(principalID)))
	_, _ = mac.Write(plen[:])
	_, _ = mac.Write([]byte(principalID))
	_, _ = mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}
