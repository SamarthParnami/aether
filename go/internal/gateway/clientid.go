package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// deriveClientID computes a stable, unforgeable client_id from the authenticated principal and the
// client-supplied session nonce (05-design-gateway.md §5).
//
// It is HMAC(secret, principalID || 0x00 || nonce): any gateway holding the same cluster secret
// re-derives the same id with zero shared state (so a reconnect to a different gateway keeps the
// id, and dedup survives), while binding the principal means a client can only ever mint ids
// within its own authenticated identity — it cannot forge another client's id to poison that
// client's (client_id, client_seq) dedup space. The nonce separates concurrent sessions of one
// principal (e.g. multiple tabs). The 0x00 separator keeps (principal, nonce) unambiguous.
func deriveClientID(secret []byte, principalID, nonce string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(principalID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}
