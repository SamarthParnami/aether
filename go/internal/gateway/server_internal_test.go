package gateway

import (
	"testing"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// send must disconnect (cancel) a client that isn't draining its outbound queue, rather than block
// the read loop forever — the design §9 "too slow to drain ⇒ disconnect" behavior. White-box so it
// drives a full queue deterministically, without depending on OS socket buffering.
func TestSendDisconnectsOnFullQueue(t *testing.T) {
	cancelled := false
	c := &conn{
		out:    make(chan *aetherv1.ServerMessage, 1),
		cancel: func() { cancelled = true },
	}

	c.send(&aetherv1.ServerMessage{}) // fits (cap 1)
	if cancelled {
		t.Fatal("send disconnected while the queue still had room")
	}

	c.send(&aetherv1.ServerMessage{}) // queue full ⇒ must disconnect
	if !cancelled {
		t.Fatal("send on a full queue must disconnect the conn, not block")
	}
}
