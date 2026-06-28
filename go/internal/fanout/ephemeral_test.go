package fanout_test

import (
	"testing"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/fanout"
)

func ephemeral(origin string) *aetherv1.Ephemeral {
	return &aetherv1.Ephemeral{OriginClientId: origin}
}

func TestEphemeralSubscribeReceivesPublished(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	var got []string
	f.Subscribe("r", func(e *aetherv1.Ephemeral) { got = append(got, e.GetOriginClientId()) })

	f.Publish("r", ephemeral("a"))
	f.Publish("r", ephemeral("b"))

	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v, want [a b]", got)
	}
}

func TestEphemeralMultipleSubscribersAllReceiveInOrder(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	var order []string
	f.Subscribe("r", func(*aetherv1.Ephemeral) { order = append(order, "A") })
	f.Subscribe("r", func(*aetherv1.Ephemeral) { order = append(order, "B") })

	f.Publish("r", ephemeral("x"))

	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Fatalf("delivery order = %v, want [A B] (subscription order)", order)
	}
}

func TestEphemeralCancelStopsDelivery(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	count := 0
	sub := f.Subscribe("r", func(*aetherv1.Ephemeral) { count++ })

	f.Publish("r", ephemeral("x"))
	sub.Cancel()
	f.Publish("r", ephemeral("y"))

	if count != 1 {
		t.Fatalf("count = %d, want 1 (no delivery after Cancel)", count)
	}
}

// A panicking subscriber is contained: it neither aborts delivery to other subscribers nor
// unwinds into the caller (the owner's broadcast path).
func TestEphemeralPanickingSubscriberIsIsolated(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	delivered := false
	f.Subscribe("r", func(*aetherv1.Ephemeral) { panic("bad gateway handler") })
	f.Subscribe("r", func(*aetherv1.Ephemeral) { delivered = true })

	f.Publish("r", ephemeral("x")) // must not panic out of Publish

	if !delivered {
		t.Fatal("a panicking subscriber skipped a later subscriber")
	}
}

func TestEphemeralPerRoomIsolation(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	gotA, gotB := 0, 0
	f.Subscribe("a", func(*aetherv1.Ephemeral) { gotA++ })
	f.Subscribe("b", func(*aetherv1.Ephemeral) { gotB++ })

	f.Publish("a", ephemeral("x"))

	if gotA != 1 || gotB != 0 {
		t.Fatalf("room a=%d b=%d, want a=1 b=0 (isolation)", gotA, gotB)
	}
}

// The ephemeral tier is LIVE-ONLY and lossy by design: a message published while a room has no
// subscriber is dropped with no backlog, so a later subscriber gets no catch-up (unlike the
// durable event path, which replays from the log).
func TestEphemeralPublishBeforeSubscribeIsDropped(t *testing.T) {
	f := fanout.NewMemoryEphemeral()
	f.Publish("r", ephemeral("missed")) // no subscriber yet — gone

	count := 0
	f.Subscribe("r", func(*aetherv1.Ephemeral) { count++ })

	if count != 0 {
		t.Fatalf("count = %d, want 0 (no catch-up: ephemerals are live-only)", count)
	}
}
