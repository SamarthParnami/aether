package fanout_test

import (
	"testing"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/fanout"
)

func event(seq uint64) *aetherv1.Event { return &aetherv1.Event{RoomSeq: seq} }

func TestSubscribeReceivesPublished(t *testing.T) {
	f := fanout.NewMemory()
	var got []uint64
	f.Subscribe("r", func(e *aetherv1.Event) { got = append(got, e.GetRoomSeq()) })

	f.Publish("r", event(1))
	f.Publish("r", event(2))

	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestMultipleSubscribersAllReceiveInOrder(t *testing.T) {
	f := fanout.NewMemory()
	var order []string
	f.Subscribe("r", func(*aetherv1.Event) { order = append(order, "A") })
	f.Subscribe("r", func(*aetherv1.Event) { order = append(order, "B") })

	f.Publish("r", event(1))

	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Fatalf("delivery order = %v, want [A B] (subscription order)", order)
	}
}

func TestCancelStopsDelivery(t *testing.T) {
	f := fanout.NewMemory()
	count := 0
	sub := f.Subscribe("r", func(*aetherv1.Event) { count++ })

	f.Publish("r", event(1))
	sub.Cancel()
	f.Publish("r", event(2))

	if count != 1 {
		t.Fatalf("count = %d, want 1 (no delivery after Cancel)", count)
	}
}

// A panicking subscriber is contained: it neither aborts delivery to other subscribers
// nor unwinds into the caller (the owner's commit path).
func TestPanickingSubscriberIsIsolated(t *testing.T) {
	f := fanout.NewMemory()
	delivered := false
	f.Subscribe("r", func(*aetherv1.Event) { panic("bad gateway handler") })
	f.Subscribe("r", func(*aetherv1.Event) { delivered = true })

	f.Publish("r", event(1)) // must not panic out of Publish

	if !delivered {
		t.Fatal("a panicking subscriber skipped a later subscriber")
	}
}

func TestPerRoomIsolation(t *testing.T) {
	f := fanout.NewMemory()
	gotA, gotB := 0, 0
	f.Subscribe("a", func(*aetherv1.Event) { gotA++ })
	f.Subscribe("b", func(*aetherv1.Event) { gotB++ })

	f.Publish("a", event(1))

	if gotA != 1 || gotB != 0 {
		t.Fatalf("room a=%d b=%d, want a=1 b=0 (isolation)", gotA, gotB)
	}
}
