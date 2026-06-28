package roomruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

func ephBody(key, val string) *aetherv1.EphemeralBody {
	return &aetherv1.EphemeralBody{
		Kind: &aetherv1.EphemeralBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

// Broadcast delivers a live ephemeral to a TailEphemeral subscriber. The tier is live-only (no
// replay/catch-up), so the test re-broadcasts until the subscription is in place and one lands.
func TestBroadcastDeliversToTailEphemeral(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())

	got := make(chan *aetherv1.Ephemeral, 16)
	go func() {
		_ = rt.TailEphemeral(ctx, "room", func(e *aetherv1.Ephemeral) error {
			got <- e
			return nil
		})
	}()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case e := <-got:
			if e.GetRoomId() != "room" || e.GetOriginClientId() != "c1" {
				t.Fatalf("ephemeral = %v, want room/c1", e)
			}
			if v := string(e.GetBody().GetKvSet().GetValue()); v != "10,20" {
				t.Fatalf("body value = %q, want 10,20", v)
			}
			return
		case <-tick.C:
			_ = rt.Broadcast(ctx, "room", "c1", ephBody("cursor", "10,20"))
		case <-deadline:
			t.Fatal("timed out waiting for a broadcast ephemeral")
		}
	}
}

// TailEphemeral returns when its context is cancelled — a clean stream end, not a failure.
func TestTailEphemeralStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())

	done := make(chan error, 1)
	go func() {
		done <- rt.TailEphemeral(ctx, "room", func(*aetherv1.Ephemeral) error { return nil })
	}()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TailEphemeral returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TailEphemeral did not return after context cancel")
	}
}

// Broadcast confirms ownership: a node that doesn't own the room would fan into a bus its
// subscribers aren't on, so it returns ErrNotOwner for the gateway to re-resolve to the real owner.
func TestBroadcastNotOwnerReturnsErrNotOwner(t *testing.T) {
	ctx := context.Background()
	log := logstore.NewMemory()
	co := coord.NewMemory()
	a := roomruntime.New(log, fanout.NewMemory(),
		roomruntime.WithNodeID("A"), roomruntime.WithCoordinator(co))
	b := roomruntime.New(log, fanout.NewMemory(),
		roomruntime.WithNodeID("B"), roomruntime.WithCoordinator(co))

	if _, applied, err := a.Commit(ctx, "room", "A", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("A commit: applied=%v err=%v", applied, err)
	}

	if err := b.Broadcast(ctx, "room", "x", ephBody("k", "v")); !errors.Is(err, roomruntime.ErrNotOwner) {
		t.Fatalf("B broadcast = %v, want ErrNotOwner", err)
	}
}
