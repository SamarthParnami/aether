package ownerrpc_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/coord/coordtest"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// callEachRPC exercises every RPC that acquires a room, so a mapping added to four handlers and
// forgotten in the fifth fails here rather than in production on whichever path is rarest.
func callEachRPC(
	ctx context.Context, client aetherv1connect.RoomServiceClient, room string,
) map[string]error {
	out := map[string]error{}

	_, out["Commit"] = client.Commit(ctx, commitReq(room, "x", 1, "k", "v"))
	_, out["GetSnapshot"] = client.GetSnapshot(ctx,
		connect.NewRequest(&aetherv1.GetSnapshotRequest{RoomId: room}))
	_, out["Broadcast"] = client.Broadcast(ctx, connect.NewRequest(&aetherv1.BroadcastRequest{
		RoomId: room, OriginClientId: "x", Body: ephBody("cursor", "1"),
	}))

	// Stream errors surface on the first Receive, not at call time.
	sub, err := client.Subscribe(ctx, connect.NewRequest(&aetherv1.SubscribeRequest{RoomId: room}))
	if err != nil {
		out["Subscribe"] = err
	} else {
		sub.Receive()
		out["Subscribe"] = sub.Err()
	}

	esub, err := client.SubscribeEphemeral(ctx,
		connect.NewRequest(&aetherv1.SubscribeEphemeralRequest{RoomId: room}))
	if err != nil {
		out["SubscribeEphemeral"] = err
	} else {
		esub.Receive()
		out["SubscribeEphemeral"] = esub.Err()
	}
	return out
}

// A refusal is RESOURCE_EXHAUSTED, not FAILED_PRECONDITION. The distinction is the gateway's
// instruction: FAILED_PRECONDITION means "the directory knows a better node, re-resolve";
// RESOURCE_EXHAUSTED means "this node said no, try a DIFFERENT one". Re-resolving a draining node
// returns the same answer, so collapsing the two turns a handover into a spin.
func TestDrainingIsResourceExhaustedOnEveryRPC(t *testing.T) {
	ctx := context.Background()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	rt.SetDraining(true)
	client := newClient(t, rt)

	for name, err := range callEachRPC(ctx, client, "room") {
		if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
			t.Errorf("%s while draining = %v (err=%v), want ResourceExhausted", name, got, err)
		}
	}
}

// Same for the capacity valve — a node that is full is refusing, not disowning.
func TestAtCapacityIsResourceExhaustedOnEveryRPC(t *testing.T) {
	ctx := context.Background()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(), roomruntime.WithMaxRooms(1))
	if _, applied, err := rt.Commit(ctx, "taken", "x", 1, kvBody("k", "v")); err != nil || !applied {
		t.Fatalf("seed commit: applied=%v err=%v", applied, err)
	}
	client := newClient(t, rt)

	for name, err := range callEachRPC(ctx, client, "overflow") {
		if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
			t.Errorf("%s at capacity = %v (err=%v), want ResourceExhausted", name, got, err)
		}
	}
}

// The other side of the boundary: a genuine ownership miss is still FAILED_PRECONDITION on every
// RPC, so the change above cannot be satisfied by mapping everything to ResourceExhausted.
func TestNotOwnerIsStillFailedPreconditionOnEveryRPC(t *testing.T) {
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

	client := newClient(t, b) // B does not own the room A holds
	for name, err := range callEachRPC(ctx, client, "room") {
		if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
			t.Errorf("%s on a non-owner = %v (err=%v), want FailedPrecondition", name, got, err)
		}
	}
}

// A coordinator that did not answer is UNAVAILABLE — transient and retryable — not INTERNAL, which
// asserts a bug. #51 made that distinction available at the coord interface; without a sentinel it
// is lost one layer up and every store brownout reaches clients as an internal error, which gRPC
// and Connect retry policies treat quite differently.
func TestCoordUnavailableIsUnavailableOnEveryRPC(t *testing.T) {
	ctx := context.Background()
	co := coordtest.New(coord.NewMemory())
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory(), roomruntime.WithCoordinator(co))
	client := newClient(t, rt)

	co.FailClaim(errors.New("dynamodb: throttled"))

	for name, err := range callEachRPC(ctx, client, "room") {
		if got := connect.CodeOf(err); got != connect.CodeUnavailable {
			t.Errorf("%s during a coord brownout = %v (err=%v), want Unavailable", name, got, err)
		}
	}
}
