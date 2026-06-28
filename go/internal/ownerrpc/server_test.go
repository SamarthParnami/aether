package ownerrpc_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/fanout"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/ownerrpc"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

func kvBody(key, val string) *aetherv1.EventBody {
	return &aetherv1.EventBody{
		Kind: &aetherv1.EventBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

func ephBody(key, val string) *aetherv1.EphemeralBody {
	return &aetherv1.EphemeralBody{
		Kind: &aetherv1.EphemeralBody_KvSet{KvSet: &aetherv1.KeyValueSet{Key: key, Value: []byte(val)}},
	}
}

// newClient mounts the owner RPC server for rt over an in-process HTTP server and returns a
// Connect client for it.
func newClient(t *testing.T, rt *roomruntime.Runtime) aetherv1connect.RoomServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(aetherv1connect.NewRoomServiceHandler(ownerrpc.NewServer(rt)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return aetherv1connect.NewRoomServiceClient(srv.Client(), srv.URL)
}

func commitReq(room, client string, seq uint64, key, val string) *connect.Request[aetherv1.CommitRequest] {
	return connect.NewRequest(&aetherv1.CommitRequest{
		RoomId: room, ClientId: client, ClientSeq: seq, Body: kvBody(key, val),
	})
}

// A new commit returns the committed Event; a replay of the same (client, seq) returns the
// DuplicateAck outcome — the three-way mapping of Runtime.Commit.
func TestCommitAppliedThenDuplicate(t *testing.T) {
	ctx := context.Background()
	client := newClient(t, roomruntime.New(logstore.NewMemory(), fanout.NewMemory()))

	resp, err := client.Commit(ctx, commitReq("room", "A", 1, "slide", "7"))
	if err != nil {
		t.Fatal(err)
	}
	if ev := resp.Msg.GetCommitted(); ev == nil || ev.GetRoomSeq() != 1 || ev.GetOriginClientSeq() != 1 {
		t.Fatalf("commit outcome = %v, want committed room_seq 1", resp.Msg)
	}

	dup, err := client.Commit(ctx, commitReq("room", "A", 1, "slide", "MUTATED"))
	if err != nil {
		t.Fatal(err)
	}
	if dup.Msg.GetDuplicate() == nil {
		t.Fatalf("replay outcome = %v, want duplicate", dup.Msg)
	}
}

// A node that doesn't own the room answers FAILED_PRECONDITION so the gateway re-resolves.
func TestCommitNotOwnerIsFailedPrecondition(t *testing.T) {
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
	_, err := client.Commit(ctx, commitReq("room", "x", 1, "k", "v2"))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("commit code = %v (err=%v), want FailedPrecondition", got, err)
	}
}

// GetSnapshot returns the materialized state at the current head.
func TestGetSnapshot(t *testing.T) {
	ctx := context.Background()
	client := newClient(t, roomruntime.New(logstore.NewMemory(), fanout.NewMemory()))

	if _, err := client.Commit(ctx, commitReq("room", "A", 1, "slide", "7")); err != nil {
		t.Fatal(err)
	}
	resp, err := client.GetSnapshot(ctx, connect.NewRequest(&aetherv1.GetSnapshotRequest{RoomId: "room"}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetRoomSeq() != 1 {
		t.Fatalf("room_seq = %d, want 1", resp.Msg.GetRoomSeq())
	}
	if got := string(resp.Msg.GetState().GetEntries()["slide"]); got != "7" {
		t.Fatalf("state slide = %q, want 7", got)
	}
}

// Subscribe streams catch-up then live events, in room_seq order, wrapped in SubscribeResponse.
func TestSubscribeStreamsCatchUpThenLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	client := newClient(t, rt)

	if _, applied, err := rt.Commit(ctx, "room", "A", 1, kvBody("k", "1")); err != nil || !applied {
		t.Fatalf("seed commit: applied=%v err=%v", applied, err)
	}

	stream, err := client.Subscribe(ctx, connect.NewRequest(&aetherv1.SubscribeRequest{RoomId: "room", FromSeq: 0}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	if !stream.Receive() {
		t.Fatalf("receive catch-up: %v", stream.Err())
	}
	if seq := stream.Msg().GetEvent().GetRoomSeq(); seq != 1 {
		t.Fatalf("catch-up room_seq = %d, want 1", seq)
	}

	if _, applied, err := rt.Commit(ctx, "room", "A", 2, kvBody("k", "2")); err != nil || !applied {
		t.Fatalf("live commit: applied=%v err=%v", applied, err)
	}
	if !stream.Receive() {
		t.Fatalf("receive live: %v", stream.Err())
	}
	if seq := stream.Msg().GetEvent().GetRoomSeq(); seq != 2 {
		t.Fatalf("live room_seq = %d, want 2", seq)
	}
}

// The ephemeral tier isn't wired yet: Broadcast is explicitly Unimplemented.
// Broadcast publishes an ephemeral that a SubscribeEphemeral stream then receives — the ephemeral
// tier end to end over RPC. It is live-only (no replay), so the test re-broadcasts until the
// server-side subscription is in place and one delivery lands.
func TestBroadcastDeliveredOverSubscribeEphemeral(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := roomruntime.New(logstore.NewMemory(), fanout.NewMemory())
	client := newClient(t, rt)

	// Ephemerals are live-only AND a Connect server-stream flushes its response headers only on the
	// handler's first Send — which here happens only when a broadcast is delivered. So the
	// broadcaster must run CONCURRENTLY with the subscribe (started first): it keeps broadcasting
	// until one lands after the server-side subscription is in place, unblocking SubscribeEphemeral.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_, _ = client.Broadcast(ctx, connect.NewRequest(&aetherv1.BroadcastRequest{
					RoomId: "room", OriginClientId: "c1", Body: ephBody("cursor", "10,20"),
				}))
			}
		}
	}()

	stream, err := client.SubscribeEphemeral(ctx,
		connect.NewRequest(&aetherv1.SubscribeEphemeralRequest{RoomId: "room"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	if !stream.Receive() {
		t.Fatalf("receive ephemeral: %v", stream.Err())
	}
	eph := stream.Msg().GetEphemeral()
	if eph.GetRoomId() != "room" || eph.GetOriginClientId() != "c1" {
		t.Fatalf("ephemeral = %v, want room/c1", eph)
	}
	if v := string(eph.GetBody().GetKvSet().GetValue()); v != "10,20" {
		t.Fatalf("body value = %q, want 10,20", v)
	}
}

// A node that doesn't own the room answers Broadcast with FAILED_PRECONDITION so the gateway
// re-resolves to the real owner (on whose ephemeral bus the subscribers are).
func TestBroadcastNotOwnerIsFailedPrecondition(t *testing.T) {
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
	_, err := client.Broadcast(ctx, connect.NewRequest(&aetherv1.BroadcastRequest{
		RoomId: "room", OriginClientId: "x", Body: ephBody("k", "v"),
	}))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("broadcast code = %v (err=%v), want FailedPrecondition", got, err)
	}
}
