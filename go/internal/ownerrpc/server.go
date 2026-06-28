// Package ownerrpc serves the RoomService RPC (gateway -> owner) over Connect, wrapping a
// roomruntime.Runtime. It is the owner side of 05-design-gateway.md's gateway↔owner RPC: a gateway
// resolves a room's owner via the coord directory and calls this server.
//
// Error mapping: a node that does not (or no longer) owns the room returns a Connect
// FAILED_PRECONDITION so the gateway re-resolves and retries — never a silent wrong answer.
package ownerrpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/gen/aether/v1/aetherv1connect"
	"github.com/SamarthParnami/aether/go/internal/logstore"
	"github.com/SamarthParnami/aether/go/internal/roomruntime"
)

// Server adapts a roomruntime.Runtime to the generated RoomServiceHandler. Embedding the
// Unimplemented handler keeps it forward-compatible if the service gains RPCs.
type Server struct {
	aetherv1connect.UnimplementedRoomServiceHandler
	rt *roomruntime.Runtime
}

// NewServer wraps rt as a RoomService handler.
func NewServer(rt *roomruntime.Runtime) *Server { return &Server{rt: rt} }

// Commit maps RoomService.Commit onto Runtime.Commit's three outcomes: committed, duplicate, or a
// not-owner failure the gateway re-resolves on.
func (s *Server) Commit(
	ctx context.Context, req *connect.Request[aetherv1.CommitRequest],
) (*connect.Response[aetherv1.CommitResponse], error) {
	m := req.Msg
	ev, applied, err := s.rt.Commit(ctx, m.GetRoomId(), m.GetClientId(), m.GetClientSeq(), m.GetBody())
	switch {
	case errors.Is(err, roomruntime.ErrNotOwner), errors.Is(err, logstore.ErrConflict):
		// Not (or no longer) the owner — the gateway re-resolves and retries.
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, err)
	case applied:
		return connect.NewResponse(&aetherv1.CommitResponse{
			Outcome: &aetherv1.CommitResponse_Committed{Committed: ev},
		}), nil
	default:
		// Duplicate (dedup) — exactly-once no-op. The original Event still reaches the client via
		// its Subscribe replay; DuplicateAck just completes its in-flight commit.
		return connect.NewResponse(&aetherv1.CommitResponse{
			Outcome: &aetherv1.CommitResponse_Duplicate{Duplicate: &aetherv1.DuplicateAck{}},
		}), nil
	}
}

// GetSnapshot returns the room's current materialized state (for a fresh / deep-resume join).
func (s *Server) GetSnapshot(
	ctx context.Context, req *connect.Request[aetherv1.GetSnapshotRequest],
) (*connect.Response[aetherv1.GetSnapshotResponse], error) {
	joined, err := s.rt.Join(ctx, req.Msg.GetRoomId())
	switch {
	case errors.Is(err, roomruntime.ErrNotOwner):
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	snap := joined.GetSnapshot()
	return connect.NewResponse(&aetherv1.GetSnapshotResponse{
		RoomSeq: snap.GetRoomSeq(),
		State:   snap.GetState(),
	}), nil
}

// Subscribe streams a room's events (catch-up then live) by piping Runtime.Tail to the stream.
func (s *Server) Subscribe(
	ctx context.Context, req *connect.Request[aetherv1.SubscribeRequest],
	stream *connect.ServerStream[aetherv1.SubscribeResponse],
) error {
	m := req.Msg
	err := s.rt.Tail(ctx, m.GetRoomId(), m.GetFromSeq(), func(ev *aetherv1.Event) error {
		return stream.Send(&aetherv1.SubscribeResponse{Event: ev})
	})

	// A cancelled context means the client (or the server) is gone — a clean end, regardless of
	// whether the cancel surfaced from Tail's own select or as a stream.Send failure mid-disconnect.
	// Check ctx, not the error kind, so routine watcher churn isn't recorded as failed streams.
	if ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, roomruntime.ErrNotOwner) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// TODO(compaction): when Tail gains a log-floor check, map its "from_seq too old" sentinel to
	// connect.CodeOutOfRange here, so the gateway's deep-resume fallback (GetSnapshot + Subscribe
	// from the snapshot seq) triggers — the G1 contract already declares OUT_OF_RANGE for it.
	return err // a real mid-stream Send/read failure (or nil)
}

// Broadcast (the ephemeral tier) is not wired yet — it needs an ephemeral delivery path on the
// owner and a contract addition for streaming ephemerals to gateways, which land with the
// Broadcast PR (G8). Until then it is explicitly Unimplemented.
func (s *Server) Broadcast(
	_ context.Context, _ *connect.Request[aetherv1.BroadcastRequest],
) (*connect.Response[aetherv1.BroadcastResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented,
		errors.New("ownerrpc: Broadcast (ephemeral tier) lands in a later PR"))
}

var _ aetherv1connect.RoomServiceHandler = (*Server)(nil)
