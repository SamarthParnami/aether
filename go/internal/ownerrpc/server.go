// Package ownerrpc serves the RoomService RPC (gateway -> owner) over Connect, wrapping a
// roomruntime.Runtime. It is the owner side of 05-design-gateway.md's gateway↔owner RPC: a gateway
// resolves a room's owner via the coord directory and calls this server.
//
// Error mapping, in two kinds — never a silent wrong answer:
//
//   - FAILED_PRECONDITION: this node does not (or no longer) own the room. The gateway re-resolves
//     the directory and retries against the real owner.
//   - RESOURCE_EXHAUSTED: this node COULD own the room and is refusing it (draining, or at its room
//     cap). Re-resolving would be useless — the directory has nothing new to say — so the gateway
//     must try a different node instead. Kept distinct precisely because the recovery differs.
//   - UNAVAILABLE: the coordinator did not answer, so ownership is unknown. Transient and
//     retryable, as against INTERNAL, which asserts a bug.
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

// routingError maps the errors that tell the gateway WHERE TO GO NEXT onto their Connect codes,
// returning nil for anything else (including nil). One function rather than the same switch in five
// handlers: the codes are a wire contract, and five copies is five chances for one to drift.
func routingError(err error) *connect.Error {
	switch {
	case errors.Is(err, roomruntime.ErrNotOwner):
		// Wrong node — the directory knows the right one.
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, roomruntime.ErrDraining), errors.Is(err, roomruntime.ErrAtCapacity):
		// Right node, refused. The directory would just send the caller back here.
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, roomruntime.ErrCoordUnavailable):
		// The store did not answer, so ownership is unknown. UNAVAILABLE (transient, retryable),
		// never INTERNAL — clients and gRPC retry policy treat those differently, and INTERNAL
		// asserts "this is a bug" about what is usually a brownout that clears itself.
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return nil
}

// Commit maps RoomService.Commit onto Runtime.Commit's three outcomes: committed, duplicate, or a
// not-owner failure the gateway re-resolves on.
func (s *Server) Commit(
	ctx context.Context, req *connect.Request[aetherv1.CommitRequest],
) (*connect.Response[aetherv1.CommitResponse], error) {
	m := req.Msg
	ev, applied, err := s.rt.Commit(ctx, m.GetRoomId(), m.GetClientId(), m.GetClientSeq(), m.GetBody())
	if ce := routingError(err); ce != nil {
		return nil, ce
	}
	switch {
	case errors.Is(err, logstore.ErrConflict):
		// Lost the conditional append — same signal as not-owner: re-resolve and retry.
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
	if ce := routingError(err); ce != nil {
		return nil, ce
	}
	if err != nil {
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
	if ce := routingError(err); ce != nil {
		return ce
	}
	// TODO(compaction): when Tail gains a log-floor check, map its "from_seq too old" sentinel to
	// connect.CodeOutOfRange here, so the gateway's deep-resume fallback (GetSnapshot + Subscribe
	// from the snapshot seq) triggers — the G1 contract already declares OUT_OF_RANGE for it.
	return err // a real mid-stream Send/read failure (or nil)
}

// Broadcast publishes an ephemeral to the room's subscribers (the best-effort tier). It returns an
// empty response on success; a not-owner failure is FAILED_PRECONDITION so the gateway re-resolves.
func (s *Server) Broadcast(
	ctx context.Context, req *connect.Request[aetherv1.BroadcastRequest],
) (*connect.Response[aetherv1.BroadcastResponse], error) {
	m := req.Msg
	err := s.rt.Broadcast(ctx, m.GetRoomId(), m.GetOriginClientId(), m.GetBody())
	if ce := routingError(err); ce != nil {
		return nil, ce
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&aetherv1.BroadcastResponse{}), nil
}

// SubscribeEphemeral streams a room's live ephemerals by piping Runtime.TailEphemeral to the
// stream. It mirrors Subscribe's disconnect handling: a cancelled context is a clean end (not a
// failed stream), and a not-owner is FAILED_PRECONDITION so the gateway re-resolves. There is no
// OUT_OF_RANGE path — the ephemeral tier has no history to fall off.
func (s *Server) SubscribeEphemeral(
	ctx context.Context, req *connect.Request[aetherv1.SubscribeEphemeralRequest],
	stream *connect.ServerStream[aetherv1.SubscribeEphemeralResponse],
) error {
	err := s.rt.TailEphemeral(ctx, req.Msg.GetRoomId(), func(e *aetherv1.Ephemeral) error {
		return stream.Send(&aetherv1.SubscribeEphemeralResponse{Ephemeral: e})
	})

	if ctx.Err() != nil {
		return nil
	}
	if ce := routingError(err); ce != nil {
		return ce
	}
	return err
}

var _ aetherv1connect.RoomServiceHandler = (*Server)(nil)
