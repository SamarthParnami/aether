// Package logstore is the durable per-room event log abstraction.
//
// The real implementation is DynamoDB (PR-13, partition=room_id, sort=room_seq); this
// interface keeps the room-runtime decoupled from storage and lets tests run against an
// in-memory implementation. Append is conditional on the expected sequence number — that
// conditional write is the split-brain guard: if two would-be owners race, only one
// Append at a given room_seq can win; the loser gets ErrConflict and learns it lost.
package logstore

import (
	"context"
	"errors"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// ErrConflict is returned by Append when expectedSeq does not match the log head — i.e.
// the write would create a gap or overwrite an existing entry. It is how a stale owner
// discovers it has lost ownership.
var ErrConflict = errors.New("logstore: sequence conflict")

// LogStore is a durable, append-only, per-room ordered event log plus snapshot storage.
type LogStore interface {
	// Append writes event as room_seq == expectedSeq. It succeeds only if expectedSeq is
	// exactly one past the current head (no gaps, no overwrite); otherwise ErrConflict.
	Append(ctx context.Context, roomID string, expectedSeq uint64, event *aetherv1.Event) error

	// Read returns events with room_seq > fromSeq, in ascending order.
	Read(ctx context.Context, roomID string, fromSeq uint64) ([]*aetherv1.Event, error)

	// Head returns the highest room_seq stored for a room (0 if the room is empty).
	Head(ctx context.Context, roomID string) (uint64, error)

	// WriteSnapshot stores an opaque snapshot blob taken at room_seq seq. The blob is
	// produced by the owner (a serialized roomcore snapshot); logstore treats it as bytes.
	WriteSnapshot(ctx context.Context, roomID string, seq uint64, data []byte) error

	// ReadSnapshot returns the latest snapshot blob and its room_seq. ok is false if the
	// room has no snapshot.
	ReadSnapshot(ctx context.Context, roomID string) (data []byte, seq uint64, ok bool, err error)
}
