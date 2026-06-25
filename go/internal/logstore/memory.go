package logstore

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// Memory is an in-memory LogStore for tests and local development. It enforces the same
// conditional-append semantics as the durable store. Safe for concurrent use.
type Memory struct {
	mu    sync.Mutex
	rooms map[string]*memRoom
}

type memRoom struct {
	events   []*aetherv1.Event // index i holds room_seq i+1
	snapData []byte
	snapSeq  uint64
	hasSnap  bool
}

// NewMemory returns an empty in-memory log store.
func NewMemory() *Memory {
	return &Memory{rooms: map[string]*memRoom{}}
}

func (m *Memory) room(roomID string) *memRoom {
	r := m.rooms[roomID]
	if r == nil {
		r = &memRoom{}
		m.rooms[roomID] = r
	}
	return r
}

// Append implements LogStore.
func (m *Memory) Append(_ context.Context, roomID string, expectedSeq uint64, event *aetherv1.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.room(roomID)
	head := uint64(len(r.events))
	if expectedSeq != head+1 {
		return ErrConflict
	}
	r.events = append(r.events, proto.Clone(event).(*aetherv1.Event))
	return nil
}

// Read implements LogStore.
func (m *Memory) Read(_ context.Context, roomID string, fromSeq uint64) ([]*aetherv1.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.room(roomID)
	if fromSeq >= uint64(len(r.events)) {
		return nil, nil
	}
	tail := r.events[fromSeq:]
	out := make([]*aetherv1.Event, len(tail))
	for i, e := range tail {
		out[i] = proto.Clone(e).(*aetherv1.Event)
	}
	return out, nil
}

// Head implements LogStore.
func (m *Memory) Head(_ context.Context, roomID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.room(roomID).events)), nil
}

// WriteSnapshot implements LogStore.
func (m *Memory) WriteSnapshot(_ context.Context, roomID string, seq uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.room(roomID)
	cp := make([]byte, len(data))
	copy(cp, data)
	r.snapData, r.snapSeq, r.hasSnap = cp, seq, true
	return nil
}

// ReadSnapshot implements LogStore.
func (m *Memory) ReadSnapshot(_ context.Context, roomID string) ([]byte, uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.room(roomID)
	if !r.hasSnap {
		return nil, 0, false, nil
	}
	cp := make([]byte, len(r.snapData))
	copy(cp, r.snapData)
	return cp, r.snapSeq, true, nil
}

// compile-time check that Memory satisfies the interface.
var _ LogStore = (*Memory)(nil)
