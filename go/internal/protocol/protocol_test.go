package protocol_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// A durable Commit survives a binary round-trip with its dedup key intact.
func TestCommitRoundTrip(t *testing.T) {
	msg := &aetherv1.ClientMessage{
		Body: &aetherv1.ClientMessage_Commit{
			Commit: &aetherv1.Commit{
				RoomId:    "room-1",
				ClientSeq: 42,
				Body:      &aetherv1.EventBody{},
			},
		},
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got aetherv1.ClientMessage
	if err := proto.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	commit := got.GetCommit()
	if commit == nil {
		t.Fatalf("expected commit body, got %T", got.GetBody())
	}
	if commit.GetRoomId() != "room-1" {
		t.Errorf("room_id = %q, want %q", commit.GetRoomId(), "room-1")
	}
	if commit.GetClientSeq() != 42 {
		t.Errorf("client_seq = %d, want 42", commit.GetClientSeq())
	}
}

// A committed Event carries the origin dedup key (origin_client_seq) — the basis
// of "fan-out is the ack".
func TestEventCarriesOrigin(t *testing.T) {
	msg := &aetherv1.ServerMessage{
		Body: &aetherv1.ServerMessage_Event{
			Event: &aetherv1.Event{
				RoomId:          "room-1",
				RoomSeq:         416,
				OriginClientId:  "client-A",
				OriginClientSeq: 42,
				Body:            &aetherv1.EventBody{},
			},
		},
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got aetherv1.ServerMessage
	if err := proto.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	ev := got.GetEvent()
	if ev == nil {
		t.Fatalf("expected event body, got %T", got.GetBody())
	}
	if ev.GetRoomSeq() != 416 || ev.GetOriginClientSeq() != 42 {
		t.Errorf("room_seq/origin_client_seq = %d/%d, want 416/42", ev.GetRoomSeq(), ev.GetOriginClientSeq())
	}
}

// The generic Phase-1 event (KeyValueSet) survives a round-trip — the test
// vehicle the backbone exercises before the real catalog exists.
func TestGenericKeyValueEvent(t *testing.T) {
	msg := &aetherv1.ClientMessage{
		Body: &aetherv1.ClientMessage_Commit{
			Commit: &aetherv1.Commit{
				RoomId:    "room-1",
				ClientSeq: 7,
				Body: &aetherv1.EventBody{
					Kind: &aetherv1.EventBody_KvSet{
						KvSet: &aetherv1.KeyValueSet{Key: "slide", Value: []byte("7")},
					},
				},
			},
		},
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got aetherv1.ClientMessage
	if err := proto.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	kv := got.GetCommit().GetBody().GetKvSet()
	if kv == nil {
		t.Fatalf("expected kv_set body")
	}
	if kv.GetKey() != "slide" || string(kv.GetValue()) != "7" {
		t.Errorf("kv = %q/%q, want slide/7", kv.GetKey(), string(kv.GetValue()))
	}
}
