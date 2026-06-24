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
