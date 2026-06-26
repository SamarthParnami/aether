package coord_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/SamarthParnami/aether/go/internal/coord"
)

func TestClaimEmptyAndContested(t *testing.T) {
	m := coord.NewMemory()

	l, ok := m.Claim("r", "A", 0, 10)
	if !ok || l.Owner != "A" || l.Token != 1 {
		t.Fatalf("first claim = %+v, %v", l, ok)
	}

	// B cannot claim while A's lease is live.
	if _, ok := m.Claim("r", "B", 5, 10); ok {
		t.Fatal("B claimed a room A still holds")
	}
}

func TestTakeoverAfterExpiryBumpsToken(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", 0, 10) // expires at 10

	l, ok := m.Claim("r", "B", 11, 10) // after expiry
	if !ok || l.Owner != "B" {
		t.Fatalf("takeover failed: %+v, %v", l, ok)
	}
	if l.Token != 2 {
		t.Errorf("token = %d, want 2 (bumped on takeover)", l.Token)
	}
}

func TestRenew(t *testing.T) {
	m := coord.NewMemory()
	first, _ := m.Claim("r", "A", 0, 10)

	l, ok := m.Renew("r", "A", 5, 10) // still ours
	if !ok || l.Expiry != 15 || l.Token != first.Token {
		t.Fatalf("renew = %+v, %v", l, ok)
	}
	if _, ok := m.Renew("r", "B", 5, 10); ok {
		t.Fatal("B renewed a lease it doesn't hold")
	}
	if _, ok := m.Renew("r", "A", 100, 10); ok {
		t.Fatal("renew succeeded after expiry")
	}
}

func TestReleaseFreesRoom(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", 0, 100)

	m.Release("r", "B") // not the owner: no-op
	if _, ok := m.Current("r", 1); !ok {
		t.Fatal("non-owner release freed the room")
	}

	m.Release("r", "A")
	if _, ok := m.Current("r", 1); ok {
		t.Fatal("owner release did not free the room")
	}
}

func TestCurrentRespectsExpiry(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", 0, 10)

	if _, ok := m.Current("r", 9); !ok {
		t.Fatal("Current false before expiry")
	}
	if _, ok := m.Current("r", 10); ok {
		t.Fatal("Current true at/after expiry")
	}
}

// Safety property: across a random sequence of competing claims/renews/releases with
// advancing time, two owners can never both hold the room at the same instant, and the
// fencing token never decreases.
func TestProp_AtMostOneOwnerAndMonotonicToken(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := coord.NewMemory()
		owners := []string{"A", "B"}
		var now, lastToken uint64

		for range make([]struct{}, rapid.IntRange(0, 60).Draw(t, "steps")) {
			now += uint64(rapid.IntRange(0, 10).Draw(t, "dt"))
			o := rapid.SampledFrom(owners).Draw(t, "owner")
			other := "A"
			if o == "A" {
				other = "B"
			}
			ttl := uint64(rapid.IntRange(1, 6).Draw(t, "ttl"))

			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0:
				if l, ok := m.Claim("r", o, now, ttl); ok {
					// A live owner exists ⇒ the other node must not be able to claim now.
					if _, ok2 := m.Claim("r", other, now, ttl); ok2 {
						t.Fatal("two owners acquired the room at the same instant")
					}
					if l.Token < lastToken {
						t.Fatalf("token decreased: %d < %d", l.Token, lastToken)
					}
					lastToken = l.Token
				}
			case 1:
				m.Renew("r", o, now, ttl)
			case 2:
				m.Release("r", o)
			}
		}
	})
}
