package coord_test

import (
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/SamarthParnami/aether/go/internal/coord"
)

// t0 is an arbitrary, fixed base instant for the unit tests.
var t0 = time.Unix(1000, 0)

func TestClaimEmptyAndContested(t *testing.T) {
	m := coord.NewMemory()

	l, ok := m.Claim("r", "A", t0, 10*time.Second)
	if !ok || l.Owner != "A" || l.Token != 1 {
		t.Fatalf("first claim = %+v, %v", l, ok)
	}

	// B cannot claim while A's lease is live.
	if _, ok := m.Claim("r", "B", t0.Add(5*time.Second), 10*time.Second); ok {
		t.Fatal("B claimed a room A still holds")
	}
}

func TestTakeoverAfterExpiryBumpsToken(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", t0, 10*time.Second) // expires at t0+10s

	l, ok := m.Claim("r", "B", t0.Add(11*time.Second), 10*time.Second) // after expiry
	if !ok || l.Owner != "B" {
		t.Fatalf("takeover failed: %+v, %v", l, ok)
	}
	if l.Token != 2 {
		t.Errorf("token = %d, want 2 (bumped on takeover)", l.Token)
	}
}

func TestRenew(t *testing.T) {
	m := coord.NewMemory()
	first, _ := m.Claim("r", "A", t0, 10*time.Second)

	l, ok := m.Renew("r", "A", t0.Add(5*time.Second), 10*time.Second) // still ours
	if !ok || !l.Expiry.Equal(t0.Add(15*time.Second)) || l.Token != first.Token {
		t.Fatalf("renew = %+v, %v", l, ok)
	}
	if _, ok := m.Renew("r", "B", t0.Add(5*time.Second), 10*time.Second); ok {
		t.Fatal("B renewed a lease it doesn't hold")
	}
	if _, ok := m.Renew("r", "A", t0.Add(100*time.Second), 10*time.Second); ok {
		t.Fatal("renew succeeded after expiry")
	}
}

func TestReleaseFreesRoom(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", t0, 100*time.Second)

	m.Release("r", "B") // not the owner: no-op
	if _, ok := m.Current("r", t0.Add(time.Second)); !ok {
		t.Fatal("non-owner release freed the room")
	}

	m.Release("r", "A")
	if _, ok := m.Current("r", t0.Add(time.Second)); ok {
		t.Fatal("owner release did not free the room")
	}
}

func TestCurrentRespectsExpiry(t *testing.T) {
	m := coord.NewMemory()
	m.Claim("r", "A", t0, 10*time.Second)

	if _, ok := m.Current("r", t0.Add(9*time.Second)); !ok {
		t.Fatal("Current false before expiry")
	}
	if _, ok := m.Current("r", t0.Add(10*time.Second)); ok {
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
		now := time.Unix(0, 0)
		var lastToken uint64

		for range make([]struct{}, rapid.IntRange(0, 60).Draw(t, "steps")) {
			now = now.Add(time.Duration(rapid.IntRange(0, 10).Draw(t, "dt")) * time.Second)
			o := rapid.SampledFrom(owners).Draw(t, "owner")
			other := "A"
			if o == "A" {
				other = "B"
			}
			ttl := time.Duration(rapid.IntRange(1, 6).Draw(t, "ttl")) * time.Second

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
