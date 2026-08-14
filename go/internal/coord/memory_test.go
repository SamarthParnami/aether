package coord_test

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/SamarthParnami/aether/go/internal/coord"
)

// t0 is an arbitrary, fixed base instant for the unit tests.
var t0 = time.Unix(1000, 0)

// The in-memory coordinator cannot fail to answer, so these helpers assert that once and unwrap —
// keeping every assertion below about OWNERSHIP rather than about error plumbing. The durable
// adapter is where the error paths get their own cases, against this same contract.
func claim(
	t *testing.T, m coord.Coordinator, room, owner, addr string, now time.Time, ttl time.Duration,
) (coord.Lease, bool) {
	t.Helper()
	l, ok, err := m.Claim(t.Context(), room, owner, addr, now, ttl)
	if err != nil {
		t.Fatalf("Claim(%q, %q): unexpected error: %v", room, owner, err)
	}
	return l, ok
}

func renew(
	t *testing.T, m coord.Coordinator, room, owner string, now time.Time, ttl time.Duration,
) (coord.Lease, bool) {
	t.Helper()
	l, ok, err := m.Renew(t.Context(), room, owner, now, ttl)
	if err != nil {
		t.Fatalf("Renew(%q, %q): unexpected error: %v", room, owner, err)
	}
	return l, ok
}

func release(t *testing.T, m coord.Coordinator, room, owner string) {
	t.Helper()
	if err := m.Release(t.Context(), room, owner); err != nil {
		t.Fatalf("Release(%q, %q): unexpected error: %v", room, owner, err)
	}
}

func current(t *testing.T, m coord.Coordinator, room string, now time.Time) (coord.Lease, bool) {
	t.Helper()
	l, ok, err := m.Current(t.Context(), room, now)
	if err != nil {
		t.Fatalf("Current(%q): unexpected error: %v", room, err)
	}
	return l, ok
}

func TestClaimEmptyAndContested(t *testing.T) {
	m := coord.NewMemory()

	l, ok := claim(t, m, "r", "A", "a.example:7001", t0, 10*time.Second)
	if !ok || l.Owner != "A" || l.Token != 1 || l.Addr != "a.example:7001" {
		t.Fatalf("first claim = %+v, %v", l, ok)
	}

	// B cannot claim while A's lease is live.
	if _, ok := claim(t, m, "r", "B", "b.example:7002", t0.Add(5*time.Second), 10*time.Second); ok {
		t.Fatal("B claimed a room A still holds")
	}
}

func TestTakeoverAfterExpiryBumpsToken(t *testing.T) {
	m := coord.NewMemory()
	claim(t, m, "r", "A", "a.example:7001", t0, 10*time.Second) // expires at t0+10s

	// after expiry
	l, ok := claim(t, m, "r", "B", "b.example:7002", t0.Add(11*time.Second), 10*time.Second)
	if !ok || l.Owner != "B" || l.Addr != "b.example:7002" {
		t.Fatalf("takeover failed: %+v, %v", l, ok)
	}
	if l.Token != 2 {
		t.Errorf("token = %d, want 2 (bumped on takeover)", l.Token)
	}
}

func TestRenew(t *testing.T) {
	m := coord.NewMemory()
	first, _ := claim(t, m, "r", "A", "a.example:7001", t0, 10*time.Second)

	l, ok := renew(t, m, "r", "A", t0.Add(5*time.Second), 10*time.Second) // still ours
	if !ok || !l.Expiry.Equal(t0.Add(15*time.Second)) || l.Token != first.Token || l.Addr != "a.example:7001" {
		t.Fatalf("renew = %+v, %v", l, ok) // Renew must preserve the published addr
	}
	if _, ok := renew(t, m, "r", "B", t0.Add(5*time.Second), 10*time.Second); ok {
		t.Fatal("B renewed a lease it doesn't hold")
	}
	if _, ok := renew(t, m, "r", "A", t0.Add(100*time.Second), 10*time.Second); ok {
		t.Fatal("renew succeeded after expiry")
	}
}

func TestReleaseFreesRoom(t *testing.T) {
	m := coord.NewMemory()
	claim(t, m, "r", "A", "a.example:7001", t0, 100*time.Second)

	release(t, m, "r", "B") // not the owner: no-op
	if _, ok := current(t, m, "r", t0.Add(time.Second)); !ok {
		t.Fatal("non-owner release freed the room")
	}

	release(t, m, "r", "A")
	if _, ok := current(t, m, "r", t0.Add(time.Second)); ok {
		t.Fatal("owner release did not free the room")
	}
}

func TestCurrentRespectsExpiry(t *testing.T) {
	m := coord.NewMemory()
	claim(t, m, "r", "A", "a.example:7001", t0, 10*time.Second)

	if l, ok := current(t, m, "r", t0.Add(9*time.Second)); !ok || l.Addr != "a.example:7001" {
		t.Fatalf("Current before expiry = %+v, %v; want addr a.example:7001", l, ok)
	}
	if _, ok := current(t, m, "r", t0.Add(10*time.Second)); ok {
		t.Fatal("Current true at/after expiry")
	}
}

// Safety property: across a random sequence of competing claims/renews/releases with
// advancing time, two owners can never both hold the room at the same instant, and the
// fencing token never decreases.
func TestProp_AtMostOneOwnerAndMonotonicToken(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		m := coord.NewMemory()
		owners := []string{"A", "B"}
		now := time.Unix(0, 0)
		var lastToken uint64

		// rapid.T is not a *testing.T, so the helpers above don't apply; the coordinator is called
		// directly and every error is a hard failure of the property.
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
				l, ok, err := m.Claim(ctx, "r", o, o+":addr", now, ttl)
				if err != nil {
					t.Fatalf("Claim: unexpected error: %v", err)
				}
				if ok {
					// A live owner exists ⇒ the other node must not be able to claim now.
					_, ok2, err := m.Claim(ctx, "r", other, other+":addr", now, ttl)
					if err != nil {
						t.Fatalf("contending Claim: unexpected error: %v", err)
					}
					if ok2 {
						t.Fatal("two owners acquired the room at the same instant")
					}
					if l.Token < lastToken {
						t.Fatalf("token decreased: %d < %d", l.Token, lastToken)
					}
					lastToken = l.Token
				}
			case 1:
				if _, _, err := m.Renew(ctx, "r", o, now, ttl); err != nil {
					t.Fatalf("Renew: unexpected error: %v", err)
				}
			case 2:
				if err := m.Release(ctx, "r", o); err != nil {
					t.Fatalf("Release: unexpected error: %v", err)
				}
			}
		}
	})
}
