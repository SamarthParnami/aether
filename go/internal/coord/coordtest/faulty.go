// Package coordtest holds test seams for the coord layer: fault injection today, and the home for
// the shared conformance suite the durable adapter will be held to.
//
// It is a separate package rather than a _test.go file because the interesting failures are not
// coord's own — they are what the gateway and the room runtime DO with a coord that stops
// answering, and those tests live in other packages. Keeping it out of coord also keeps fault
// injection out of the production surface.
package coordtest

import (
	"context"
	"sync"
	"time"

	"github.com/SamarthParnami/aether/go/internal/coord"
)

// Faulty wraps a Coordinator and fails chosen methods on demand, so a test can reproduce the one
// thing an in-memory map never does: not answer.
//
// Errors are set per method because the branches under test are per method — the gateway's resolve
// only reads Current, while the owner's acquire only calls Claim, and a fault that hit both could
// not tell which side froze. Every failing method returns the zero lease and false ALONGSIDE the
// error, deliberately: that is the shape a careless durable adapter produces, so a caller that
// checks the bool before the error will read "unowned" and take the wrong branch. Tests that pass
// against this decorator are tests that check the error first.
//
// PRECEDENCE: a cancelled ctx wins over an armed fault. Arming FailClaim(errBoom) and passing a
// dead context yields context.Canceled, not errBoom — the same order a real adapter would apply,
// and worth knowing before discovering it from a failing assertion.
//
// Safe for concurrent use — the gateway suite runs under -race with the relay resolving from its
// own goroutine.
type Faulty struct {
	inner coord.Coordinator

	mu      sync.Mutex
	claim   error
	renew   error
	release error
	current error
}

// New returns a Faulty wrapping inner, with no faults set (every call passes through).
func New(inner coord.Coordinator) *Faulty { return &Faulty{inner: inner} }

// FailClaim makes Claim return err. A nil err clears the fault.
func (f *Faulty) FailClaim(err error) { f.mu.Lock(); f.claim = err; f.mu.Unlock() }

// FailRenew makes Renew return err. A nil err clears the fault.
func (f *Faulty) FailRenew(err error) { f.mu.Lock(); f.renew = err; f.mu.Unlock() }

// FailRelease makes Release return err. A nil err clears the fault.
func (f *Faulty) FailRelease(err error) { f.mu.Lock(); f.release = err; f.mu.Unlock() }

// FailCurrent makes Current return err. A nil err clears the fault.
func (f *Faulty) FailCurrent(err error) { f.mu.Lock(); f.current = err; f.mu.Unlock() }

// FailAll makes every method return err — a total coord brownout. A nil err clears all faults.
func (f *Faulty) FailAll(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claim, f.renew, f.release, f.current = err, err, err, err
}

// fault returns the error this call should fail with: a cancelled ctx first, then the armed
// per-method fault.
//
// Honouring ctx is not politeness — it is the only thing that makes ctx misuse VISIBLE. coord.Memory
// takes `_ context.Context` and every test in the repo runs against it, so without this check no
// Coordinator in the tree can fail because of ctx state, and a caller that passes an
// already-cancelled context (the signal context, or a t.Context() that Cleanup has just cancelled)
// gets a silent success here and a stranded lease against the real DynamoDB adapter. A durable
// store WILL reject a dead context, so the fake has to as well or the fault-injection seam is blind
// to the one class of bug it exists to catch.
func (f *Faulty) fault(ctx context.Context, pick func(*Faulty) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return pick(f)
}

// Claim implements coord.Coordinator.
func (f *Faulty) Claim(
	ctx context.Context, roomID, owner, addr string, now time.Time, ttl time.Duration,
) (coord.Lease, bool, error) {
	if err := f.fault(ctx, func(f *Faulty) error { return f.claim }); err != nil {
		return coord.Lease{}, false, err
	}
	return f.inner.Claim(ctx, roomID, owner, addr, now, ttl)
}

// Renew implements coord.Coordinator.
func (f *Faulty) Renew(
	ctx context.Context, roomID, owner string, now time.Time, ttl time.Duration,
) (coord.Lease, bool, error) {
	if err := f.fault(ctx, func(f *Faulty) error { return f.renew }); err != nil {
		return coord.Lease{}, false, err
	}
	return f.inner.Renew(ctx, roomID, owner, now, ttl)
}

// Release implements coord.Coordinator.
func (f *Faulty) Release(ctx context.Context, roomID, owner string) error {
	if err := f.fault(ctx, func(f *Faulty) error { return f.release }); err != nil {
		return err
	}
	return f.inner.Release(ctx, roomID, owner)
}

// Current implements coord.Coordinator.
func (f *Faulty) Current(ctx context.Context, roomID string, now time.Time) (coord.Lease, bool, error) {
	if err := f.fault(ctx, func(f *Faulty) error { return f.current }); err != nil {
		return coord.Lease{}, false, err
	}
	return f.inner.Current(ctx, roomID, now)
}

var _ coord.Coordinator = (*Faulty)(nil)
