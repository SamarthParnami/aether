package gateway

import (
	"context"
	mathrand "math/rand/v2"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
)

// lockedRand is the DST jitterRand: a seeded source guarded by a mutex, so the per-conn relays that
// share a Server's rng draw reproducibly AND race-free. (A bare *rand.Rand shared across goroutines
// is not concurrency-safe — the reason globalRand wraps the top-level pool for production instead.)
type lockedRand struct {
	mu sync.Mutex
	r  *mathrand.Rand
}

func newLockedRand(seed uint64) *lockedRand {
	return &lockedRand{r: mathrand.New(mathrand.NewPCG(seed, seed))}
}

func (l *lockedRand) Int64N(n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Int64N(n)
}

// framePipe is an in-memory frameConn pair (like net.Pipe, but frame-oriented): a WriteFrame on one
// end is a ReadFrame on the other. Every operation is a channel send/recv or a select — all
// DURABLY BLOCKING under testing/synctest — so a gateway connection served over it parks only on
// channels/timers and the bubble can drive it with a fake clock. This is the transport swap the
// synctest-hybrid DST approach requires (real socket I/O is not durably blocking). It is the seed of
// the G11b/c harness.
type framePipe struct {
	recv    chan []byte // frames delivered to THIS end
	send    chan []byte // frames THIS end writes (= peer's recv)
	done    chan struct{}
	closeFn func()
}

// newFramePipe returns the two connected ends (client, server). Channels are unbuffered, so a write
// is a strict handoff that blocks until the peer reads — realistic backpressure, and durably
// blocking for synctest. Create it INSIDE the bubble so the channels are bubble channels.
func newFramePipe() (client, server *framePipe) {
	c2s, s2c := make(chan []byte), make(chan []byte)
	done := make(chan struct{})
	var once sync.Once
	closeFn := func() { once.Do(func() { close(done) }) }
	client = &framePipe{recv: s2c, send: c2s, done: done, closeFn: closeFn}
	server = &framePipe{recv: c2s, send: s2c, done: done, closeFn: closeFn}
	return client, server
}

func (p *framePipe) ReadFrame(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, net.ErrClosed
	case data := <-p.recv:
		return data, nil
	}
}

func (p *framePipe) WriteFrame(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return net.ErrClosed
	case p.send <- data:
		return nil
	}
}

// Ping is a no-op success: an in-memory pipe can't be silently half-open.
func (p *framePipe) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return net.ErrClosed
	default:
		return nil
	}
}

func (p *framePipe) Close() error {
	p.closeFn()
	return nil
}

func writePipe(ctx context.Context, t *testing.T, c frameConn, m *aetherv1.ClientMessage) {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFrame(ctx, data); err != nil {
		t.Fatal(err)
	}
}

func readPipe(ctx context.Context, t *testing.T, c frameConn) *aetherv1.ServerMessage {
	t.Helper()
	data, err := c.ReadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var m aetherv1.ServerMessage
	if err := proto.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return &m
}

// The synctest-hybrid DST de-risk: a REAL gateway connection (its read/write/ping/ops goroutines,
// unmodified) served over an in-memory frame pipe inside a testing/synctest bubble reaches a
// deterministic quiescence point (synctest.Wait returns → every goroutine is durably blocked, so
// none busy-spins), answers an app Ping, and tears down cleanly on cancel with no deadlock
// (synctest.Test fails on a stuck goroutine). This is the property the whole G11 approach rests on.
// (The relay path — the 5th goroutine — needs the owner transport and is de-risked in G11b.)
func TestSynctestConnReachesQuiescenceAndTearsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := NewServer(DevAuthenticator{Header: "X-Aether-Principal"}, NewOwnerLocator(coord.NewMemory()))
		client, server := newFramePipe()

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			newConn(srv, Principal{ID: "user-1"}, server).run(ctx)
		}()

		// An app-level Ping exercises the read loop → inline Pong → write loop, with the ops worker and
		// ping keepalive also running. (No Join, so no relay — that's G11b.)
		writePipe(ctx, t, client, &aetherv1.ClientMessage{
			Body: &aetherv1.ClientMessage_Ping{Ping: &aetherv1.Ping{Id: "hello"}},
		})

		// Every connection goroutine must reach a durably-blocked state — if any busy-spun, Wait would
		// hang and the test would time out.
		synctest.Wait()

		if got := readPipe(ctx, t, client).GetPong().GetId(); got != "hello" {
			t.Fatalf("pong id = %q, want hello", got)
		}

		cancel() // read loop's ReadFrame returns ctx.Err → run() drains every goroutine
		<-done   // run() returned: clean teardown, no leaked/deadlocked goroutine
	})
}

// The WithRand seam (from #39 review, which noted it was plumbed but unexercised): the relay backoff
// jitter draws from the INJECTED source, a seeded source is REPRODUCIBLE run-to-run, and a locked
// source is -RACE-CLEAN under the concurrent per-conn draws the seam exists for.
func TestWithRandSeam(t *testing.T) {
	// Reproducibility: c.waitRetry draws its jitter from c.srv.rng. Under synctest the backoff sleep
	// is instant and the fake clock advances by exactly the jittered duration — so we read the drawn
	// jitter straight off the clock, and the same seed must draw the same jitter.
	draw := func(seed uint64) time.Duration {
		var jitter time.Duration
		synctest.Test(t, func(t *testing.T) {
			srv := NewServer(DevAuthenticator{Header: "X"}, NewOwnerLocator(coord.NewMemory()), WithRand(newLockedRand(seed)))
			c := &conn{srv: srv}
			start := time.Now()
			if !c.waitRetry(context.Background(), time.Second) {
				t.Fatal("waitRetry returned false (cancelled) unexpectedly")
			}
			jitter = time.Since(start)
		})
		return jitter
	}
	if a, b := draw(42), draw(42); a != b {
		t.Fatalf("seeded jitter not reproducible: %v vs %v", a, b)
	}
	if j := draw(42); j < 500*time.Millisecond || j > time.Second { // half jitter of d=1s ∈ [d/2, d]
		t.Fatalf("jitter %v outside [500ms, 1s] — the draw did not happen as expected", j)
	}

	// Race-cleanliness: many concurrent draws on one locked source (per-conn relays sharing a Server's
	// rng) must be -race-clean; a bare *rand.Rand here would trip the detector.
	shared := newLockedRand(1)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = shared.Int64N(1000)
			}
		}()
	}
	wg.Wait()
}
