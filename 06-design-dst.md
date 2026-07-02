# 06 — Full-path DST (client ↔ gateway ↔ owner): design

Status: proposed (G11). Precedes the G11 implementation PRs, the way
[05-design-gateway.md](05-design-gateway.md) preceded G1–G10.

## 1. Goal — the Phase-1 exit criterion

Phase 1 is "done" when a **deterministic-simulation (DST) chaos suite** exercises the *whole*
`client ↔ gateway ↔ owner ↔ log` path across thousands of seeds and, under injected chaos (kill the
owner, kill the gateway, partition the network, drop/reorder/dup messages, expire leases, reconnect
storms), every run still upholds the load-bearing invariants:

- **Exactly-once** — no committed event is applied twice (dedup survives reconnect + failover).
- **No-loss** — every committed event eventually reaches every live subscriber.
- **Total order / convergence** — all clients converge to the same room state (`room_seq` 1..N, gap-free).
- **At-most-one effective owner** — no split-brain write is ever durable.
- **Recovery, not reload** — a client re-homes/resumes from its cursor; it never has to reload from zero.

We already have this **at the owner**: `roomruntime/dst_failover_test.go` sweeps seeds 1..120,
driving a 3-node cluster through the `sim` kernel with `WithClock(s.Now)`, killing/reviving owners,
and asserting exactly-once + no-loss + total-order + convergence. G11 extends the guarantee across
the gateway to the client.

## 2. The obstacle — the gateway is concurrent, the sim is single-threaded

The owner DST is bit-for-bit replayable for one reason: the `sim` is a **single-threaded
discrete-event loop** (`Schedule(delay, fn)` → `Run()`), and `roomruntime`'s methods are
**synchronous**, so the sim calls them directly from scheduled callbacks. There are no real
goroutines and no wall-clock time in the loop, so a seed replays identically every time.

The **gateway is the opposite by design** (and correctly so — it's a network server): one client
connection is *five* real goroutines (readLoop, writeLoop, pingLoop, opsLoop, and the per-room relay
supervisor) coordinating over channels and a shared context, plus real timers (the ping ticker, the
relay backoff). Go provides **no deterministic goroutine scheduler**, so:

> You cannot drive the merged gateway from the single-threaded sim, and real goroutines mean the
> gateway layer is **not** byte-for-byte replayable — no injected clock changes that.

This is the fork G11 must resolve.

## 3. Approaches considered

**A. Rewrite the gateway as an event-driven sim node.** Re-express the connection as a state
machine with no goroutines/timers — every action (frame in, RPC reply, timer fire) is a `sim` event
handled synchronously — so the whole path runs single-threaded and replays bit-for-bit.
- ✅ True bit-for-bit determinism across the *entire* path.
- ❌ Throws away the merged, tested, goroutine-based gateway (G1–G10) and maintains a *second*
  implementation — or ships the sim version to prod, abandoning the natural concurrent design. A
  large step backward on working code, and a permanent two-implementations tax.

**B. Integration DST — real gateway + owner over in-memory, sim-faulted transports.** Keep the real
concurrent gateway and owner. Replace only the *transports* with in-memory implementations the sim
controls, drive **seeded** client workloads and **seeded** chaos, and assert the invariants at
quiescence, over thousands of seeds.
- ✅ Exercises the *actual* production code (races, teardown, recovery, backoff) under chaos.
- ✅ Reuses `sim.Network`'s fault model (drop/dup/delay/reorder/partition) and seeded RNG for the
  workload + fault schedule — so a failing seed re-runs the *same* workload and *same* fault
  schedule (run it under `-race` to debug).
- ⚠️ **Not byte-for-byte replayable**: real goroutine scheduling can interleave differently between
  runs of the same seed. The *inputs* (workload, faults) are seeded and reproducible; the internal
  interleaving is not. The invariants are checked on **every** seed regardless.

**Decision: B, layered on the owner's existing bit-for-bit DST (a hybrid).** The Phase-1 exit
criterion is about the **invariants holding under chaos over thousands of seeds**, not about the
gateway's goroutine interleaving being reproducible. Bit-for-bit is a *means* (cheap debugging), and
we keep it where it's achievable — the owner core (`roomcore` pure, `roomruntime` single-threaded).
Rewriting the concurrent gateway solely to make its scheduling replayable is not worth abandoning
the real design. Integration DST checks the same invariants on every seed and runs the real code;
the reproducibility gap is bounded to internal interleaving and mitigated by seeded inputs + `-race`.

## 4. The seams we need

Most of the injection surface already exists; G11 fills the gaps.

| Seam | Today | G11 needs |
|------|-------|-----------|
| **gateway → owner RPC** | `OwnerLocator.Owner()` returns the `RoomServiceClient` *interface*; `WithLocatorHTTPClient(connect.HTTPClient)` injects the transport | An **in-memory `connect.HTTPClient`** (or a direct in-memory `RoomServiceClient`) that routes calls to in-process owners through `sim.Network`, so RPCs can be dropped / delayed / partitioned and owners killed. |
| **client → gateway WS** | `conn` reads/writes a concrete `*coder/websocket.Conn` | Abstract the read/write behind a tiny **frame transport interface** (`ReadFrame`/`WriteFrame`) with a real WS impl (prod) and an in-memory impl (DST) the sim can fault. |
| **gateway clock** | ping ticker + relay backoff use real `time` | A **clock seam** (`Now` + `NewTimer`/ticker) — real in prod, sim-driven in DST — so keepalive/backoff advance on the sim clock. *(Owner already has `WithClock`.)* |
| **gateway RNG** | relay jitter uses global `math/rand/v2` | An injectable **rng** (`WithRand`), defaulting to a real source, so the fault/backoff randomness is seeded in DST. *(Flagged in #36 review.)* |
| **owner clock** | `roomruntime.WithClock` | Reuse as-is. |

The client → gateway seam is the only real refactor; the rest are additive options mirroring the
owner's existing pattern.

## 5. Fault matrix

Driven by `sim.Network` (`FaultConfig{DropProb, DupProb, MinDelay, MaxDelay}` + `Partition`/`Heal`)
plus lifecycle events scheduled on the sim:

- **Message faults** on both transports: drop, duplicate, delay, reorder.
- **Partitions**: client|gateway, gateway|owner, owner|owner (coord), healed after a while.
- **Owner death / re-home**: kill an owner mid-session; a survivor re-homes the room from the shared log.
- **Gateway death**: kill a gateway mid-session; the client reconnects (to another gateway) and resumes from its cursor.
- **Lease expiry / reconnect storms**: many clients reconnect at once; leases lapse and are re-claimed.

## 6. Invariants asserted (every seed, at quiescence)

Run the seeded workload + fault schedule to completion, then drain and assert:

1. **Exactly-once**: for each `(client_id, client_seq)`, exactly one `room_seq` in the log.
2. **No-loss**: every applied commit appears in every live client's received stream.
3. **Total order + convergence**: the log is `room_seq` 1..N gap-free; every client's final state equals `RestoreAndReplay(log)`.
4. **At-most-one-owner**: no two owners hold a non-expired lease for the same room at the same sim time; no `ErrConflict`-losing write is durable.
5. **Recovery**: after every owner/gateway kill, affected clients reach head via resume — never a from-zero reload (assert cursor monotonicity, and a `FROZEN` is always followed by a `LIVE`).

Teeth (as in the owner DST): assert the chaos actually *bit* — counts of takeovers, conflicts,
dedup-hits, reconnects each `> 0` across the sweep, so a suite that accidentally injects no faults fails.

## 7. PR decomposition (small, independently-mergeable)

- **G11a — gateway clock + RNG seam.** `WithClock` / `WithRand` on the gateway `Server` (mirror the owner); relay backoff/jitter + ping use them; default to real time/rand. No prod behaviour change. Removes the global `math/rand/v2` (#36 review). *(Small, foundational.)*
- **G11b — client↔gateway frame transport seam.** Extract `ReadFrame`/`WriteFrame` behind an interface; real WS impl + an in-memory impl. Existing gateway tests re-pointed at the seam (no behaviour change). *(Interface-before-callers.)*
- **G11c — in-memory sim transports.** An in-process `RoomServiceClient` (gateway→owner) and the in-memory client transport, both routed through `sim.Network` with faults; a `dstCluster` harness wiring N gateways + M owners + K clients + coord + logs on one sim.
- **G11d — the chaos suite.** Seeded workload (presenters commit, watchers read, some broadcast) + the fault schedule from §5; the §6 invariant assertions; sweep seeds 1..N with the §6 teeth. This is the Phase-1 exit gate.

Each PR ships its own tests; G11d is the milestone.

## 8. Reproducibility & debugging a failing seed

The workload and fault schedule are a pure function of the seed, so a failing seed **re-runs the
same scenario**. Because goroutine interleaving isn't pinned, reproduction is *statistical*, not
guaranteed on the first replay — so the debug loop is: re-run the seed under `-race` and with a tight
`-count` until it trips, with verbose per-event logging keyed by sim time. Where a bug is actually in
the owner core (dedup, ordering, conflict handling), it also reproduces bit-for-bit in the existing
owner DST, which stays the first line of defence.
