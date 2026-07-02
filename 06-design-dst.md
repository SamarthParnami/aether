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

> This section's decision was validated by a dedicated research pass (Go `testing/synctest`, gosim,
> Dropbox Nucleus/Trinity, Antithesis, Polar Signals). Sources are cited inline; see §3.1.

**A. Rewrite the gateway as an event-driven sim node.** Re-express the connection as a state
machine with no goroutines/timers — every action a `sim` event handled synchronously — so the whole
path runs single-threaded and replays bit-for-bit.
- ✅ True bit-for-bit determinism across the *entire* path (Dropbox took this path — Nucleus runs
  nearly all code on one control thread; Trinity re-runs a seed to the identical final state).
- ❌ Throws away the merged, tested, goroutine-based gateway (G1–G10) and maintains a *second*
  implementation. Bit-for-bit interleaving replay in Go **only** comes from this kind of heavyweight
  route — a single-threaded rewrite (Dropbox), source-translating the runtime (gosim), or an external
  hypervisor (Antithesis). Worth it *only if reproducible replay-debugging is itself a hard
  requirement* — which a "invariants over thousands of seeds" suite is not.

**B (plain). Integration DST over in-memory transports, real goroutines, real time.** Keep the real
gateway/owner; swap only transports; drive seeded workloads + chaos; assert invariants at quiescence.
- ✅ Runs the *actual* production code under chaos; minimal change.
- ⚠️ Real wall-clock timers make the suite slow (real backoff sleeps) and quiescence hard to detect;
  not byte-for-byte replayable. Still effective — Polar Signals' only-"mostly"-deterministic seeded
  DST found 3 data-loss + 2 data-duplication bugs in weeks — but leaves determinism on the table.

**C. synctest-hybrid — B, but run the real goroutines inside a `testing/synctest` bubble. ← CHOSEN.**
`testing/synctest` (Go 1.24 experimental, **stable in 1.25**; we build on 1.26) runs **real,
unmodified** goroutines in an isolated bubble with a **fake clock that advances only when every
goroutine is durably blocked**, and `synctest.Wait` blocks until the bubble is idle — a deterministic
**quiescence point**. So timers/backoff/keepalive fire **instantly and deterministically**, and the
harness gets a clean "system is quiet, now assert" signal — all with **no changes to the gateway**.
- ✅ Deterministic **time** + quiescence on the real concurrent code, no rewrite. Timer-heavy paths
  (relay backoff, ping) run instantly, so thousands of seeds are cheap.
- ✅ Reuses B's transport swap: synctest **requires** it anyway — real socket / Connect-RPC I/O is
  *not durably blocking*, so a goroutine parked on a real conn never lets the bubble go idle. The Go
  docs prescribe `net.Pipe`-style in-memory conns for exactly this.
- ⚠️ synctest gives deterministic *time*, **not** deterministic goroutine *interleaving* — it is a
  clock + idle detector, not a replay engine, and is designed to run **with `-race`**. So the suite
  is reproducible in its *inputs* (seeded workload + faults) but not byte-for-byte in scheduling.
  That is precisely what a Phase-1 invariants suite needs, and no more.

**Decision: C (synctest-hybrid), layered on the owner's existing bit-for-bit DST.** The Phase-1 exit
criterion is *invariants holding under chaos over thousands of seeds*, not reproducible gateway
scheduling. synctest gives the achievable determinism (time + quiescence) on the **real** code for
free on top of the transport swap we needed regardless; option A's rewrite buys byte-for-byte replay
we don't require, at a cost we don't want. We keep bit-for-bit where it already exists and is cheap —
the owner core (`roomcore` pure, `roomruntime` single-threaded, its own `sim`-driven DST).

### 3.1 Sources
- Go `testing/synctest`: fake clock, durable-block idle detection, `synctest.Wait`, "requires a fake
  network implementation (e.g. `net.Pipe`)" — [go.dev/blog/synctest](https://go.dev/blog/synctest),
  [pkg.go.dev/testing/synctest](https://pkg.go.dev/testing/synctest).
- Bit-for-bit interleaving replay needs heavyweight routes: gosim (source-translates the runtime),
  Dropbox Nucleus/Trinity (single-threaded rewrite), Antithesis (deterministic hypervisor).
- Imperfect ("mostly") deterministic seeded-fault DST still catches serious bugs — Polar Signals
  (3 data-loss + 2 data-dup bugs in weeks); "an 80% solution suffices."

## 4. The seams we need

Most of the injection surface already exists; G11 fills the gaps.

| Seam | Today | G11 needs |
|------|-------|-----------|
| **gateway → owner RPC** | `OwnerLocator.Owner()` returns the `RoomServiceClient` *interface*; `WithLocatorHTTPClient(connect.HTTPClient)` injects the transport | An **in-memory `connect.HTTPClient`** (or a direct in-memory `RoomServiceClient`) that routes calls to in-process owners through `sim.Network`, so RPCs can be dropped / delayed / partitioned and owners killed. |
| **client → gateway WS** | `conn` reads/writes a concrete `*coder/websocket.Conn` | Abstract the read/write behind a tiny **frame transport interface** (`ReadFrame`/`WriteFrame`) with a real WS impl (prod) and an in-memory impl (DST) the sim can fault. |
| **gateway clock** | ping ticker + relay backoff use the standard `time` package | **Nothing** — `testing/synctest` replaces the `time` package's clock *inside the bubble automatically*. `time.NewTimer`/`Ticker`/`Now`/`Sleep` become fake-clock-driven with no code change. (No `WithClock` injection needed, unlike the owner.) |
| **gateway RNG** | relay jitter uses global `math/rand/v2` | Minor: an optional injectable rng (`WithRand`) so jitter is seeded. Not load-bearing — synctest makes the jittered sleep instant anyway; do it only to silence the #36 non-determinism note. |
| **owner clock** | `roomruntime.WithClock` | Pass `time.Now`; the bubble's fake clock drives lease expiry too. |

The two **transport seams are the real work** (and the enabler — synctest *requires* them). The clock
"seam" evaporates: synctest gives it for free. The RNG is a nice-to-have.

## 5. Fault matrix

Injected at the in-memory transport wrappers (the fault model mirrors `sim.Network`'s
`DropProb`/`DupProb`/`MinDelay`/`MaxDelay` + `Partition`/`Heal`), seeded by the per-seed `*rand.Rand`;
lifecycle events (kills, reconnects) scheduled on the bubble's fake clock via `time.AfterFunc`:

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

- **G11a — synctest smoke + transport seams (client↔gateway).** Extract the connection's frame
  read/write behind a tiny interface (real `coder/websocket` impl unchanged for prod; a `net.Pipe`-
  style in-memory impl for tests). Prove the payoff up front: a `synctest.Test` that runs **one real
  gateway conn + one real owner over in-memory transports** through a Join/Commit/relay round-trip,
  reaching `synctest.Wait` quiescence with no deadlock. **This de-risks the whole approach** (the §8
  open question: do the ~5 per-conn goroutines actually go durably-blocked under synctest?). Ship the
  optional `WithRand` here too if convenient.
- **G11b — in-memory faulted transports + `dstCluster` harness.** A fault-injecting wrapper over the
  in-memory transports (drop/dup/delay/reorder/partition), seeded by a per-seed `*rand.Rand`; an
  in-process `RoomServiceClient` (gateway→owner) over the same; a `dstCluster` that wires N gateways +
  M owners + K client drivers + shared coord/logs, all inside one synctest bubble. No chaos yet — just
  the wiring + a clean multi-node happy path under `synctest.Wait`.
- **G11c — the chaos suite (Phase-1 exit gate).** Seeded workload (presenters commit, watchers read,
  some broadcast) + the §5 fault schedule (incl. kill owner/gateway, partition, lease expiry); advance
  the fake clock; at `synctest.Wait` quiescence, drain and assert the §6 invariants; sweep seeds 1..N
  with the §6 teeth (assert takeovers/conflicts/dedup-hits/reconnects each `> 0`). Runs with `-race`.

The single-threaded `sim` kernel stays the owner core's DST driver; the full-path suite uses synctest
(the two are complementary — owner bugs still reproduce bit-for-bit in the owner DST). Each PR ships
its own tests; G11c is the milestone.

## 8. Reproducibility & debugging a failing seed

The workload and fault schedule are a pure function of the seed, so a failing seed **re-runs the same
scenario**, and synctest pins **time** (the fake clock is a deterministic function of the durably-
blocked sequence). What synctest does **not** pin is goroutine *interleaving* — so reproduction of a
race-driven failure is *statistical*, not guaranteed on the first replay. The debug loop: re-run the
seed under `-race` (which synctest is designed to be used with) at a tight `-count` until it trips,
with per-event logging keyed by the fake clock. Where a bug is in the owner core (dedup, ordering,
conflict handling) it also reproduces **bit-for-bit** in the existing `sim`-driven owner DST, which
stays the first line of defence. A hard requirement for byte-for-byte replay-debugging would push
toward option A (Dropbox/Trinity-style); this suite's goal — invariants under chaos over thousands of
seeds — does not need it.
