# Aether — Real-Time State Backbone (Design & Phase 1)

> *The rare, fluid element once thought to fill all space and connect everything.*

**Status:** Design / pre-build · **Audience:** Engineering (deep) · **Scope:** whole-system context + **Phase 1 in depth**

---

## 1. Why we're doing this

Inclass today spans three repos (tutor `lms-frontend`, student `frontend`, `socket-server`). The
current `socket-server` is **more than a dumb relay** (a claim worth getting right): it persists
classroom state to Redis (`class:{classId}`) and checkpoints to MongoDB (~every 5 min), runs
**socket.io with the Redis Streams adapter** so instances scale horizontally and fan out via Redis, and
enforces a **"tutor-in-control"** check on state changes. So the problem is *not* "it has no memory."

The problem is that it lacks the **foundation** for the guarantees we now need (verified against the
code, June 2026):

- **No ordered event log / sequence cursor → no content recovery.** On reconnect, mic and screen-share
  state *are* restored from Redis (`socket-server/index.js:654-731`), but **slides/content are not
  replayed** — there are no sequence numbers, no event log, no resync (no such mechanism in the codebase;
  `frontend/.../useSocketConnection.ts:116-129` just logs the reconnect). A client that misses events can
  silently fall out of sync on content.
- **No room ownership / leader election / coordinated failover.** Instances scale via the Redis adapter,
  but no instance *owns* a class (no election/owner/failover logic in `socket-server`). An instance dying
  is an **uncoordinated client reconnect to any instance**, not a clean handoff — and because content
  isn't resynced, that reconnect can lose content state.
- **No first-class admin override.** Only 4 user types — `STUDENT/GUARDIAN/TUTOR/RECORDER`
  (`socket-server/constants/user.js`); control is **tutor-only**, with no privileged admin actor at the
  protocol level.
- **Push-only model**, tutor → students (`htmlSlidesEve`/`CLASS_SYNC`); no authoritative, reconcilable
  state a client can pull or rebuild from.

Aether rebuilds this with the missing foundation: an **authoritative per-room ordered log**,
**sequence-cursor recovery**, **room ownership with coordinated re-homing**, and **admin override as a
first-class event** — so the failures the current system handles ad-hoc (or not at all) become uniform,
provable, and tested.

Aether rebuilds this as a principled, highly-available backbone. The goals, in priority order:

1. **No-reload resilience.** A client survives socket drops, server failover, and AZ loss without ever
   reloading the tab. The UI reacts only to an abstraction that hides reconnects.
2. **Authoritative shared state.** One consistent picture every participant sees (current slide,
   presenter, raised hands, mute, …), with ordering, audit, and **admin override** as first-class.
3. **Redundant backends.** Many interchangeable servers across AZs; rooms re-home automatically on
   failure. Redundant **media** providers (LiveKit ↔ Dyte) behind one abstraction.
4. **Testable & regression-free.** Fault injection is a first-class *test input*, not an afterthought.

### Non-goals (deliberately out of scope)
- Cross-region / multi-cloud active-active. (See §7 — single region, multi-AZ is the chosen posture.)
- Hard DR failover (cross-region standby, ECS-as-backup-to-EKS). Explicitly **not** built.
- Phase-1 does **not** ship UI, features, or live media — it ships the plumbing.

---

## 2. The phased plan (brief)

| Phase | Goal | Deliverable |
|-------|------|-------------|
| **1 — Backbone** | The plumbing between browsers and servers. Redundant servers, the durable per-room log, recovery, and a chaos test harness. **No UI, no features, no media.** | Kill any component; SDK clients stay convergent with **no reload, no dupes, no lost durable events**. The chaos suite passing **is** the deliverable. |
| **2 — UI + plumbing** | A thin app that reads/writes a few *real* state fields through the Phase-1 SDK, with persistence. Admin override becomes a real event type. | Human-in-the-loop demo of end-to-end state sharing + override + recovery. |
| **3 — Features into Inclass** | Map real Inclass features onto the proven backbone; wire LiveKit/Dyte behind the media abstraction, signalled over the durable channel. | Inclass running on Aether. |

The phasing derisks the hard part first: Phase 1 proves resilience in isolation, and the contract frozen
in Phase 1–2 (`protocol/`) makes Phase 3 mostly *mapping*, not *reinventing*.

---

## 3. Architecture at a glance

```
        ┌─ CloudFront (India edges) ── static app/SDK bundle
        │
browser ─┴─ ONE WebSocket ─▶ gateway pods (stateless, multi-AZ)   ◀ scale on connections
                                   │  inbound = directed to owner
                                   ▼
                            room-runtime pods (the OWNER, multi-AZ) ◀ scale on rooms
                                   │
              ┌────────────────────┼─────────────────────┐
              ▼                    ▼                     ▼
   Redis (coord/lease)   Redis (stream + ephem)   DynamoDB (per-room log)
   ElastiCache Multi-AZ  ElastiCache Multi-AZ     regional, multi-AZ quorum = TRUTH
                                                         │ snapshots / cold history
                                                         ▼  (+ DynamoDB Streams → datalake/CDC later)
   media (LiveKit/Dyte, Indian SFU POPs) ── signalled OVER the durable channel, separate plane
```

**The spine, in one paragraph:** Each client holds exactly **one disposable WebSocket**. The server is
many interchangeable nodes. The only source of truth is a **per-room, append-only, quorum-replicated
log** (DynamoDB). Everything fast (socket nodes, Redis) is disposable and rebuildable from the log.
A client survives any failure by **recovery** — it tracks the last sequence number it saw (`lastSeq`)
and, on reconnect to *any* node, says "catch me up from N." Redundancy lives in **many nodes across AZs
+ fast re-homing**, never in a second connection.

---

## 4. Core concepts (glossary)

- **Room** — one live class session: its membership + its shared state + its durable event log, treated
  as one atomic, independently-owned thing. **The room is the unit of ownership, ordering, sharding,
  and blast-radius.** A bad/busy room cannot affect another room.
- **Gateway** — stateless WebSocket terminator. Authenticates, authorizes, routes to the owner, relays
  fan-out back down. Holds **nothing** durable. Disposable.
- **Room-runtime (Owner)** — the single writer for a set of rooms. Sequences events, dedups, persists,
  fans out, snapshots. Stateful but **fully rebuildable from the log**.
- **Durable tier** — ordered, authoritative events (slide, mute, presenter, admin override). Go through
  the full journey: sequence → quorum-persist → ack. Never lossy.
- **Ephemeral tier** — high-frequency, loss-tolerant state (cursors, presence, "typing…"). Fire-and-forget.
- **`roomSeq`** — the authoritative, monotonic per-room sequence number. The truth's ordering.
- **`lastSeq`** — the highest `roomSeq` a client has applied. The entire basis of recovery.
- **Ack-after-persist** — the owner does not ack a durable event to the client until a **quorum** of log
  replicas has it. Makes a client's `lastSeq` never exceed the durable log → recovery is lossless.
- **Re-homing** — when an owner (or AZ) dies, a surviving node claims the room's lease and rebuilds it
  from snapshot + log. Automatic, in-region. The recovery spine — **not** a "DR failover."
- **Lease** — the ownership token (one owner per room). Fail-safe: ambiguity → **freeze**, never split-brain.

---

## 5. The stack & the monorepo

**Stack (decided):** **Go** backend (`gateway` + `room-runtime`), **React + TypeScript** frontend, a
**TypeScript SDK**, and a **Protobuf contract** between them (tooled with [Buf](https://buf.build)). It's a
**polyglot monorepo** — Go services + a TS workspace + a language-neutral `proto/` spine. *(Build details,
tooling, and the incremental PR plan live in [04-phase1-implementation-plan.md](04-phase1-implementation-plan.md);
this section is the conceptual map.)*

The contract being **language-neutral and machine-enforced** is the structural win: `proto/` generates
both Go structs and TS types, and **`buf breaking` fails CI on any backward-incompatible change** — so the
"freeze the contract" guarantee holds across two languages, in one PR.

```
proto/             # Protobuf contract — THE spine. Wire envelopes + event catalog. Generates Go + TS.
                   #   buf lint + buf breaking gate every change. Depended on by everything.
go/                # Go module workspace (go.work)
  internal/roomcore/   # pure room logic: fold, seq, dedup, snapshot/replay. NO I/O. Most-tested package.
  internal/logstore/   # durable-log iface + impls (dynamo / inmem-for-tests). conditional-write guard.
  internal/coord/      # lease + room→owner directory iface + impls. fail-safe.
  internal/fanout/     # redis stream/pubsub + direct-push degrade
  internal/sim/        # deterministic simulation harness (injected clock/net/rng)
  cmd/gateway/         # stateless WS terminator
  cmd/roomruntime/     # the owner service
packages/          # TS workspace (Yarn 4 + Turborepo)
  protocol/        # generated TS types + golden-vector conformance runner
  client-sdk/      # the browser abstraction: Connection, commit/broadcast, seq cursor, resume,
                   #   optimistic apply + rollback. Hides ALL recovery machinery.
  react/           # (Phase 2) hooks
apps/web/          # (Phase 2) React app
test/chaos/        # DST scenarios + Go↔TS e2e harness
infra/             # terraform + Devtron configs, versioned atomically with the code
```

**Hard rules (linted):** everything depends on `proto/`; nothing depends "up." **The SDK / browser packages
must not import server code** (enforced by package layout + module-boundary lint). The Go side enforces its
own boundaries (`roomcore` has no I/O imports).

**Reducer parity without a shared function.** In an all-TS design the client and server would import *one*
reducer. Polyglot, we can't — so the shared artifact is a set of **golden test vectors**: protobuf-encoded
`(initialState, events) → expectedState` fixtures that **both** the Go `roomcore` fold and the TS SDK
reducer must pass in CI. That's the cross-language guard against the worst bug class in these systems
(silent client/server state divergence).

**The SDK surface** (the two tiers are visible, so misuse is impossible):
```ts
const room = await client.join(roomId);
room.connectionState;                 // 'connecting'|'live'|'reconnecting'|'frozen'  — UI binds HERE
await room.commit({ type:'SLIDE_SET', payload:{ index:7 } });  // durable: resolves on ack, rejects on NACK
room.broadcast({ type:'CURSOR', payload:{ x,y } });            // ephemeral: fire-and-forget
room.state;                           // materialized state, maintained by the TS reducer (golden-vector-checked)
room.on('state', render);             // fires on every confirmed change
```

---

## 6. Phase 1 in depth

Phase 1 builds: `proto/` (contract), Go `roomcore` / `logstore` / `coord` / `fanout` / `sim`,
`cmd/gateway`, `cmd/roomruntime`, the TS `client-sdk` (durable path), and `test/chaos`. No `web`, no
features, no media. *(The PR-by-PR sequence is in [04](04-phase1-implementation-plan.md) §5.)*

### 6.1 The journey of a write (durable event — e.g. teacher sets slide 7)

```
0. client: mint event {clientId, clientSeq:42, type:SLIDE_SET, payload:{index:7}}
           → apply OPTIMISTICALLY (mark pending) → buffer outbound until acked
1. → ONE WebSocket → gateway G1
2. G1: authn + authz (role check; admin override = privileged + audited)
3. G1: directory lookup "who owns room R?" → R5 → directed send  (INBOUND = point-to-point)
4. R5: dedup on (clientId, clientSeq) → assign roomSeq 416
5. R5: APPEND to DynamoDB log, AWAIT QUORUM            ◀── the ONLY moment the event becomes "real"
6. R5: XADD room:R:log (Redis hot tail)  + update in-mem state   (accelerator; skippable if Redis down)
7. fan-out: every gateway tailing room:R:log gets 416  (OUTBOUND = pub/sub, one→many)
8. each gateway pushes 416 down its local sockets:
     - originating client: sees clientSeq 42 confirmed as 416 → clear pending   (fan-out IS the ack)
     - other clients: gap-check (415→416 ok) → apply → lastSeq=416
```

**The bright line:** everything *before* step 5 is reversible/replayable; everything *after* is just
propagation of an already-durable fact. "Real iff quorum-committed" is what makes every failure below
resolve to exactly-once, no-divergence, no-reload.

The **ephemeral path** (cursor) skips steps 4–6 entirely: owner → `PUBLISH room:R:ephem` → fan-out, lossy.

### 6.2 Fan-out (pub/sub) — the key asymmetry

- **Inbound (client→owner): point-to-point.** One consumer (the owner). Gateway routes directly via the
  owner directory. *Not* pub/sub.
- **Outbound (owner→clients): pub/sub fan-out.** Many consumers (every gateway holding a room member).
  Owner publishes once to the room's channel; gateways subscribed-by-local-membership do the final WS push.
  The owner never knows the gateway topology — that decoupling is the point.

The durable channel is a **Redis Stream**, so **fan-out, ack, and resume are one mechanism** read from
different positions: live tail (`XREAD BLOCK`), ack (the fanned-out event carries `clientSeq`), resume
(`XRANGE` from `lastSeq`). Dynamo underneath is the deep truth when the stream window isn't enough.

> **Live fan-out is always PUSH, never poll.** Normal: Redis Stream tail. Redis down: the owner
> **direct-pushes from its in-memory membership** (it already has the event and knows each member's
> gateway). Dynamo is read **event-driven** (resume / cold-start) only — never in a polling loop.
> This is what lets fan-out scale to 100k rooms (see §9).

### 6.3 Recovery mechanics (the no-reload guarantee)

Every drop — edge-node death, owner failover, wifi, sleep — is handled identically by the SDK:
`reconnect → RESUME{roomId, lastSeq} → catch up (stream tail, or Dynamo snapshot+replay if the gap is
deep) → live`. The UI never sees it.

Non-negotiables that make this work:
1. **`lastSeq` cursor** on every client — the basis of resume.
2. **Client-generated event IDs + server dedup** — on reconnect the client replays unacked outbound
   intents; the owner dedups the ones that landed. Prevents double-apply.
3. **Snapshot + replay tail** — owner answers "catch me up from N" fast.
4. **Ack-after-persist** for durable events — a crash/failover never leaves a client ahead of the log.

### 6.4 Ownership, leases & re-homing

One owner per room, held via a **lease** (renew ~2s, expire ~6s). Owner death → lease expires → a
surviving node **CAS-claims** it → rebuilds from snapshot + log → live. The ~6s detection window is the
**failover floor** and the primary SLA dial (lower = faster failover but more false-positives under
GC/jitter). Two independent guards against split-brain:
- **Lease fail-safe:** ambiguity → freeze (refuse writes), never assume ownership.
- **Conditional write on the log:** `append` is conditional on `(roomId, roomSeq)` not existing. Two
  writers racing → one's write fails → it *discovers* it lost ownership. The durable write doubles as
  the single-writer guard, so correctness doesn't rest on the lease alone.

### 6.5 Infra & deployment (Phase 1)

- **Single cloud, single region: AWS Mumbai (`ap-south-1`), 3 AZs.** Serves all-India at <100ms; also
  satisfies DPDP data-residency for minors' data. (Rationale & rejected alternatives: §7.)
- **EKS**, pods spread **hard across AZs** (`topologySpreadConstraints` `DoNotSchedule` on zone) +
  pod anti-affinity. Karpenter NodePools must span all 3 AZs' subnets, with **N-1 AZ headroom** so a
  dead AZ's re-homed rooms have somewhere to land.
- **Truth:** DynamoDB (`partitionKey=roomId`, `sortKey=roomSeq`), regional multi-AZ quorum, PITR,
  append-only. **Redis split by role** (coord/lease · stream+ephem) on ElastiCache Multi-AZ; use
  **sharded pub/sub** (`SSUBSCRIBE`) so fan-out scales with subscribers, not cluster size.
- **Devtron** (k8s manager). `gateway` = web-shaped template; `room-runtime` = **worker-shaped
  `rolling.yaml`** (like the ai-tutor agent). **Every room-runtime rollout is a deliberate re-homing
  event** → graceful `preStop` (stop writes → snapshot → **release lease** so survivors claim instantly,
  not after TTL) + long `terminationGracePeriod` + slow rollout. Gateway drains connections on SIGTERM.
  - ⚠️ **Known Devtron traps — must fix:** `minAvailable:""` silently breaks PDBs (for HA, real PDBs are
    mandatory); `0.2Gi`→milli-bytes resource bug (use `512Mi`/`1Gi`). *(Go services ship as a static binary,
    so the prior TS `tsc`-at-startup OOM trap doesn't apply — but keep startup fast regardless: slow startup
    lengthens every failover.)*
  - Beta + stable room-runtime apps with named-dispatch isolation enable dark-deploying a new owner build
    against a few real rooms before promotion.

### 6.6 What "done" means for Phase 1

The **chaos harness (`test/chaos`)** is the deliverable — a Go deterministic-simulation suite (seeded
clock/net/rng) plus a Go↔TS e2e rig. It spins up gateways + room-runtimes + fake SDK clients and injects
faults, asserting **convergence + no-reload + no-dupes + no-lost-durable-events + at-most-one-owner**:
- kill a gateway · kill the owner mid-write · kill an entire AZ · partition gateway↔owner ·
  wipe Redis (stream + coord) · simulate quorum loss · trigger a reconnect storm · roll a deploy.

Because *a deploy is a failover* in this design, "kill the owner" and "deploy the owner" are the **same
test** — Phase 1 validates deploy-safety and failure-safety with one suite.

---

## 7. Locked decisions & rationale

| # | Decision | Why | Rejected alternative |
|---|----------|-----|----------------------|
| D1 | **Single WebSocket per client; resilience via recovery** | A second connection only speeds detection of the *easy* failure (edge node) and is useless for the *hard* one (owner death — both sockets depend on the same owner). You need `lastSeq` resume anyway (sleep/wifi). | Dual hot-standby connections |
| D2 | **Per-room quorum log (DynamoDB) is the only truth; everything else disposable** | Concentrates all correctness in one bulletproof layer; makes every "what if X dies" answer "rebuild from the log." | Trusting Redis / socket-node memory as truth |
| D3 | **DynamoDB over Kinesis for the log** | Our shape is "thousands of independent per-room ordered logs you seek into by `(roomId, seq)`" — Dynamo's home turf. Native per-room seek; conditional write = free split-brain guard. Kinesis orders by *shard* (many rooms interleaved), can't seek per-room, doesn't enforce single-writer. | Kinesis as truth (kept as optional CDC tap) |
| D4 | **Redis = accelerator, never load-bearing for correctness** | Replication copies logical/config corruption (it doesn't save you from a bad `maxmemory` or `FLUSHALL`). So Redis holds nothing unrecoverable; failure = slow, not wrong. Split by role for blast-radius isolation; coordination fails safe. | Redis as store-of-record |
| D5 | **Ack-after-persist for durable events** | Client `lastSeq` never exceeds the durable log → recovery is provably lossless across owner failover. Costs one quorum round-trip — invisible for low-frequency control events. | Ack-fast / persist-async (risks client ahead of server) |
| D6 | **Single cloud, single region (Mumbai), multi-AZ. No cross-region, no multi-cloud, no ECS-backup** | Multi-AZ targets ~99.9–99.95% (derived in §12, not asserted) and covers every realistic failure. All-India ≠ global: one India region serves the country at <100ms + satisfies DPDP. Multi-region adds an untested failover path + RPO>0 for a rare regional-outage risk we consciously accept. Multi-cloud insures the rarest failure at the highest cost and *reduces* real reliability. ECS-as-backup only covers EKS-control-plane (which rarely drops running pods) and shares the same data layer anyway. | Cross-region active-active, multi-cloud, ECS standby |
| D7 | **Keep the portability seam, don't build the room** | `log-adapter` interface + "home location" as a first-class concept + k8s = we *could* add Hyderabad (`ap-south-2`, in-country, DPDP-safe) warm-standby or migrate clouds later, as config — without paying to run it now. | Hard-coding AWS-isms everywhere |

---

## 8. FAQ — "What happens if…" (Phase 1 backbone, quantitative)

> **Read the probabilities as planning-grade, order-of-magnitude estimates** for the system at scale
> (assume ~100k concurrent rooms), not measured SLAs. "Per year" = expected occurrences across the
> whole fleet. They exist to size effort and set expectations, and should be replaced with measured
> numbers once we have telemetry. **In every case the invariant is the same: no reload, no lost durable
> event, no divergence — at worst a brief, correct freeze.**

### Common (the system is *designed* around these — they happen constantly)

**Q1. A client's connection drops (wifi switch, laptop sleep, tunnel)?**
- *Likelihood:* **Very High** — ~1–5% of sessions see at least one blip; thousands/day at scale.
- *What happens:* SDK reconnects to any gateway → `RESUME{lastSeq}` → catch up → live. UI never reacts.
- *Mitigation:* `lastSeq` resume; jittered backoff. **Impact: none** (sub-second, invisible).

**Q2. A gateway pod restarts or crashes (deploy, OOM, node recycle)?**
- *Likelihood:* **Very High** — every deploy (several/week) + occasional crashes. ~100s/year/fleet.
- *What happens:* its clients' sockets drop → reconnect to a sibling gateway → resume. Room truth
  untouched (it lives on the owner + log).
- *Mitigation:* stateless gateways; SIGTERM connection-drain. **Impact: none** (a ripple of reconnects).

**Q3. A client re-sends an in-flight action on reconnect (duplicate)?**
- *Likelihood:* **High** — happens on most reconnects, by design.
- *What happens:* owner dedups on `(clientId, clientSeq)` → re-acks, does **not** re-apply.
- *Mitigation:* client-generated IDs + server dedup. **Impact: none** (no double slide-skip / double hand-raise).

**Q4. Events arrive out of order / a client misses one (gap)?**
- *Likelihood:* **High** at the mechanism level (any reconnect).
- *What happens:* receiver sees a `roomSeq` gap (at 413, gets 416) → triggers `RESUME` instead of applying
  out of order.
- *Mitigation:* monotonic `roomSeq` + gap detection. **Impact: none**.

### Occasional (handled automatically; brief, bounded effect)

**Q5. The room's OWNER pod dies (deploy or crash)?**
- *Likelihood:* **High** — every room-runtime deploy is a controlled mass re-homing; crashes ~monthly/fleet.
- *What happens:* lease expires (graceful `preStop` makes this near-instant on deploys) → a survivor
  CAS-claims it → rebuilds from snapshot + log → queued clients resume. In-flight intents replayed + deduped.
- *Mitigation:* graceful lease-release on `preStop`; fast rebuild (<1s) via snapshots; conditional-write guard.
- *Impact:* **a freeze of ~the detection window** (target <1s on graceful, up to ~6s on hard crash) for *that
  room only*. No reload, no lost durable event.

**Q6. A Redis node fails (stream/ephem or coord)?**
- *Likelihood:* **Medium** — managed failover a few times/year.
- *What happens:* ElastiCache Multi-AZ promotes a replica. Briefly: live fan-out degrades to **owner
  direct-push from memory**; resume serves from Dynamo. Coord blips → affected rooms freeze (fail-safe),
  not split-brain.
- *Mitigation:* Multi-AZ Redis; degrade-to-direct-push; fail-safe leases. **Impact:** seconds of slower
  fan-out for some rooms; correct throughout.

**Q7. Network partition between a gateway and the owner?**
- *Likelihood:* **Medium** — transient, ~monthly/fleet.
- *What happens:* gateway can't reach the owner → treats it like an owner-loss for routing → clients
  on that gateway reconnect/resume elsewhere; owner's lease logic resolves authority.
- *Mitigation:* directory re-lookup; fail-safe leases. **Impact:** brief freeze for affected clients.

**Q8. The Redis hot-tail window is too short for a deep resume?**
- *Likelihood:* **Medium** — long disconnects / cold owners.
- *What happens:* resume falls through to **Dynamo snapshot + replay**. Slower (a few reads) but correct.
- *Mitigation:* tuned snapshot cadence; per-room single-`Query` reads. **Impact:** slightly slower catch-up.

### Rare (consciously planned for; degraded-but-correct)

**Q9. An entire AZ goes down?**
- *Likelihood:* **Low** — partial AZ events ~1–2×/year industry; full AZ loss ~once/1–2yr per AZ.
- *What happens:* that AZ's gateways' clients reconnect to the other 2 AZs; its owners' rooms (~1/3 of
  fleet) lose their lease → re-home to survivors → rebuild from Dynamo (regional, unaffected). A
  **correlated reconnect storm** follows (see Q11).
- *Mitigation:* hard AZ pod-spread; **N-1 AZ headroom** so re-homed load has somewhere to land; Dynamo &
  Redis are Multi-AZ. **Impact:** ~1/3 of rooms freeze for a re-home window; no reload, RPO-0.

**Q10. Redis suffers a *logical/config* failure (bad `maxmemory`, poison value, accidental `FLUSHALL`)?**
- *Likelihood:* **Low–Medium** — depends on discipline. With config-as-code + staged rollout: ~rare;
  without: ~1–2×/year. (Replication does **not** protect against this — it copies the badness.)
- *What happens:* circuit-breaker trips → **degrade-to-direct-push + Dynamo-served resume**. Truth (Dynamo)
  is untouched.
- *Mitigation:* role-split Redis (small blast radius); config-as-code; eviction **disabled** on the stream
  instance (full = alarm, not silent loss); alarm on eviction/keyspace-miss. **Impact:** slower, correct.

**Q11. A correlated reconnect storm (e.g. triggered by Q9)?**
- *Likelihood:* **Low** — tied to AZ-loss / large deploys.
- *What happens:* tens of thousands of clients resume at once → a one-shot Dynamo read burst.
- *Mitigation:* **snapshots** make resume one `Query`/room (not per-event); **jittered backoff** spreads
  the herd; Redis hot-tail absorbs most; Dynamo on-demand absorbs the rest. **Impact:** a few seconds of
  elevated latency; bounded, not a meltdown. *(This is the scenario the §6.2 "never poll" rule exists for.)*

**Q12. DynamoDB has a regional incident (the truth layer)?**
- *Likelihood:* **Low** — a notable regional event ~1–2×/year industry; a severe one ~once/2–3yr.
- *What happens:* the durable stream is **CP — it freezes** (refuses new durable writes) rather than
  diverge. **Media + ephemeral keep flowing** (people still see/hear each other, cursors move); control
  state goes read-only; everything catches up on recovery. This is the **regional-outage risk we
  explicitly accept** (D6) — Inclass is degraded for the incident's duration (historically minutes–hours).
- *Mitigation:* append-only immutability (tiny corruption surface) + PITR; the seam to add Hyderabad
  warm-standby later if the SLA ever demands it. **Impact:** control read-only for the duration; no divergence.

### Very rare (near-eliminated by design)

**Q13. Two nodes both think they own a room (split-brain)?**
- *Likelihood:* **Very Low** — target <0.001% of re-homes; near-zero.
- *What happens:* even if a lease race occurs, the **conditional write** on `(roomId, roomSeq)` lets only
  one append succeed; the loser's write fails and it stands down.
- *Mitigation:* fail-safe lease **and** conditional-write guard (two independent defenses). **Impact:** none
  in practice — at worst one rejected write that the client replays.

**Q14. Quorum is lost on the log (majority of Dynamo replicas)?**
- *Likelihood:* **Very Low** — effectively AWS-internal; visible <once/3yr.
- *What happens:* same as Q12 — durable writes freeze (CP), no divergence, resume on recovery.
- *Mitigation:* managed quorum; CP freeze. **Impact:** as Q12.

**Q15. A snapshot is corrupt / replay fails on rebuild?**
- *Likelihood:* **Very Low**.
- *What happens:* fall back to an **older snapshot + longer replay** from the append-only log (history is
  immutable, so a full replay is always possible, just slower).
- *Mitigation:* keep N recent snapshots; the log is the ultimate fallback. **Impact:** slower rebuild for
  that room.

**Q16. Clock skew breaks lease timing?**
- *Likelihood:* **Very Low** — NTP-managed nodes.
- *What happens:* worst case a premature/late failover (a freeze or a brief double-claim, caught by Q13's
  conditional write).
- *Mitigation:* monotonic-clock lease math where possible; conditional-write backstop. **Impact:** at worst
  a brief freeze.

### FAQ summary

| Band | Scenarios | Worst-case impact |
|------|-----------|-------------------|
| Common | Q1–Q4 | **None** — invisible, by design |
| Occasional | Q5–Q8 | Brief per-room freeze (sub-second to seconds), correct |
| Rare | Q9–Q12 | Bounded freeze / degraded-but-correct; Q12 = accepted regional risk |
| Very rare | Q13–Q16 | Near-zero; correctness preserved by design |

**The invariant across all 16:** never a reload, never a lost durable event, never divergence — at worst a
brief, correct freeze while the system recovers. That is the entire bet of the architecture.

---

## 9. Capacity sanity-check (≈100k concurrent rooms)

Classroom pace → durable control events are **low-frequency** (~1 event / 10s / room):
- **Durable events:** ~10k/s globally → ~10k Dynamo writes/s (trivial for Dynamo).
- **Fan-out:** ~10k × ~30 participants ≈ **~300k client-pushes/s** — sharded Redis Streams + a gateway
  fleet doing local WS writes; distributes cleanly.
- **Ephemeral (cursors):** higher rate but lossy + throttle/sample-able, rides sharded pub/sub, **never
  touches Dynamo**.
- **Sharding:** rooms spread across many owners (hundreds–thousands each) + sharded pub/sub.
- **Truth store:** writes-once + **event-driven reads** (resume/cold-start), never a poll loop. The only
  burst is correlated reconnect (Q11), smoothed by snapshots + jitter.

100k holds because the hot path is **push + low-frequency durable events + sharding**, and the truth store
is never polled.

---

## 10. Testing strategy

- **Unit** (the bulk): `room-core`, `protocol` reducer, SDK logic — pure, deterministic, I/O-free. The hard
  logic is tested in isolation. Property test: any permutation/replay of an event sequence → same state.
- **Integration:** `log-adapter` / `coordination` against Dynamo-local + Redis in containers.
- **Chaos / e2e:** the §6.6 suite — fault injection as a first-class test input. **This is the regression
  guard for the whole system**, run in CI; a PR that breaks recovery fails before merge.

---

## 11. Open questions (next decisions)

1. **Sharding model** — exact mapping of 100k rooms → owners → Redis shards, and how a gateway resolves
   "which owner / which shard for room R" (the directory's design).
2. **Room granularity** — is a room always 1:1 with a class session? Breakout groups = sub-rooms or
   sub-state? Persistent weekly class = one long log or fresh log per session? (Drives log size & compaction.)
3. **Snapshot cadence / max replay-tail** — the knob that makes re-homing fast; pin a target.
4. **Lease detection window** — the SLA dial (failover speed vs false-positive failovers). Pick a number.
5. **Admin-override semantics** — force-a-value vs revoke-a-permission (different event types & audit).
6. **`protocol/` envelope** — the concrete wire format + reducer signature. Freezing this unblocks parallel
   client/server work; it's the natural first build artifact.

---

## 12. Appendix — Availability model (deriving the ~99.9–99.95% target)

> This replaces the asserted heuristic with a *derivation*. It is still an **estimate** — built from AWS
> published SLAs and **assumed** failover windows, not measured data — but the number is now reasoned, not
> guessed. AWS SLAs are financial-credit thresholds, not guarantees. **Replace with measured availability
> once telemetry exists; the single biggest accuracy upgrade is cross-checking the failover windows and
> operational overhead below against our own incident history (Grafana/PagerDuty).**

**Method.** "Availability" = fraction of time the system can serve durable reads/writes correctly. We take
the ceiling from hard critical-path dependencies, subtract our own failover/operational overhead, and
account for the rare full-region outage **separately** (it's the explicitly accepted tail risk, D6 / Q12).

**Critical-path hard dependencies** (a regional degradation of the truth store forces a CP freeze):

| Component | Published SLA (single region, multi-AZ) | ~Downtime/yr | On the correctness path? |
|-----------|------------------------------------------|--------------|--------------------------|
| **DynamoDB** (truth) | 99.99% | ~52 min | **Yes — binding ceiling** |
| ElastiCache Multi-AZ | 99.99% | ~52 min | No — degrade-to-direct-push (latency, not correctness) |
| EKS control plane | 99.95% | ~4.4 hr | No — running pods survive a control-plane blip (affects deploys/scaling) |

DynamoDB is the binding ceiling: **~99.99% (~52 min/yr)**. Redis and the EKS control plane don't cleanly
multiply in, because the design **degrades around them** — they cost latency and operations, not
correctness-downtime.

**Our own overhead (subtract from the ceiling) — these are the assumed numbers to validate:**
- **Owner failovers** (deploys ~weekly + crashes): per-room freeze <1s graceful, ~6s hard crash.
  ~50–80 transitions/room/yr × ~1–6s ≈ **~1–8 min/yr/room**.
- **AZ loss** (~once/1–2yr): ~1/3 of rooms freeze for a re-home + reconnect-storm window (~30–60s).
  Fleet-averaged ≈ **~15–30s/yr**; an unlucky room in the affected third sees ~30–60s during the event.
- **Operational reality** (bad deploys, config errors, the long tail in no SLA): empirically the *dominant*
  real-world factor — budget **a few × 10 min/yr**.

**Composite (a normal year, excluding regional outage):**
> ~99.99% dependency ceiling − our failover/operational overhead ≈ **~99.9–99.95% realized**
> (~4.4–8.8 hr/yr), almost all of it **brief per-room freezes**, not total outage. The heuristic holds —
> but now bounded: *ceiling = Dynamo (~99.99%); realized = dragged to ~99.9–99.95% by our failover windows
> + operational reality.*

**The tail we exclude (accepted risk, D6 / Q12).** A full Mumbai-region outage. A single multi-hour event
(say 2–4 hr) **consumes an entire year's 99.95% budget by itself**. So: in years *without* a regional
outage, ~99.9–99.95% is realistic; a regional-outage year busts the budget. That is precisely the risk we
chose not to insure against (no cross-region) — and the honest reason the headline is a **band, not a
promise**.
