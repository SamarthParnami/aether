# Start Here — Understanding Aether

> *Aether: the rare, fluid element once thought to fill all space and connect everything.*

New to this repo? Read this first. It gives you the **why**, the **how**, and an **abstract mental
model** — enough to understand what Aether is and to know where to look next. It assumes no prior
context. For build commands see [CLAUDE.md](CLAUDE.md); for the exhaustive code map see
[REPO-INDEX.md](REPO-INDEX.md).

---

## The 60-second version

Aether is a **real-time state backbone** for Inclass — the live online-classroom product. Its one
job: keep every participant's screen showing the **same thing** (current slide, who's presenting,
raised hands, mute state…) and keep the class running **even when servers crash, deploy, or a whole
data-center zone fails** — all without the student ever reloading the tab.

The whole design rests on one bet:

> **Reliability comes from *recovery*, not from redundant connections.**
> Each client holds **one disposable WebSocket**. When it drops (for any reason), the client
> reconnects and says "catch me up from where I was." A **per-room append-only log is the single
> source of truth**; every server node and cache is disposable and rebuildable from that log.

Everything else in the codebase is machinery in service of that sentence.

---

## Why we're building it (the problem)

Inclass today runs on a `socket-server`. It's not a dumb relay — it saves classroom state to Redis
and Mongo and scales across instances. But (verified against the real code) it lacks *foundations*
that can't be patched in later:

- **No ordered event log → no content recovery.** When a student's connection blips, mic and
  screen-share come back, but **slides/content don't reliably re-sync** — there's no record of "what
  happened, in what order," so a reconnecting student can silently fall out of sync.
- **No room ownership / clean failover.** Instances scale, but no instance *owns* a class — so when
  one restarts or dies, students are bounced with no coordinated handoff.
- **No first-class admin override.** Control is tutor-only; there's no privileged admin actor.

These are *architectural* gaps. Aether rebuilds the foundation properly: an authoritative per-room
ordered log, sequence-cursor recovery, coordinated ownership + re-homing, and admin override as a
first-class event. (Longer version: [03-brief-leadership.md](03-brief-leadership.md) for plain
language, [01-design-backbone.md §1](01-design-backbone.md) for the deep version.)

---

## The one big idea, expanded

Distributed real-time systems usually try to survive failures by adding **redundancy in the client**
(e.g. a second hot connection). Aether rejects that: a second connection only helps with the *easy*
failure (your edge node dies) and is useless for the *hard* one (the room's owner dies — both
sockets depend on it). And you need reconnect-and-resume logic *anyway* (laptops sleep, wifi
switches). So Aether builds **one** excellent recovery path and routes **every** failure through it:

```
any drop (wifi / sleep / gateway crash / owner death / AZ loss)
        │
        ▼
reconnect  →  "RESUME from lastSeq"  →  server replays the gap from the log  →  live again
```

Because there's one path, it can be **tested exhaustively** — which is the actual Phase-1
deliverable (see "What's built" below).

Two consequences make it work:
1. **"Real" means committed to the log.** The owner does **ack-after-persist**: it doesn't confirm a
   durable event until it's safely in the quorum-replicated log. So a client's cursor can never get
   ahead of the truth, and replay is always lossless.
2. **The room is the atom.** A "room" = one live class: its membership + its shared state + its
   durable log, owned by exactly one node at a time. It's the unit of ownership, ordering, sharding,
   and blast-radius. One bad room can't affect another.

---

## The abstract architecture

```
        ┌───────────┐   ONE WebSocket    ┌──────────────┐      RPC        ┌──────────────┐
        │ browser / │   (ClientMessage / │   GATEWAY    │  (RoomService,  │ ROOM-RUNTIME │
        │  TS SDK   │◀── ServerMessage ─▶│  stateless   │◀─ Connect/gRPC)▶│  the OWNER   │
        └───────────┘   aether.proto     │  terminator  │                 │              │
                                         └──────┬───────┘                 └──────┬───────┘
                                                │ coord.Current(room)            │
                                                ▼  → owner address               ▼
                                          ┌───────────┐              ┌──────────────────────┐
                                          │   coord   │              │ logstore = THE TRUTH │
                                          │ (leases + │              │ (per-room event log) │
                                          │ directory)│              │  + fanout (delivery) │
                                          └───────────┘              └──────────────────────┘
```

Three roles:

- **Gateway** — a stateless WebSocket terminator. It authenticates, figures out *which node owns this
  room*, forwards the client's messages there, and relays events back down. It holds **nothing**
  durable, so losing one only drops live sockets, which reconnect and recover. → code: `gateway`.
- **Room-runtime (the Owner)** — the single writer for a set of rooms. It sequences events, dedups
  replays, persists to the log, and fans out. Stateful, but **fully rebuildable from the log**, so any
  node can take a room over. → code: `roomruntime`.
- **The truth layer** — a per-room, append-only, quorum-replicated **log** (`logstore`), plus a
  **lease + directory** for who-owns-what (`coord`) and a **delivery bus** for fan-out (`fanout`). In
  production these are DynamoDB + Redis; **today they're in-memory implementations behind interfaces.**

### The five things to internalize
1. **One disposable WebSocket per client.** Resilience = recovery, not a 2nd connection.
2. **The per-room log is the ONLY truth.** Everything else is disposable and rebuildable.
3. **"Real" = persisted.** Ack-after-persist ⇒ a client's cursor never exceeds the log ⇒ lossless replay.
4. **Two QoS tiers, visible in the API.** *Durable* (`commit` — slides, mute, admin: ordered, acked,
   never lost) vs *ephemeral* (`broadcast` — cursors, presence: fast, lossy). You can't send one as
   the other.
5. **The room is the atom** — one owner per room, guarded by a lease *and* a conditional write so two
   nodes can never both write. A deploy is a failover, so graceful handoff matters.

---

## How it works, by following the code

### Journey of a durable write (teacher sets "slide = 7")
Trace it through the real functions:

1. The SDK mints a `Commit{room_id, client_seq, body}` and sends it over the one WebSocket.
2. **Gateway** `handleCommit` ([gateway/server.go](go/internal/gateway/server.go)) resolves the room's
   owner via `OwnerLocator.Owner` → `coord.Current(room)` and forwards a `RoomService.Commit` RPC.
3. **Owner** `Runtime.Commit` ([roomruntime/runtime.go](go/internal/roomruntime/runtime.go)):
   - confirms it holds the room's lease (`acquire`),
   - `roomcore.Room.Apply` dedups on `(client_id, client_seq)`, assigns the next `room_seq`, folds the
     event into state,
   - **`logstore.Append`** writes it *conditionally* at that exact `room_seq` — this is the moment it
     becomes real (**ack-after-persist**),
   - then, *outside the lock*, `fanout.Publish` broadcasts it.
4. **Fan-out is the ack.** Every gateway tailing the room (via `Runtime.Tail` →
   `RoomService.Subscribe`) receives the event and pushes it down its sockets. The originating client
   recognizes its own `origin_client_seq` coming back = confirmed. Other clients advance their cursor.

The bright line: everything *before* the append is reversible; everything *after* is just propagation
of an already-durable fact. That's what makes every failure resolve cleanly. (Full version:
[01 §6.1](01-design-backbone.md).)

### Journey of a recovery (the connection drops)
The client reconnects to *any* gateway and sends `Join{room_id, from_seq=lastSeq, session_nonce}`.
- The gateway re-derives the **same** `client_id` from the principal + nonce (`deriveClientID`, an
  HMAC) — so dedup identity survives even though the gateway is stateless and may be a different one.
- It calls `RoomService.Subscribe(from_seq)`; the owner's `Tail`
  ([roomruntime/tail.go](go/internal/roomruntime/tail.go)) **replays the gap from the log** in strict
  order, then continues live. No reload, no dupes (re-applying a seen `room_seq` is a no-op).

### Ownership & failover, plainly
One node owns a room via a **lease** in `coord` (a time-bound token). If the owner dies, its lease
lapses; a surviving node claims it and rebuilds the room by replaying the log. Two independent guards
stop two nodes ever both writing: the lease (soft) **and** the log's conditional `Append`, which
fails the loser of any race (hard). This is proven over 120 seeds of fault injection in
[dst_failover_test.go](go/internal/roomruntime/dst_failover_test.go).

### The two tiers
- **Durable** (`Commit` → `Event`): ordered, deduped, persisted, acked. For state that must be right.
- **Ephemeral** (`Broadcast` → `Ephemeral`): a separate bus and stream, lossy, never persisted, dropped
  first under load. For high-frequency signals (a moving cursor) where latency beats durability.

---

## What's built today vs. the roadmap

Be honest with yourself about the current state — a lot works, but it's all **in-memory and
in-process**, driven by tests, not yet a deployable service:

**Built:** the frozen Protobuf contract; the pure reducer (`roomcore`); in-memory `logstore`/`coord`/
`fanout`; the full owner (`roomruntime`) with the write journey, tailing, and ephemeral tier; the
Connect owner RPC (`ownerrpc`); the client-facing gateway through failover (`gateway`, G1–G10); the
deterministic simulator (`sim`) and the owner-layer chaos sweep; and the Go↔TS reducer parity.

**Not built yet:** any runnable binary (`go/cmd/` is empty); the real DynamoDB/Redis adapters; the
TypeScript client SDK / React hooks / web app; log compaction + deep-resume; the full
client↔gateway↔owner chaos harness; and all infra. (Exhaustive list: [REPO-INDEX.md §8](REPO-INDEX.md).)

The plan is three phases — **1: the backbone** (now — plumbing + a chaos suite that deliberately breaks
things to prove it heals), **2: a thin UI + real state fields on the proven backbone**, **3: real Inclass
features + redundant video (LiveKit/Dyte)**. Reliability is built and proven *first*, so we never debug
"is it the feature or the foundation?" in production.

---

## Where to go next

1. **The briefs** — [03-brief-leadership.md](03-brief-leadership.md) (plain language) and
   [02-brief-engineering.md](02-brief-engineering.md) (one-page eng).
2. **The deep design** — [01-design-backbone.md](01-design-backbone.md): the full architecture, the
   "what happens if X dies" FAQ (16 failure scenarios), and the availability model. Then
   [05-design-gateway.md](05-design-gateway.md) for the gateway/RPC design, and
   [04-phase1-implementation-plan.md](04-phase1-implementation-plan.md) for the build plan.
3. **The code** — read in dependency order: `roomcore` (pure logic) → `roomruntime` (the owner) →
   `gateway`. Then [REPO-INDEX.md](REPO-INDEX.md) as your map.
4. **Run it** — `task test` runs the whole Go suite with the race detector; the chaos sweep is
   `go test -run TestDST_FailoverConvergence ./internal/roomruntime/`. Watching those pass is the
   fastest way to trust the design.

## Mini-glossary

| Term | Meaning |
|------|---------|
| **Room** | One live class session: its membership + shared state + durable log. The unit of ownership/ordering/blast-radius. |
| **Gateway** | Stateless WebSocket terminator. Routes to the owner, relays fan-out down. Holds nothing durable. |
| **Owner / room-runtime** | The single writer for a room. Sequences, persists, fans out. Rebuildable from the log. |
| **`room_seq`** | The authoritative, monotonic per-room sequence number — the truth's ordering. |
| **`lastSeq` / cursor** | The highest `room_seq` a client has applied. The whole basis of recovery. |
| **Ack-after-persist** | The owner confirms a durable event only after it's in the log ⇒ replay is lossless. |
| **Fan-out is the ack** | A committed event returning via the client's subscription *is* its confirmation (matched by `origin_client_seq`). |
| **Re-homing** | A surviving node claiming a dead owner's room and rebuilding it from the log. |
| **Lease** | The ownership token (one owner per room). Fail-safe: ambiguity → freeze, never split-brain. |
| **Durable vs ephemeral** | `commit` (ordered/acked/never-lost) vs `broadcast` (fast/lossy). Schema-enforced. |
| **DST** | Deterministic Simulation Testing — replay the whole cluster from a seed to inject and reproduce faults. |
</content>
