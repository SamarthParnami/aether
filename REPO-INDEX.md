# Aether — Repository Index

> A map of the repo **as built**, package by package: what each piece does, where it
> lives, its current implementation state, and how it traces back to the design docs.
> For the *why* and a first-timer walkthrough, start with [ONBOARDING.md](ONBOARDING.md).
> For build/test commands and working conventions, see [CLAUDE.md](CLAUDE.md).

**Repo:** `github.com/SamarthParnami/aether` (public) · **Phase:** 1 (backbone) · in active build.

---

## 1. Status snapshot

The design docs (`01`–`05`) describe the whole system; **Phase 1 is being built now**, and a
lot of it exists. Everything runs **in-memory, in-process** today — the durable/Redis adapters
are interfaces with in-memory implementations; the real DynamoDB/Redis impls and the runnable
service binaries are not written yet.

| Layer | Built? | Where |
|-------|--------|-------|
| Protobuf contract (WS envelope + owner RPC + events) | ✅ frozen v1 | [proto/aether/v1/](proto/aether/v1/) |
| `roomcore` — pure reducer, seq, dedup, snapshot, cursor | ✅ | [go/internal/roomcore/](go/internal/roomcore/) |
| `logstore` — durable log iface + **in-memory** impl | ✅ iface + memory · ❌ DynamoDB | [go/internal/logstore/](go/internal/logstore/) |
| `coord` — lease + directory iface + **in-memory** impl | ✅ iface + memory · ❌ DynamoDB | [go/internal/coord/](go/internal/coord/) |
| `fanout` — delivery bus iface + **in-memory** impl | ✅ iface + memory · ❌ Redis | [go/internal/fanout/](go/internal/fanout/) |
| `roomruntime` — the owner (write journey, tail, ephemeral, ownership) | ✅ | [go/internal/roomruntime/](go/internal/roomruntime/) |
| `ownerrpc` — Connect RoomService server over the runtime | ✅ | [go/internal/ownerrpc/](go/internal/ownerrpc/) |
| `gateway` — WS terminator, router, relay, failover | ✅ through G10 (open PR stack) | [go/internal/gateway/](go/internal/gateway/) |
| `sim` — deterministic simulator + fault network | ✅ | [go/internal/sim/](go/internal/sim/) |
| DST failover chaos test | ✅ (owner-layer) | [go/internal/roomruntime/dst_failover_test.go](go/internal/roomruntime/dst_failover_test.go) |
| TS `protocol` — generated types + reducer parity | ✅ | [packages/protocol/](packages/protocol/) |
| TS `client-sdk`, `react`, `apps/web` | ❌ not started | — |
| Service binaries (`cmd/aether-node`) | ❌ not started | [go/cmd/](go/cmd/) is empty |
| Infra (Terraform / Devtron) | ❌ placeholder | [infra/](infra/) is a `.gitkeep` |

**Git state (at time of writing):** `origin/main` is at PR **#32** (owner-side ephemeral, G8b).
The current working branch `feat/gateway-g10-failover` is ~6 commits ahead, carrying an **open,
stacked PR chain**: **#33** ops-worker → **#34** G8c ephemeral relay → **#35** G9 resume →
**#36** G10 failover FROZEN/LIVE. So the *gateway* code described below (resume, ephemeral relay,
failover re-subscribe) reflects the working tree; `main` has the gateway through Commit + owner
ephemeral only.

---

## 2. Directory layout

```
aether/
├── README.md                     # top-level intro + doc table
├── ONBOARDING.md                 # ← start here if new (why/how/abstract)
├── REPO-INDEX.md                 # ← this file (the code map)
├── CLAUDE.md                     # build/test commands + conventions (for agents & devs)
├── 01-design-backbone.md         # deep design: why, phases, Phase-1, "what if X dies" FAQ, SLA model
├── 02-brief-engineering.md       # one-page eng brief
├── 03-brief-leadership.md        # plain-language PM/leadership brief
├── 04-phase1-implementation-plan.md  # the PR-by-PR build plan + tooling/testing choices
├── 05-design-gateway.md          # gateway architecture (the G1–G11 PR plan)
│
├── proto/aether/v1/              # THE contract. buf lint + buf breaking gate every change.
│   ├── aether.proto              #   client↔gateway WebSocket envelope (ClientMessage/ServerMessage)
│   ├── events.proto              #   EventBody/EphemeralBody/RoomState catalog (Phase-1: generic KV)
│   └── owner.proto               #   internal gateway↔owner RPC (RoomService), Go-only
│
├── go/                           # Go workspace (go.work → ./go), one module
│   ├── gen/                      #   generated protobuf (gitignored; `buf generate` builds it)
│   ├── cmd/                      #   service binaries — EMPTY (.gitkeep); no runnable node yet
│   └── internal/
│       ├── protocol/             #   conformance over generated wire types + version const
│       ├── buildinfo/            #   name/version, stamped at release via -ldflags
│       ├── roomcore/             #   PURE room logic — fold, seq, dedup, snapshot, cursor. No I/O.
│       ├── logstore/             #   durable per-room log iface + in-memory impl
│       ├── coord/                #   ownership lease + room→owner directory iface + in-memory impl
│       ├── fanout/               #   outbound delivery bus (durable + ephemeral tiers) + in-memory
│       ├── roomruntime/          #   THE OWNER: wires roomcore+logstore+coord+fanout; write journey
│       ├── ownerrpc/             #   Connect RoomService server wrapping a Runtime
│       ├── gateway/              #   client WS terminator + owner locator/router + relay + failover
│       └── sim/                  #   deterministic discrete-event simulator + fault-injecting network
│
├── packages/                     # TS workspace (Yarn 4 + workspaces)
│   └── protocol/                 #   generated TS types + the reducer (Go↔TS golden-vector parity)
│       └── src/gen/              #   generated protobuf-es (gitignored)
├── apps/                         # (Phase 2) React app — EMPTY (.gitkeep)
├── test/chaos/                   # (Phase-1 exit) full-stack DST — EMPTY (.gitkeep); owner DST is in-package
├── testdata/golden/roomcore.json # cross-language golden vectors (Go fold == TS reducer)
├── infra/                        # (later) Terraform + Devtron — EMPTY (.gitkeep)
│
├── Taskfile.yml                  # unified entrypoint (go + buf + yarn)
├── go.work / go.mod              # Go 1.26; deps: protobuf, connectrpc, coder/websocket, rapid
├── buf.yaml / buf.gen.yaml       # Buf v2; generates Go (protoc-gen-go + connect-go) and TS (es)
├── package.json / yarn.lock      # Yarn 4.5.3, Node ≥22; vitest, eslint, typescript
└── .github/workflows/ci.yml      # proto / go / ts / pr-title lanes
```

---

## 3. The Protobuf contract (`proto/aether/v1/`)

The language-neutral spine. `buf generate` emits Go into `go/gen/` and TS into
`packages/protocol/src/gen/` (both gitignored, built in CI). `buf breaking` fails CI on any
backward-incompatible change, so the contract is frozen across both languages in one gate.

### [aether.proto](proto/aether/v1/aether.proto) — the client ↔ gateway WebSocket envelope
One disposable WS per client, multiplexing rooms (every frame carries `room_id`). Two top-level
oneof frames:
- **`ClientMessage`**: `Join` · `Commit` (durable) · `Broadcast` (ephemeral) · `Leave` · `Ping`.
- **`ServerMessage`**: `Joined` · `Event` (a committed durable event — live *or* replay) ·
  `Ephemeral` · `Nack` · `RoomStatus` (LIVE/FROZEN) · `Error` · `Pong`.

Key fields: `Join.from_seq` (0 = fresh → snapshot; >0 = resume from cursor), `Join.session_nonce`
(the SDK's per-session random used to derive a stable `client_id`); `Event.room_seq` (the
authoritative cursor) and `origin_client_seq` (how "fan-out is the ack" matches a commit back to
its sender). `NackReason` includes `NACK_REASON_UNAVAILABLE` (transient freeze — resubmit on
`RoomStatus{LIVE}`).

### [events.proto](proto/aether/v1/events.proto) — the event catalog
Phase 1 carries **one generic event**, `KeyValueSet` (set key = value), inside `EventBody` (durable)
and `EphemeralBody` (ephemeral) oneofs, folded into `RoomState` (a `map<string,bytes>`) as
last-write-wins. Real feature events (SlideSet, Mute, HandRaise, AdminOverride, Cursor, Presence…)
are Phase 2, added as **additive** oneof members so `buf breaking` stays green.

### [owner.proto](proto/aether/v1/owner.proto) — the internal gateway → owner RPC (`RoomService`)
Go-only (both sides are Go); generated with the Connect-Go plugin. Reuses the envelope's
`Event`/`Nack`/`EventBody`/`EphemeralBody`/`RoomState`. Five RPCs:
- `Commit` → `CommitResponse{committed | nack | duplicate}` (three outcomes mirror `Runtime.Commit`).
- `GetSnapshot` → current materialized state for a fresh/deep-resume join.
- `Subscribe(from_seq)` → `stream Event`, **strict `room_seq` order, gap-free** — catch-up then live.
- `Broadcast` → fire-and-forget ephemeral fan-out.
- `SubscribeEphemeral` → `stream Ephemeral` (separate stream so a cursor flood can't reorder/stall events).

---

## 4. Go packages (`go/internal/`)

Dependency direction is strictly one-way (nothing depends "up"):

```
gen (protobuf) ─┐
                ├─ roomcore (pure)
                ├─ logstore ─┐
                ├─ coord ─────┤
                ├─ fanout ────┤
                │             ▼
                │        roomruntime ── ownerrpc ── gateway
                └─ sim (orthogonal test kernel; injected into the above)
```

### `roomcore` — the pure state machine (no I/O)
Everything correctness-critical, testable in isolation.
- [roomcore.go](go/internal/roomcore/roomcore.go): `Room{state, nextSeq, dedup}`. `Apply(clientID, clientSeq, body)`
  dedups on `(clientID, clientSeq)` high-water, assigns the next `room_seq`, folds the body, returns
  the `Event` (or `(nil,false)` for a duplicate → exactly-once). `Replay(snapshot, events)` rebuilds
  state purely. `fold` is last-write-wins for `KeyValueSet` (copies the value bytes).
- [cursor.go](go/internal/roomcore/cursor.go): `Cursor` + `Decision{Apply|Skip|Gap}` — the gap-detection the
  SDK/gateway use to apply, ignore a dup, or trigger a resume.
- [recovery.go](go/internal/roomcore/recovery.go): `Snapshot{RoomSeq, State, Dedup}`, `Restore`,
  `RestoreAndReplay(snapshot, tail)` — the re-home / cold-start reconstruction (carries dedup marks
  so a re-home never double-applies).
- Tests: [roomcore_test.go](go/internal/roomcore/roomcore_test.go), [property_test.go](go/internal/roomcore/property_test.go)
  (rapid: idempotency, replay determinism, snapshot equivalence), [golden_test.go](go/internal/roomcore/golden_test.go)
  (the shared vectors), cursor/recovery tests.

### `logstore` — the durable per-room log (truth)
- [logstore.go](go/internal/logstore/logstore.go): `LogStore` iface — `Append(roomID, expectedSeq, event)`
  (conditional: only if `expectedSeq == head+1`, else `ErrConflict` — the **split-brain guard**),
  `Read(from)`, `Head`, `Write/ReadSnapshot`. `ErrConflict` is how a stale owner discovers it lost.
- [memory.go](go/internal/logstore/memory.go): in-memory impl enforcing the same conditional-append.
- **Pending:** the DynamoDB impl (`pk=room_id, sk=room_seq`, conditional write) — design's PR-13.

### `coord` — ownership lease + directory
- [coord.go](go/internal/coord/coord.go): `Lease{Owner, Addr, Expiry, Token}` and `Coordinator` iface —
  `Claim(roomID, owner, addr, now, ttl)` (publishes owner **and** dialable addr atomically),
  `Renew`, `Release`, `Current` (the directory lookup gateways route with). Time is passed in
  explicitly (`now`, `ttl`) so it's deterministic under the sim. Fail-safe: ambiguity → no ownership.
- [memory.go](go/internal/coord/memory.go): in-memory impl; fencing `Token` bumps on takeover, is
  retained across `Release` so it stays monotonic.
- **Pending:** the DynamoDB impl (conditional writes + TTL).

### `fanout` — the outbound delivery bus
- [fanout.go](go/internal/fanout/fanout.go): two interfaces — `Fanout` (durable events) and
  `EphemeralFanout` (cursors/presence). Delivery is **live-only** (no backlog; catch-up is the log's
  job). Kept as separate buses so an ephemeral flood can't reorder/backpressure committed events.
- [bus.go](go/internal/fanout/bus.go): the generic `bus[T]` both tiers share — subscription-order
  delivery, handlers invoked outside the lock, panic-isolated.
- [memory.go](go/internal/fanout/memory.go) / [ephemeral.go](go/internal/fanout/ephemeral.go): the
  `Memory` and `MemoryEphemeral` impls.
- **Pending:** the Redis Streams + sharded pub/sub impl. (Deferred by the gateway design: the
  `Subscribe`-stream relay is the cross-process path for now; Redis slots in behind the same iface.)

### `roomruntime` — the room owner (the write journey)
Wires the pure reducer to the log + buses and enforces ownership. **This is the heart of the backend.**
- [runtime.go](go/internal/roomruntime/runtime.go): `Runtime` owns a set of rooms on a node.
  - `Commit`: confirm ownership (claim-on-serve lease) → dedup → assign seq → **`Append` (ack-after-persist)**
    → fan out *outside the lock* (fan-out is the ack). On `ErrConflict`, drop the in-mem room so it rebuilds.
  - `Join`: claim + return a snapshot clone (never the live mutable state).
  - `Release`: graceful lease release + drop the room so a survivor claims instantly.
  - `acquire`/`ensureRoom`: the ownership gate + log-replay rebuild (`RestoreAndReplay`).
  - Two ownership guards: **soft** = the coord lease (`ErrNotOwner`); **hard** = the log's conditional
    `Append`. A single Runtime-wide mutex serializes all rooms (a deliberate Phase-1 simplification).
- [tail.go](go/internal/roomruntime/tail.go): `Tail(roomID, fromSeq, send)` — streams events in strict
  `room_seq` order, gap-free, replaying history from the log then going live. **The fan-out bus is only
  a wakeup;** events are always read from the log, so the stream is immune to fan-out reorder/dup. A
  `tailPoll` ticker re-reads the log as a correctness floor (catches a re-home to another node, a
  dropped wake, or a full Redis outage).
- [ephemeral.go](go/internal/roomruntime/ephemeral.go): `Broadcast` + `TailEphemeral` — the lossy tier
  (bounded per-subscriber buffer, drop-on-overflow, no replay, no re-home self-heal — it relies on
  being paired with an event stream).
- Tests: [runtime_test.go](go/internal/roomruntime/runtime_test.go), [ownership_test.go](go/internal/roomruntime/ownership_test.go),
  [tail_test.go](go/internal/roomruntime/tail_test.go), [ephemeral_test.go](go/internal/roomruntime/ephemeral_test.go),
  and the sweep **[dst_failover_test.go](go/internal/roomruntime/dst_failover_test.go)** (see §6).

### `ownerrpc` — the RoomService server
- [server.go](go/internal/ownerrpc/server.go): adapts a `*roomruntime.Runtime` to the generated
  `RoomServiceHandler`. Maps `Runtime.Commit`'s three outcomes to `CommitResponse{committed|nack|duplicate}`;
  pipes `Tail`→`Subscribe` and `TailEphemeral`→`SubscribeEphemeral`. **Error mapping:** `ErrNotOwner` /
  `ErrConflict` → Connect `FAILED_PRECONDITION` (gateway re-resolves); a cancelled ctx is a clean end,
  not a failed stream. `OUT_OF_RANGE` (deep-resume) is a documented TODO pending compaction.

### `gateway` — the client-facing WS terminator + router
The first layer that faces the client. Holds **no durable state**.
- [server.go](go/internal/gateway/server.go): `Server` (auth + locator + cluster HMAC secret) and the
  per-connection `conn`. A `conn` runs four goroutines — `readLoop` (decode frames, answer `Ping` inline),
  `opsLoop` (a single worker draining room frames off the read loop so a slow owner RPC can't stall
  keepalive), `writeLoop` (the sole socket writer), `pingLoop` (WS keepalive). Handlers: `handleJoin`
  (fresh → `GetSnapshot`+`Joined`; resume → skip snapshot, relay the gap), `handleCommit` (→ owner
  `Commit`; failures → `Nack{UNAVAILABLE}`; enforces `NOT_JOINED`), `handleBroadcast`, `handleLeave`.
  The **`relay` supervisor** per room: subscribes the owner's event + paired ephemeral streams and, on
  an owner-side failure (death / re-home), emits `RoomStatus{FROZEN}`, re-resolves, and re-subscribes
  from the cursor (`LIVE` on recovery) — gap-free because the cursor drives the re-subscribe and the log
  is shared. Backpressure: `send` disconnects a slow client (it resumes from `lastSeq`), `sendEphemeral`
  drops first (ephemerals are lossy).
- [locator.go](go/internal/gateway/locator.go): `OwnerLocator` — resolves `coord.Current(room)` → owner
  addr, pools one Connect `RoomServiceClient` per address, `Invalidate(addr)` on failover. An empty
  `Addr` = non-routable → `ErrNoOwner` (re-resolve, not a black hole).
- [clientid.go](go/internal/gateway/clientid.go): `deriveClientID` = `HMAC(secret, len(principal)‖principal‖nonce)` —
  a stable, unforgeable id any stateless gateway re-derives (so dedup survives a reconnect to a
  *different* gateway).
- [auth.go](go/internal/gateway/auth.go): `Authenticator` iface + `DevAuthenticator` (trusts a header — dev only).
- Tests: join/commit/broadcast/resume/failover/opsworker/locator/clientid `_test.go` files.

### `sim` — the deterministic simulation kernel
- [sim.go](go/internal/sim/sim.go): `Sim` — a single-threaded discrete-event simulator with a seeded
  RNG and a virtual clock (real `time.Time`/`Duration`, not ticks). `Schedule(delay, fn)` + `Run(maxSteps)`.
  Same seed → identical run, so a failing chaos seed replays bit-for-bit.
- [network.go](go/internal/sim/network.go): `Network` — an in-memory bus with `FaultConfig`
  (drop / duplicate / delay / reorder) and `Partition`/`Heal`. All faults draw from the sim's RNG.

### `protocol` / `buildinfo`
- [protocol/protocol.go](go/internal/protocol/protocol.go): conformance/round-trip over the generated
  wire types + `PROTOCOL_VERSION`. [buildinfo/buildinfo.go](go/internal/buildinfo/buildinfo.go): `Name`/`Version`
  (stamped at release via `-ldflags`).

---

## 5. TypeScript workspace (`packages/`)

Only `packages/protocol` exists today (`client-sdk`, `react`, `apps/web` are not started).
- [packages/protocol/src/reducer.ts](packages/protocol/src/reducer.ts): the **client-side mirror** of Go's
  `roomcore` fold — `MaterializedState = Map<string, Uint8Array>`, `fold`, `replay`. It is an independent
  implementation kept in lockstep with Go by the shared golden vectors (`.slice()` mirrors Go's defensive copy).
- [packages/protocol/src/index.ts](packages/protocol/src/index.ts): re-exports generated types + `PROTOCOL_VERSION`.
- Tests: `reducer.test.ts`, `envelope.test.ts`, `index.test.ts` (vitest).

---

## 6. Testing

The confidence strategy (design doc [04](04-phase1-implementation-plan.md) §3): a testing *trophy* —
static → unit → property → integration → contract → **deterministic simulation (DST)**.

- **Unit + property (Go, `rapid`):** `roomcore` idempotency, replay/permutation determinism, snapshot
  equivalence; `logstore`/`coord`/`fanout` conditional-append, lease lifecycle, bus behavior.
- **Golden vectors:** [testdata/golden/roomcore.json](testdata/golden/roomcore.json) — the same
  `(events → expectedState)` fixtures run against **both** the Go fold and the TS reducer, so client and
  server can't silently diverge.
- **DST / chaos:** [go/internal/roomruntime/dst_failover_test.go](go/internal/roomruntime/dst_failover_test.go)
  sweeps 120 seeds of a 3-node cluster sharing one log + coordinator on the virtual clock, killing/reviving
  owners mid-session and modelling lost acks. It asserts per seed: **exactly-once**, **no loss**, **contiguous
  total order**, **convergence** (a fresh node re-homes to the same head); and asserts once across the sweep
  that every failure path actually fired (a "teeth" check so the chaos can't quietly degrade to a happy path).
- **Pending:** the full client↔gateway↔owner DST (design's G11 / PR-23–25) that lands in `test/chaos/`;
  Testcontainers integration tests once the DynamoDB/Redis impls exist.

**CI** ([.github/workflows/ci.yml](.github/workflows/ci.yml)): four lanes — `proto` (buf lint + breaking
vs main), `go` (generate → build → `go test -race` → golangci-lint), `ts` (generate → yarn
lint/typecheck/test/format:check), and `pr-title` (semantic Conventional-Commit title check).

---

## 7. Traceability — design ↔ code

| Design doc | Realized by |
|-----------|-------------|
| [01 §4](01-design-backbone.md) core concepts (room, owner, roomSeq, lastSeq, ack-after-persist, re-home, lease) | `roomcore`, `roomruntime`, `coord`, `logstore` |
| [01 §6.1](01-design-backbone.md) journey of a write | `roomruntime.Commit` → `logstore.Append` → `fanout.Publish` |
| [01 §6.3](01-design-backbone.md) recovery mechanics | `roomcore.Cursor`, `roomruntime.Tail`, gateway `relay` |
| [01 §6.4](01-design-backbone.md) ownership/leases/re-homing | `coord`, `roomruntime.acquire`, DST test |
| [04 §5](04-phase1-implementation-plan.md) PR plan (M0–M6) | proto + `roomcore`/`logstore`/`coord`/`fanout`/`sim` + `roomruntime` |
| [05](05-design-gateway.md) gateway (G1–G11) | `owner.proto`, `ownerrpc`, `gateway` (G1–G10 done; G11 pending) |

**Reading order for a newcomer:** [ONBOARDING.md](ONBOARDING.md) → [03](03-brief-leadership.md)/[02](02-brief-engineering.md)
briefs → [01](01-design-backbone.md) → [05](05-design-gateway.md) → this index → the code (start at
`roomcore`, then `roomruntime`, then `gateway`) → run `task test`.

---

## 8. What is NOT built yet (so you don't assume it)

- **No runnable service binary** — `go/cmd/` is empty; there is no `aether-node` wiring gateway+owner
  together yet. The system is exercised only through tests.
- **No real infra adapters** — `logstore`/`coord` are in-memory (no DynamoDB); `fanout` is in-memory
  (no Redis). The interfaces exist; the AWS-backed impls do not.
- **No TS SDK / React / web app** — only the shared `protocol` reducer.
- **No snapshots-to-logstore / compaction / deep resume** — the contract reserves it
  (`Subscribe` `OUT_OF_RANGE`, `Joined.snapshot`), but there's no log floor yet, so every cursor replays.
- **No background lease-renewal loop** — ownership is "claim-on-serve" (each Commit/Join renews); a quiet
  room's lease lapses and is re-homed on next touch.
- **Gateway G11 + full-stack chaos harness** (`test/chaos/`) — pending; the current DST is owner-layer only.
- **Infra** ([infra/](infra/)) and **`apps/`** are `.gitkeep` placeholders.
</content>
</invoke>
