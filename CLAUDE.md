# CLAUDE.md

Guidance for Claude Code (and humans) working in this repo. For a first-timer walkthrough see
[ONBOARDING.md](ONBOARDING.md); for the full code map see [REPO-INDEX.md](REPO-INDEX.md).

## What this is

**Aether** is the greenfield real-time state backbone for Inclass (a live online-classroom
platform). One disposable WebSocket per client; correctness via **recovery** (sequence cursor +
idempotent replay + ack-after-persist), not redundant connections. A **per-room quorum log is the
only source of truth**; everything else (gateway nodes, Redis) is disposable and rebuildable.

Polyglot monorepo: **Go** backend + **TypeScript** SDK/app + a **Protobuf/Buf** contract between
them. Phase 1 (the backbone) is in active build; everything runs **in-memory, in-process** today
(the DynamoDB/Redis adapters and service binaries are not written yet — see REPO-INDEX §8).

## ⚠️ Generated code is not checked in — generate before you build

`go/gen/` and `packages/*/src/gen/` are gitignored and produced by `buf generate`. **A fresh
checkout will not compile until you generate.** Always run `task proto:gen` (or `buf generate`)
after cloning or changing `.proto` files. `task build`/`task test` do this for you.

## Prerequisites

- **Go 1.26** (pinned in `go/go.mod` / `go.work`)
- **[Buf](https://buf.build/docs/installation)** (`buf`) — proto lint/breaking/generate
- **[Task](https://taskfile.dev/installation/)** (`task`) — the unified entrypoint
- **Node ≥22** + **Yarn 4** via Corepack (`corepack enable`) — for the TS side
- **golangci-lint v2** — Go lint/format

## Commands

Use the Taskfile from the repo root:

```bash
task build          # buf generate → go build ./...
task test           # buf generate → go test -race ./...   (the default confidence gate)
task lint           # golangci-lint run
task fmt            # golangci-lint fmt (gofumpt+goimports)
task proto:gen      # buf generate  (Go into go/gen, TS into packages/protocol/src/gen)
task proto:lint     # buf lint
task proto:breaking # buf breaking --against main
```

Direct, when iterating on one side (run from the right dir):

```bash
cd go && go test -race ./...                       # all Go tests
cd go && go test -race ./internal/roomruntime/...  # one package
cd go && go test -run TestDST_FailoverConvergence ./internal/roomruntime/  # the chaos sweep
yarn install --immutable && yarn test              # TS (needs `buf generate` first)
yarn lint && yarn typecheck && yarn format:check   # TS checks (mirror CI)
yarn verify:dual-build                             # build both formats + load each via require()/import()
```

### TS packages ship ESM **and** CommonJS

Each package builds twice — `dist/esm` (the `import` condition) and `dist/cjs` (the `require`
condition) — so Node tooling can `require('@aether/client')`. Three pieces make it work, and all
three are load-bearing:

- **`tsconfig.esm.json` / `tsconfig.cjs.json`** per package do the emitting; the package's plain
  `tsconfig.json` is typecheck-and-editor only (`noEmit`, tests included). The CJS config must
  override `module`, `moduleResolution` (Node10 — Bundler is illegal with a CJS `module`) and
  `verbatimModuleSyntax: false` (it otherwise forbids rewriting `import` to `require`).
- **`scripts/write-dist-type-markers.mjs`** stamps `dist/{esm,cjs}/package.json` with the module
  `type`. tsc emits `.js` for both formats, so without the marker Node reads `dist/cjs` as ESM
  (the package is `"type": "module"`) and every `require()` throws.
- **`scripts/check-dual-build.mjs`** actually loads each package both ways in CI. A wrong `exports`
  condition or a missing marker builds and typechecks cleanly, then fails at the consumer.

Adding a package? Give it the same three configs and the same `exports` map — the marker script
picks up any directory under `packages/` automatically.

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs four lanes: `proto` (buf lint +
breaking vs `main`), `go` (generate → build → `test -race` → golangci-lint), `ts` (generate →
lint/typecheck/test/format:check), `pr-title` (semantic Conventional-Commit title).

## Architecture in one screen

```
browser/SDK ── ONE WebSocket ──▶ gateway (stateless) ──RPC──▶ room-runtime (the OWNER)
   ClientMessage/ServerMessage      terminate WS,            dedup→seq→APPEND→fanout
   (aether.proto)                   resolve owner, relay      (RoomService, owner.proto)
                                          │                          │
                                    coord.Current(room)         logstore (per-room log = TRUTH)
                                    (room→owner directory)      coord (lease) · fanout (delivery)
```

Two wire protocols, kept distinct: the **client↔gateway WS envelope**
([aether.proto](proto/aether/v1/aether.proto)) and the internal **gateway↔owner RPC**
([owner.proto](proto/aether/v1/owner.proto), Connect-Go). Go dependency direction is one-way:
`roomcore` (pure) ← `logstore`/`coord`/`fanout` (adapters) ← `roomruntime` (owner) ← `ownerrpc` ←
`gateway`; `sim` is the orthogonal test kernel injected into all of them.

### Package map (`go/internal/`)
- **`roomcore`** — pure, I/O-free reducer: fold, assign `room_seq`, dedup `(client_id, client_seq)`,
  snapshot/restore, cursor gap-detection. Most-tested; **keep it pure** (no time/rand/net/log).
- **`logstore`** — durable per-room log iface + in-memory impl. `Append` is conditional on
  `expectedSeq` → `ErrConflict` is the split-brain guard. (DynamoDB impl pending.)
- **`coord`** — ownership lease (`Claim`/`Renew`/`Release`) + room→owner directory (`Current`), with a
  fencing token and dialable `Addr`. Time passed in explicitly. Fail-safe. (DynamoDB impl pending.)
- **`fanout`** — live-only delivery bus, separate durable + ephemeral tiers, in-memory impl. (Redis pending.)
- **`roomruntime`** — the owner: the write journey (ownership → dedup → seq → **ack-after-persist** →
  fan-out-is-the-ack), `Tail` (in-order gap-free event stream), ephemeral `Broadcast`/`TailEphemeral`.
- **`ownerrpc`** — Connect `RoomService` server over a `Runtime`; maps `ErrNotOwner`/`ErrConflict` →
  `FAILED_PRECONDITION`.
- **`gateway`** — client WS terminator + `OwnerLocator` (pooled RPC clients) + per-room `relay`
  supervisor (FROZEN/LIVE failover re-subscribe) + `deriveClientID` (HMAC) + `Authenticator`.
- **`sim`** — deterministic discrete-event simulator + fault-injecting network (drop/dup/delay/reorder/partition).

## Load-bearing invariants (do not break these)

1. **The per-room log is the only truth.** Gateways and Redis hold nothing unrecoverable. Any state
   can be rebuilt by replaying the log (`roomcore.RestoreAndReplay`).
2. **Ack-after-persist.** A durable event becomes "real" only when `logstore.Append` succeeds; only
   then is it fanned out. Never ack a commit before the append. Fan-out *is* the ack (matched by
   `origin_client_seq`).
3. **Exactly-once via dedup.** `(client_id, client_seq)` high-water in `roomcore` makes replays no-ops.
   `client_id` must be stable across reconnects — hence HMAC derivation from principal + session nonce,
   never a per-connection server assignment.
4. **At-most-one effective owner.** Two guards: the coord lease (soft) **and** the log's conditional
   `Append` (hard). Correctness must not rest on the lease alone.
5. **In-order, gap-free delivery.** `roomruntime.Tail` always reads events from the log (fan-out is
   only a wakeup), so it's immune to fan-out reorder/dup. A consumer seeing a `room_seq` gap must
   resume, not apply out of order.
6. **Two QoS tiers stay separate.** Durable (`Commit`/`Event`) vs ephemeral (`Broadcast`/`Ephemeral`):
   different buses, different streams; ephemerals are dropped first under pressure and never touch the log.
7. **Determinism in the sim.** No `time.Now`, `rand`, or real sockets in domain code — inject
   `clock`/`net`/`rng` so a failing chaos seed replays bit-for-bit. `roomruntime`/`coord` already take
   an injectable clock; keep new code the same.

## Conventions & workflow

- **Small, independently-mergeable PRs**, lowest-risk first, ideally <~400 LOC; land interfaces before
  callers; refactors get their own PR. Each PR ships the tests that prove the invariant it touches.
- **Every change via PR** — no direct pushes to `main`, **no force-push ever** (use forward reverts).
  `main` stays green and releasable. Squash-merge, linear history.
- **Conventional-Commit PR titles** (`feat(gateway): …`, `fix(fanout): …`) — CI lints them. Feature
  branches are `feat/<name>` (e.g. `feat/gateway-g10-failover`).
- **Do NOT add a `Co-Authored-By` trailer** to commits.
- **Don't run formatters/prettier unless asked**; if you do format, it's `task fmt` / `yarn format`.
- **`buf breaking` is load-bearing** — proto changes must be additive (new oneof members, new fields).
- Don't self-merge PRs; open + get CI green, hand off for review. Keep the number of open PRs small.
- The gateway work is a **stacked PR chain** (ops-worker → G8c → G9 → G10, PRs #33–#36 open at time
  of writing) — respect the stack order when rebasing/merging.

## Gotchas

- **Compile errors after checkout** almost always mean you forgot `task proto:gen`.
- **Empty `Lease.Addr` = deliberately non-routable** — the locator returns `ErrNoOwner` so a
  misconfigured owner (forgot `WithAddr`) becomes a fast re-resolve, not a silent black hole.
- **Dev-only HMAC secret**: `NewServer` warns and uses a public dev key if `WithClientIDSecret` is
  unset. Production must inject a shared cluster secret (every gateway must share it).
- **`ErrNotOwner` is expected**, not exceptional — the gateway re-resolves the owner and retries; the
  owner-side single-threaded DST can't reach it, but the gateway path can.
- **`Runtime` uses one global mutex** across all rooms and holds it across the append — a Phase-1
  simplification. Fan-out happens *outside* the lock on purpose (a slow subscriber must not stall
  commits or deadlock a re-entrant call).
- **The status in `01-design-backbone.md` header ("pre-build") is stale** — Phase 1 is well underway.
  Trust the code and [REPO-INDEX.md](REPO-INDEX.md) for current state.

## Where to change what

- **New event type** → add an additive oneof member in [events.proto](proto/aether/v1/events.proto),
  regenerate, extend `roomcore.fold` **and** the TS `reducer.ts`, add a golden vector — both reducers
  must stay in lockstep.
- **New wire message / RPC** → [aether.proto](proto/aether/v1/aether.proto) /
  [owner.proto](proto/aether/v1/owner.proto) (additive only), regenerate, wire the handler in
  `gateway` / `ownerrpc`.
- **Real infra adapter** → implement `logstore.LogStore` (DynamoDB), `coord.Coordinator` (DynamoDB),
  or `fanout.Fanout` (Redis) behind the existing interfaces; the runtime shouldn't change.
- **New fault / chaos scenario** → extend `sim` + a `dst_*_test.go`.
</content>
