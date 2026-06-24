# Aether — Phase 1 Implementation Plan

> *The element that fills all space and connects everything.*
> **Audience:** Engineering · **Status:** Ready to execute · **Prereq reading:** [01-design-backbone.md](01-design-backbone.md)

This is the build plan for **Phase 1 (the backbone)**. It is organized as a sequence of **small,
independently-reviewable PRs**. Reliability is the product: **testing and confidence are baked into every
PR**, not bolted on at the end. The practices below are grounded in current (2025–2026) authoritative
sources — see [§7 References](#7-references).

---

## 0. Operating principles

1. **Confidence is the deliverable.** A PR isn't done when the code works — it's done when a test proves it
   *keeps* working under the faults it's meant to survive. The Phase-1 exit criterion is a **green chaos
   suite**, not a running server.
2. **Small, incremental PRs.** Each PR is one self-contained change, ideally <~400 LOC of diff — the band
   where review catches 70–90% of defects and approvals are ~3× faster.[^smartbear][^infoq] Slice big work
   into a sequence that builds up; land interfaces/contracts before callers; **refactors get their own
   PRs**.[^google] Smallest / lowest-risk first.
3. **Every change goes through a PR.** No direct pushes to `main`, no force-push, ever. `main` is always
   green and releasable.
4. **Foundationally strong, polyglot-honest.** Go backend, React/TS frontend, a Protobuf contract between
   them. Each language gets first-class linting, type-safety, and testing — no second-class side.
5. **The contract is frozen early and machine-enforced.** Backward-incompatible protocol changes fail CI
   (`buf breaking`). This is what makes parallel client/server work safe.

---

## 1. Tech stack decisions

| Layer | Choice | Why |
|-------|--------|-----|
| **Backend** | **Go** — `gateway` + `room-runtime` services | Concurrency-native; best-in-class story for `-race` and deterministic simulation testing (the chaos harness). |
| **Frontend** | **React + TypeScript** (Vite) | Phase 2+; consumes the SDK via hooks. |
| **Client SDK** | **TypeScript** package, generated types from Protobuf | Hides all recovery machinery behind `commit`/`broadcast`/`state`. |
| **Contract / protocol** | **Protobuf + [Buf](https://buf.build)** | One canonical schema → generates Go *and* TS; `buf lint` + `buf breaking` enforce the frozen contract in CI.[^buf] Binary-efficient on the wire at 100k-room scale. |
| **Transport** | Persistent **WebSocket**, thin protobuf-framed `ClientMessage`/`ServerMessage` oneof envelope | Long-lived stateful stream with resume — not request/response, so not Connect-RPC. WS lib: [`coder/websocket`](https://github.com/coder/websocket) (context-aware, modern). |
| **Truth store** | **DynamoDB** (`pk=roomId, sk=roomSeq`), conditional writes | Native per-room seek; conditional write = free split-brain guard (D3 in the design doc). `aws-sdk-go-v2`. |
| **Hot tail / fan-out / leases** | **Redis** (`go-redis/v9`) split by role; leases on Dynamo conditional writes | Accelerator, never truth (D4). |
| **Reducer parity** | **Golden test vectors** (protobuf fixtures) both Go + TS reducers must pass | Polyglot replacement for "one shared reducer" — prevents client/server state drift. |

### Repo shape (polyglot monorepo)
```
aether/
├── proto/                      # Protobuf contract — THE spine. buf.yaml, buf.gen.yaml
│   └── aether/v1/*.proto
├── gen/                        # generated code (Go + TS), checked in or built in CI
├── go/                         # Go module workspace (go.work)
│   ├── internal/protocol/      # generated Go types + thin helpers
│   ├── internal/roomcore/      # pure room logic: fold, seq, dedup, snapshot. NO I/O.
│   ├── internal/logstore/      # log-adapter iface + dynamo + inmem impls
│   ├── internal/coord/         # lease + room→owner directory iface + impls
│   ├── internal/fanout/        # redis stream/pubsub + direct-push degrade
│   ├── internal/sim/           # deterministic simulation harness (clock/net/rng)
│   ├── cmd/gateway/            # the WS terminator service
│   └── cmd/roomruntime/        # the owner service
├── packages/                   # TS workspace (Yarn 4 + Turborepo for the TS side)
│   ├── protocol/               # generated TS types + golden-vector conformance
│   ├── client-sdk/             # the browser SDK
│   └── react/                  # (Phase 2) hooks
├── apps/web/                   # (Phase 2) React app
├── test/chaos/                 # Go DST scenarios + Go↔TS e2e harness
├── Taskfile.yml                # unified entrypoint (wraps go + buf + yarn/turbo)
├── lefthook.yml                # pre-commit hooks
└── .github/workflows/          # CI lanes
```
> **Monorepo orchestration:** `go.work` for Go modules + Yarn 4 workspaces (`workspace:*`) + Turborepo for
> the TS side;[^yarn][^turbo] a top-level **Taskfile** is the one entrypoint that ties Go, Buf, and the TS
> workspace together. We deliberately **don't** force Turborepo to own Go — Go has its own build cache.

---

## 2. Foundation & developer experience (Milestone 0)

Grounded in the tooling research. Set up once, enforced forever.

### Go
- **`golangci-lint`** (curated meta-linter) + **`gofumpt`** (stricter gofmt) + `go vet`. Fails CI.
- **`go test -race`** on by default in CI — the cheapest distributed-bug catcher we have.
- Pin Go via `go.mod` `go 1.2x` + a `.tool-versions`/Taskfile check.

### TypeScript (frontend + SDK)
- **ESLint flat config** (`eslint.config.js`) + **typescript-eslint v8** with `parserOptions.projectService:
  true` (type-aware lint, no `tsconfig.eslint.json` hack).[^tseslint] **Prettier** owns formatting.
- **Strict tsconfig**: `strict`, **`noUncheckedIndexedAccess`**, **`exactOptionalPropertyTypes`**.[^tsconfig]
- **Module-boundary lint** (`eslint-plugin-boundaries` or `import/no-restricted-paths`): the **SDK/browser
  packages must not import server code**.[^boundaries] (Belt-and-suspenders with package layout.)
- **Vitest** (`test.projects` for unit/integration/chaos; `vitest.workspace` is deprecated ≥3.2).[^vitest]

### Protobuf / contract
- **Buf**: `buf lint` (style) + **`buf breaking`** (backward-compat gate vs `main`) + `buf generate` (Go via
  `protoc-gen-go`, TS via `protoc-gen-es`/`@bufbuild/protobuf`).[^buf]

### Cross-cutting DX
- **Lefthook** pre-commit: `gofumpt`+`golangci-lint` on staged Go, `eslint`+`prettier` on staged TS, `buf
  lint` on staged proto — parallel, fast.[^lefthook]
- **commitlint + Conventional Commits** on `commit-msg`; semantic PR-title lint in CI.[^conventional]
- **Corepack** pins Yarn (`"packageManager": "yarn@4.x"`); `.nvmrc` pins Node; `.editorconfig` baseline.[^corepack]

---

## 3. Testing strategy (the core)

> Shape: a lean **testing trophy** — *"write tests, not too many, mostly integration,"* maximizing
> confidence-per-test.[^kcd][^fowler] Five layers, each owning a class of risk.

1. **Static** — TS strict + ESLint + `golangci-lint` + `go vet`. Free, always-on.
2. **Unit (pure logic)** — `roomcore` fold/seq/dedup/snapshot in Go; SDK cursor/buffer logic in TS. Fast,
   deterministic, table-driven.
3. **Property-based (the invariants that *must* hold)** — Go: **[`rapid`](https://github.com/flyingmutant/rapid)**;
   TS: **`@fast-check/vitest`**.[^fastcheck] Encode the load-bearing guarantees as properties:
   - **Replay/permutation determinism** — folding a log (and valid reorderings of commutative events) → same
     materialized state.
   - **Idempotency** — `apply(apply(s,e),e) == apply(s,e)` (validates at-least-once + dedup).
   - **Snapshot equivalence** — `materialize(snapshot + tail) == materialize(fullLog)` (recovery doesn't drift).
   Every failing seed is committed as a permanent regression test.
4. **Integration (real infra, locally)** — **Testcontainers** starting `amazon/dynamodb-local` + Redis once
   via global setup; state reset (truncate/`FLUSHALL`) between tests.[^testcontainers] Exercises conditional
   writes, leases, fan-out, TTLs — the things mocks model poorly. *Caveat:* emulators diverge from real
   AWS (error strings, validation order) — **don't assert exact error strings; back conditional-writes with a
   real-AWS staging smoke test.**[^conformance]
5. **Contract** — the Protobuf schema is snapshot-tested; **`buf breaking` gates every change**; the
   **golden vectors** are run against both Go and TS reducers so the two ends can't drift.
6. **Deterministic Simulation Testing (DST) / chaos — the headline.** An in-process Go harness running the
   whole cluster on one goroutine with a **seeded RNG + virtual clock + in-memory network**, so any run
   replays exactly from its seed.[^tigerbeetle][^fdb][^dst] All nondeterminism is injected as deps
   (`{clock, net, rng}`) — no `time.Now`/`rand`/real sockets in domain code. Faults injected: **drop, delay,
   reorder, duplicate, partition, kill-owner-mid-handoff, clock-skew, dropped-release-ack.** After each fault
   storm we assert the **safety + liveness invariants**:
   - **At-most-one effective owner** per room, always.
   - **Convergence** — hash every node's room state/log; assert equality.
   - **Idempotency** — redelivered duplicates don't change final state.
   - **No committed (acked) event lost** across a re-home.
   - **Liveness** — after faults heal, a new owner is elected and progress resumes.

**CI gating for confidence:**[^vitest-cov][^ghchecks]
- Coverage thresholds as a hard merge gate; **100% on `roomcore/**` and `logstore/**`** (the load-bearing
  packages), lower elsewhere.
- **Fast lane on every PR** (required checks): lint, typecheck, unit, property, `buf breaking`, affected
  integration, + a **fixed-seed chaos smoke** (a handful of seeds).
- **Slow lane nightly + pre-release**: the full chaos suite over **thousands of seeds**, plus a real-AWS
  staging smoke.
- **Flaky handling:** scope retries to known-transient infra errors only; DST failures aren't flaky — they
  carry a seed and replay exactly. Fix, never blanket-mask.

---

## 4. Git / PR / CI workflow

**Branch protection (GitHub Rulesets on `main`):**[^rulesets]
- Require a PR before merging — **no direct pushes**.
- ≥1 approval · dismiss stale approvals on push · **require Code Owners** (`CODEOWNERS`) · require approval
  of most recent push · require conversation resolution.
- **Require status checks to pass** (the fast lane) · **require linear history** · **block force-pushes**.
- **Merge queue** (tests each PR against the future state of `main` so two green PRs can't break it
  together) — the modern alternative to "require branches up to date," without serializing rebases.[^mergequeue]
- **Squash-merge only**, "default to PR title" → clean linear history; PR titles are Conventional-Commit
  linted.[^merges]

**Per-PR checklist:** one self-contained change · <~400 LOC · refactor split out · Conventional-Commit title
· tests that prove the invariant it touches · green fast lane.

**CI shape (GitHub Actions):**[^turbo-ci]
- `pull_request` **fast lane** = required checks; **affected-only** (`turbo run --affected` for TS; Go path
  filters + `go test ./...` on changed modules); remote/`.turbo` + Go build cache; `concurrency:
  cancel-in-progress`.
- `schedule` + `workflow_dispatch` + `pull_request: types:[labeled]` (`run-chaos`) **slow lane** = full
  chaos + staging smoke.
- `merge_group` trigger mirrors the fast-lane checks for the queue.
- **`fetch-depth: 0`** so affected-detection and `buf breaking` see history.

---

## 5. The incremental PR plan

Sequenced smallest/lowest-risk first. Each PR lists **scope · tests · done-when**. Milestones are review
checkpoints, *not* barriers — later PRs can start once their dependency merges.

### Milestone 0 — Foundation (no product logic)
- **PR-01 · Repo scaffold.** Layout, `go.work`, Yarn4 workspace, Taskfile, `.editorconfig`/`.nvmrc`/corepack,
  LICENSE. *Tests:* `task build` no-ops green. *Done:* clean checkout builds.
- **PR-02 · Go tooling.** `golangci-lint`+`gofumpt`+vet config, a trivial `internal/version` + its test.
  *Tests:* lint+`go test -race` green. *Done:* Go lane runs.
- **PR-03 · TS tooling.** ESLint flat + typescript-eslint + Prettier + strict tsconfig + Vitest projects, a
  trivial package + test. *Done:* TS lane runs.
- **PR-04 · Buf setup.** `buf.yaml`/`buf.gen.yaml`, lint+breaking config, empty `aether/v1`. *Done:* `buf
  lint`+`buf breaking` (vs empty baseline) green.
- **PR-05 · CI + branch protection.** Fast-lane workflow (Go+TS+buf, affected, cached, concurrency-cancel);
  Ruleset on `main`; `CODEOWNERS`; semantic-PR-title lint; Lefthook + commitlint. *Done:* a dummy PR is
  blocked until checks pass; force-push rejected.

### Milestone 1 — The contract (protocol)
- **PR-06 · Wire envelope v1.** `ClientMessage`/`ServerMessage` oneof, `Event` envelope (`roomId, roomSeq,
  clientId, clientSeq, type, payload`), `Resume`/`Ack`/`Nack`/`Join`. Generate Go+TS. *Tests:* schema
  snapshot; round-trip encode/decode both langs. *Done:* `buf breaking` is now load-bearing.
- **PR-07 · Event catalog v1.** Durable (`SLIDE_SET`, `MUTE`, `HAND_RAISE`, `ADMIN_OVERRIDE`), ephemeral
  (`CURSOR`, `PRESENCE`). *Tests:* exhaustive enum/round-trip. *Done:* contract frozen for Phase 1.
- **PR-08 · Golden vectors + harness.** `(initialState, events) → expectedState` protobuf fixtures + a runner
  in both Go and TS. *Tests:* both runners pass the same vectors. *Done:* cross-lang parity is enforceable.

### Milestone 2 — Pure room logic (Go, no I/O)
- **PR-09 · `roomcore` fold + seq + dedup.** Apply events, assign `roomSeq`, dedup `(clientId, clientSeq)`.
  *Tests:* table-driven + **rapid** property (idempotency, dedup). 100% coverage. *Done:* passes golden vectors.
- **PR-10 · Snapshot + replay + gap detection.** *Tests:* **property** `snapshot+tail == fullLog`; gap →
  resume signal. *Done:* recovery math proven pure.
- **PR-11 · Simulation kernel.** `internal/sim`: injected `{clock, net, rng}`, in-memory bus (drop/delay/
  reorder/dup/partition). *Tests:* deterministic replay from seed on a trivial scenario. *Done:* harness
  replays bit-for-bit.

### Milestone 3 — Adapters (Go, integration-tested)
- **PR-12 · `logstore` iface + in-mem impl.** `Append(condOn roomSeq)`, `Read(from)`, snapshot R/W. *Tests:*
  unit + property (conditional-append rejects gaps/dupes). *Done:* `roomcore` runs on it.
- **PR-13 · DynamoDB `logstore`.** Conditional write enforcing `roomSeq` (split-brain guard); per-room query.
  *Tests:* **Testcontainers dynamodb-local** integration; no exact-error-string asserts. *Done:* + staging-smoke task.
- **PR-14 · `coord` leases + directory.** Lease via Dynamo conditional write; room→owner lookup; **fail-safe
  (freeze, never split-brain).** *Tests:* integration; **property/model test of the lease lifecycle**
  (claim/renew/expire/release races). *Done:* two racers → exactly one owner.
- **PR-15 · Redis `fanout`.** Stream hot-tail (`XADD`/`XREAD`), sharded pub/sub, ephemeral channel. *Tests:*
  Testcontainers Redis integration. *Done:* fan-out + resume-from-tail work.

### Milestone 4 — Services (Go)
- **PR-16 · `room-runtime` owner (in-mem fan-out).** Wire `roomcore`+`logstore`+`coord`; the write journey
  (dedup→seq→**ack-after-persist**→fan-out). *Tests:* integration + **DST: single-owner happy path under
  message reorder/dup**. *Done:* a durable write is exactly-once under reordering.
- **PR-17 · `gateway` WS terminator.** `coder/websocket`, authn stub, directory lookup, route-to-owner,
  relay fan-out down. *Tests:* integration with a fake client. *Done:* client↔owner round-trip over WS.
- **PR-18 · Redis fan-out + degrade-to-direct-push.** Replace in-mem fan-out; owner direct-pushes when Redis
  is down. *Tests:* **DST: kill Redis mid-session → fan-out continues, no loss.** *Done:* Redis is provably
  non-load-bearing.
- **PR-19 · Re-homing + graceful drain.** Lease-expiry detection, CAS-claim, rebuild from snapshot+log,
  `preStop` lease-release. *Tests:* **DST: kill owner (graceful + hard) → re-home, convergence, no-lost-acked-
  event, at-most-one-owner.** *Done:* the failover spine is proven.

### Milestone 5 — Client SDK (TS)
- **PR-20 · SDK skeleton.** `Connection` (single WS), `join`, `commit` (durable, awaits ack), `state` via the
  TS reducer. *Tests:* SDK reducer passes golden vectors; commit→ack against a stub server. *Done:* a
  durable write round-trips.
- **PR-21 · Recovery.** `lastSeq` cursor, `RESUME` handshake, outbound buffer + idempotent replay, optimistic
  apply + rollback on NACK. *Tests:* **fast-check property** of replay-dedup; reconnect mid-flight → no
  double-apply. *Done:* no-reload recovery works against a real gateway.
- **PR-22 · Ephemeral tier.** `broadcast` (lossy, fast). *Tests:* fan-out received, loss-tolerant. *Done:*
  both QoS tiers exposed.

### Milestone 6 — Chaos & Phase-1 exit
- **PR-23 · Full DST fault matrix.** All faults × invariants (at-most-one-owner, convergence, idempotency,
  no-lost-events, liveness) over many seeds; fixed-seed PR smoke wired into the fast lane, thousands-of-seeds
  into nightly. *Done:* the suite is the regression guard.
- **PR-24 · Go↔TS e2e harness.** Real `gateway`+`room-runtime`+fake SDK clients; the design-doc §6.6 matrix
  (kill gateway/owner/AZ-sim, partition, wipe Redis, quorum-loss-sim, reconnect-storm, rolling deploy).
  *Done:* end-to-end no-reload/no-dupe/no-loss proven across the stack.
- **PR-25 · Capacity sanity harness (scaled).** A downscaled model validating the push-not-poll + sharding
  claims. *Done:* numbers from design-doc §9 are demonstrated at small scale.

---

## 6. Definition of done — Phase 1

- [ ] `main` is always green; every change landed via PR (no direct pushes, no force-push).
- [ ] Contract frozen and machine-enforced (`buf breaking` gate live; golden vectors green both langs).
- [ ] `roomcore` + `logstore` at 100% coverage; property tests for idempotency / replay-determinism /
      snapshot-equivalence all green.
- [ ] DynamoDB conditional-write split-brain guard + lease lifecycle integration-tested (+ staging smoke).
- [ ] **The chaos suite is green** over thousands of seeds: kill gateway / owner (graceful + hard) / AZ-sim,
      partition, wipe Redis, quorum-loss-sim, reconnect-storm, rolling deploy → **every client converges,
      no reload, no dupes, no lost acked event, at-most-one-owner, post-heal liveness.**
- [ ] SDK delivers no-reload recovery against a real gateway; both QoS tiers work.
- [ ] CI fast lane < a few minutes; slow lane (chaos + staging) on nightly/pre-release.

When this checklist is green, Phase 1 is done — and Phases 2–3 build on a foundation whose reliability is
*demonstrated*, not asserted.

---

## 7. References

**Monorepo / tooling / DX**
[^yarn]: Yarn Workspaces — https://yarnpkg.com/features/workspaces · Corepack — https://yarnpkg.com/corepack
[^turbo]: Turborepo config + CI — https://turborepo.dev/docs/reference/configuration · https://turborepo.dev/docs/crafting-your-repository/constructing-ci
[^tseslint]: typescript-eslint v8 typed linting — https://typescript-eslint.io/getting-started/typed-linting/
[^tsconfig]: TSConfig strict family — https://www.typescriptlang.org/tsconfig/ · noUncheckedIndexedAccess — https://www.typescriptlang.org/tsconfig/noUncheckedIndexedAccess.html
[^boundaries]: eslint-plugin-boundaries — https://github.com/javierbrea/eslint-plugin-boundaries · no-restricted-imports — https://eslint.org/docs/latest/rules/no-restricted-imports
[^lefthook]: Lefthook vs Husky (monorepo hooks) — https://www.pkgpulse.com/guides/husky-vs-lefthook-vs-lint-staged-git-hooks-nodejs-2026
[^corepack]: Corepack — https://corepack.org/
[^conventional]: Conventional Commits — https://www.conventionalcommits.org/en/v1.0.0/
[^buf]: Buf (lint + breaking + generate) — https://buf.build/docs

**Testing**
[^kcd]: Kent C. Dodds — Write tests. Not too many. Mostly integration — https://kentcdodds.com/blog/write-tests
[^fowler]: Martin Fowler — The Practical Test Pyramid — https://martinfowler.com/articles/practical-test-pyramid.html
[^vitest]: Vitest Test Projects (workspace deprecated ≥3.2) — https://vitest.dev/guide/projects
[^vitest-cov]: Vitest coverage gating — https://vitest.dev/guide/coverage.html
[^fastcheck]: fast-check — https://fast-check.dev/ · @fast-check/vitest — https://www.npmjs.com/package/@fast-check/vitest · Go: rapid — https://github.com/flyingmutant/rapid
[^testcontainers]: Testcontainers node — https://node.testcontainers.org/ · Go DynamoDB Local + Testcontainers — https://dev.to/aws/run-and-test-dynamodb-applications-locally-using-docker-and-testcontainers-108i
[^conformance]: DynamoDB Local vs LocalStack conformance — https://martinhicks.dev/articles/dynoxide-conformance-suite
[^tigerbeetle]: TigerBeetle VOPR (deterministic simulation) — https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md · Liveness — https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/
[^fdb]: FoundationDB simulation — https://pierrezemb.fr/posts/diving-into-foundationdb-simulation/
[^dst]: DST primer — https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html · Jepsen consistency — https://jepsen.io/consistency

**Git / PR / CI**
[^google]: Google eng-practices — Small CLs — https://google.github.io/eng-practices/review/developer/small-cls.html
[^smartbear]: SmartBear/Cisco code-review study — https://smartbear.com/learn/code-review/best-practices-for-peer-code-review/
[^infoq]: 1.5M-PR analysis (small PRs, fewer defects) — https://www.infoq.com/news/2026/04/github-stacked-prs/
[^rulesets]: GitHub Rulesets — https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets
[^mergequeue]: GitHub merge queue — https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue
[^merges]: PR merge methods — https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-pull-request-merges
[^turbo-ci]: Turborepo CI + GitHub Actions concurrency — https://turborepo.dev/docs/crafting-your-repository/constructing-ci · https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency
[^ghchecks]: GitHub required status checks — https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches
