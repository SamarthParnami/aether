# 05 — Gateway design

> Status: **design pass** (no code yet). This doc aligns the gateway architecture before the
> incremental PRs. It builds on `01-design-backbone.md` (the spine) and `04-phase1-implementation-plan.md`.

The owner layer is done and chaos-proven: a node serves a room only while it holds the lease
(ownership gate), and failover under churn is enforced over a deterministic-simulation sweep. The
**gateway** is the next layer — the first piece that faces the *client* instead of the log.

## Decisions locked (this pass)

| Decision | Choice | Why |
|---|---|---|
| Gateway ↔ owner transport | **Remote RPC from the start** (Connect-Go / gRPC) | Cross-node routing + that fault domain exist from day one, not bolted on later. |
| Client WebSocket library | **coder/websocket** | Minimal, context-aware, stdlib-style; fits the disposable-connection model. |
| Auth (Phase 1) | **Pluggable `Authenticator` stub** | Keep the backbone decoupled from Uprio auth; wire real verification at app integration. |

Everything else below is designed off the **existing frozen Buf contract** (`aether/v1/aether.proto`).

---

## 1. The two interfaces

The gateway sits between two completely different wire protocols. Keeping them distinct is the
core of the design.

```
   browser/SDK                  gateway (stateless)                 owner node (room-runtime)
  ┌───────────┐   WS: ClientMessage/    ┌──────────────┐   RPC: RoomService    ┌──────────────┐
  │  client   │◀───ServerMessage  ─────▶│  WS terminator│◀──(Connect/gRPC)────▶│  RPC server   │
  │  (1 WS)   │    (protobuf frames)    │  + router     │                      │  + Runtime    │
  └───────────┘                         └──────┬───────┘                      └──────┬───────┘
                                               │ coord.Current(room) → owner addr     │ logstore
                                               ▼                                      │ coord
                                         ┌───────────┐                                │ fanout
                                         │   coord   │  (room → owner directory)      └──────────┘
                                         └───────────┘
```

1. **Client ↔ Gateway — the WebSocket envelope** (already specified in `aether.proto`): one
   disposable WS per client carrying `ClientMessage`/`ServerMessage` protobuf frames, multiplexing
   rooms (every frame has `room_id`). The gateway *terminates* this.
2. **Gateway ↔ Owner — a new RPC service** (`RoomService`, defined below): the gateway is an RPC
   *client*; each owner node runs the RPC *server* wrapping its `roomruntime.Runtime`.

The gateway holds **no durable state** — it is pure plumbing: terminate WS, authenticate, resolve
owner, forward, relay. Any gateway can serve any client; losing one only drops live sockets, which
reconnect and recover.

## 2. Topology & process model

- **Node = gateway + owner, co-located in one binary** (`cmd/aether-node`). Simpler ops (one
  Devtron app), and the gateway still always speaks **RPC** to owners — localhost for a room owned
  here, remote for a room owned elsewhere. The seam to split into separate gateway/owner tiers
  later is preserved (it's just an address).
- **The directory.** When an owner claims a room it must be *dialable*. `coord` today returns
  `Lease.Owner` (a node id) for fencing; the gateway also needs an **address**. We add a node-id →
  RPC-address resolution (either an `Addr` on the lease/directory, or a small membership registry).
  `coord.Current(room)` → `(ownerNodeID, addr)` is what the router uses.
- **Connection pooling.** The gateway keeps a pooled RPC client per owner address; invalidated on
  failover / dial errors.

## 3. The owner RPC service (`RoomService`)

A new internal proto (`proto/aether/v1/owner.proto`), Go-only (gateway↔owner are both Go), reusing
the existing `Event` / `EventBody` / `EphemeralBody` / `RoomState` / `Nack` messages. Generated with
the **Connect-Go** plugin added to `buf.gen.yaml`.

```proto
service RoomService {
  // Durable commit. Maps to Runtime.Commit. App-level rejection → Nack in the response;
  // transport-level "I'm not the owner" → Connect FAILED_PRECONDITION so the gateway re-resolves.
  rpc Commit(CommitRequest) returns (CommitResponse);

  // Current materialized state for a fresh join.
  rpc GetSnapshot(GetSnapshotRequest) returns (GetSnapshotResponse);

  // Catch-up + live tail in ONE call: stream every Event with room_seq > from_seq — the owner
  // replays the gap from the log, then continues live, with no window where events are missed.
  // This unifies fresh-join-subscribe and resume.
  rpc Subscribe(SubscribeRequest) returns (stream Event);

  // Ephemeral (lossy) tier. Fire-and-forget fan-out; no ack, no dedup.
  rpc Broadcast(BroadcastRequest) returns (BroadcastResponse);
}
```

- `Commit(room_id, client_id, client_seq, EventBody)` → `Event` **or** `Nack`. The committed
  `Event` is *not* returned as the success payload that the client waits on — **fan-out is the
  ack**: the event arrives at the client via its `Subscribe` stream (matched by
  `origin_client_seq`). The unary response just tells the gateway "persisted / rejected / not-owner".
- `Subscribe(room_id, from_seq)` is the heart of the relay. Implementation note: the owner must
  attach to `fanout` **before** reading the log up to head, then dedupe the overlap by `room_seq`,
  so the live tail can't slip through the gap between replay and subscription.
- `GetSnapshot(room_id)` → `(room_seq, RoomState)` for the initial state on a fresh join.

Why a stream per (gateway, room) rather than Redis pub/sub? Phase-1 `fanout` is in-memory and
per-process, so the owner streaming to remote subscribers is the natural cross-process path. Redis
fan-out (a gateway subscribes to a room channel without holding a stream to each owner) is the
**scale evolution**, added behind the same relay abstraction later — and it's safe to lose Redis
events because the durable log + recovery fills any gap.

## 4. Connection lifecycle (client ↔ gateway)

1. **Open + auth.** Client dials the WS. The gateway runs the `Authenticator` (stub: trust a dev
   token/header) → an authenticated principal. Connection state: the set of joined rooms + their
   live subscriptions.
2. **Join.** `Join{room_id, from_seq, client_id?}` →
   - *fresh* (`from_seq=0`, no `client_id`): gateway **assigns a `client_id`**, `GetSnapshot` →
     send `Joined{client_id, current_seq=S, snapshot}`, then `Subscribe(room, from_seq=S)` and relay
     `Event`s `S+1…` as they arrive.
   - *resume* (`from_seq>0`, prior `client_id`): validate the `client_id` belongs to this principal,
     send `Joined{client_id, current_seq}` (snapshot omitted on a shallow resume), `Subscribe(room,
     from_seq)` → the owner replays `from_seq+1…head` from the log then goes live. No snapshot, no
     reload — pure cursor catch-up.
3. **Commit.** `Commit{room_id, client_seq, body}` → enforce the client has joined the room (else
   `Nack{NOT_JOINED}`) → `RoomService.Commit`. On `Nack`, forward it. On not-owner, re-resolve and
   retry (§6). Success is silent here — the `Event` returns via the subscription.
4. **Broadcast.** `Broadcast{room_id, body}` → `RoomService.Broadcast` → relayed to room
   subscribers as `Ephemeral`. Lossy by design.
5. **Leave.** Drop the room's subscription + joined-state for this connection.
6. **Ping/Pong.** App-level `Ping`→`Pong` for RTT, *plus* WS-level ping/read-deadline for liveness.
7. **Close.** Tear down all subscriptions and joined-state. The durable log is untouched; the
   client reconnects and resumes from its `lastSeq`.

## 5. Recovery model (single disposable WS)

This is the backbone's central promise — **resilience via recovery, not a second connection** —
realized end to end:

- The SDK holds **one** WS. On any drop it reconnects and sends `Join{from_seq=lastSeq,
  client_id=<its id>}` per room. The `Subscribe(from_seq)` replay closes the gap idempotently
  (events are keyed by `room_seq`; re-applying a seen one is a no-op).
- **Stable identity across reconnects.** Dedup is `(client_id, client_seq)`; exactly-once across a
  reconnect requires the client to keep the *same* `client_id` and continue its `client_seq`. So the
  `client_id` must round-trip. **Contract addition (non-breaking):** add `string client_id = 3;` to
  `Join` (proto3 field addition passes `buf breaking`). Server assigns on first join; client echoes
  it on resume.
- **Ack-after-persist** is preserved: a `Commit`'s `Event` only ever reaches the client *after* the
  owner durably appended it, because the event is sourced from the same fan-out the durable write
  triggers.

## 6. Routing & failover handling

- **Resolve.** For every Join/Commit/Broadcast the gateway calls `coord.Current(room)` → owner
  addr, and forwards over the pooled RPC client.
- **Not-owner / re-home.** If `coord.Current` returns no owner (claim in flight) or the owner RPC
  returns `FAILED_PRECONDITION` (it lost the lease mid-flight — the runtime's `ErrNotOwner`), the
  gateway re-resolves and retries with bounded backoff. This is exactly the `ErrNotOwner` path that
  is structurally unreachable in the single-threaded owner DST but becomes live here.
- **Subscription failover.** When a room re-homes, the gateway's `Subscribe` stream to the old owner
  errors; the gateway re-resolves and re-subscribes to the new owner `from_seq=lastDelivered` — no
  client-visible gap.

## 7. FROZEN / LIVE (`RoomStatus`)

When durable commits can't proceed (quorum loss / re-homing in progress — no resolvable owner), the
gateway sends `RoomStatus{FROZEN}` to affected clients; the SDK pauses commits (buffers locally).
Once an owner is resolvable again, `RoomStatus{LIVE}`. Phase-1 trigger: `coord.Current` returns no
live owner *and* a claim hasn't yet succeeded; cleared on the next successful resolve.

## 8. Auth (pluggable)

```go
type Authenticator interface {
    // Authenticate verifies the handshake and returns the principal (and any pre-bound client_id).
    Authenticate(ctx context.Context, r *http.Request) (Principal, error)
}
```

Phase-1 impl trusts a dev token/header and derives a principal; `client_id` assignment lives in the
gateway. Real JWT verification against Uprio auth slots in behind this interface at app integration
— no transport changes.

## 9. Liveness & backpressure

- coder/websocket read deadline + periodic WS ping detects dead sockets; app-level `Ping/Pong`
  gives RTT and an application keepalive.
- Per-connection write pump with a bounded queue; a client too slow to drain **durable** events is
  disconnected (it will recover from `lastSeq` on reconnect) rather than allowed to balloon memory.
  Ephemeral events are dropped first under pressure (they're lossy by contract).

## 10. What this unlocks for the DST (Phase-1 exit)

With both networks present, the deterministic-simulation matrix extends from "owner-only" to the
full path:

- **client ↔ gateway** faults: drop / delay / reorder / duplicate, reconnect storms → assert
  recovery converges with no reload / loss / dup.
- **gateway ↔ owner** faults + **routing** under failover: kill an owner mid-session, partition a
  gateway from an owner → assert re-resolve + re-subscribe, at-most-one-owner, exactly-once.
- This is where the deferred `sim.Network` fault injection and the `ErrNotOwner` routing assertion
  finally attach. Green over thousands of seeds = **Phase-1 exit**.

---

## 11. Incremental PR plan

Small, independently reviewable+mergeable, lowest-risk first. Each is its own PR; review→merge one
before stacking the next where there's a dependency.

| # | PR | Scope | Depends on |
|---|----|-------|-----------|
| G1 | **owner RPC contract** | `owner.proto` `RoomService` + Connect-Go plugin in `buf.gen.yaml` + codegen wiring. No impl. `buf breaking` stays green (additive). | — |
| G2 | **owner RPC server** | Adapter wrapping `roomruntime.Runtime`: Commit / GetSnapshot / Broadcast handlers + the Subscribe replay-then-live bridge. In-process Connect tests. | G1 |
| G3 | **owner directory address** | Resolve owner node-id → RPC addr (add `Addr` to the directory or a membership registry); `coord.Current` usable for routing. | G1 |
| G4 | **owner RPC client + locator** | Gateway-side `OwnerClient` (pooled) + `OwnerLocator` (room → owner via G3). Tested against G2's in-process server. | G2, G3 |
| G5 | **WS transport skeleton** | coder/websocket accept loop, `Authenticator` stub, protobuf frame read/write, Ping/Pong, clean teardown. No room logic. | — |
| G6 | **Join (fresh) + relay** | `Join{from_seq=0}` → GetSnapshot → assign `client_id` → `Joined`; Subscribe → relay live `Event`s. | G4, G5 |
| G7 | **Commit + Nack** | `Commit` → owner; fan-out-is-the-ack; failures → `Nack`; enforce `NOT_JOINED`. | G6 |
| G8 | **Broadcast (ephemeral)** | `Broadcast` → owner → `Ephemeral` relay. | G6 |
| G9 | **Resume / recovery** | additive `Join.client_id`; `Join{from_seq>0}` replay-then-live; stable id + dedup continuity across reconnect. | G7 |
| G10 | **RoomStatus + routing under failover** | FROZEN/LIVE; re-resolve on not-owner; re-subscribe on stream failover. | G7 |
| G11 | **full client↔gateway↔owner DST** | extend the sim matrix to both networks + routing (the Phase-1 exit chaos suite). | G9, G10 |

After G11: minimal node binary (`cmd/aether-node`) wiring it together, then the **TS SDK**
(Connection / commit / broadcast / recovery + `useRoomState` hooks) against this gateway.

## 12. Open questions / deferred

- **Directory address** (G3): extend `coord.Lease` with `Addr`, or a separate membership registry?
  Leaning registry, so the lease stays a pure fencing primitive — to confirm at G3.
- **Redis fan-out**: deferred; Subscribe-stream relay first, Redis behind the same abstraction when
  gateway↔owner stream count becomes a scale concern.
- **Admin override** (`NACK_REASON_OVERRIDDEN`) and **presence**: Phase 2 features; the contract
  already reserves the shapes.
- **Rate limiting** (`NACK_REASON_RATE_LIMITED`): gateway-side; stub now, real policy later.
