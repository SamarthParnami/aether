# Aether — Engineering Brief

> *The element that fills all space and connects everything.* A real-time state backbone for Inclass.
> **Full design:** [01-design-backbone.md](01-design-backbone.md)

## The problem (verified against the code, not assumed)
Today's `socket-server` is **not** a dumb relay — it persists state to Redis (`class:{classId}`) +
MongoDB, runs socket.io with the **Redis Streams adapter** (so it scales horizontally + fans out via
Redis), and enforces a "tutor-in-control" check. What it **lacks is the foundation**:
- **No ordered event log / seq cursor → no content recovery.** Mic/screen-share *are* restored on
  reconnect; **slides/content are not** (no sequence numbers, no replay) → clients can silently fall out
  of sync on content.
- **No room ownership / leader election / coordinated failover.** Scales via the adapter, but no instance
  *owns* a class → an instance dying is an uncoordinated reconnect, not a clean handoff.
- **No first-class admin override** (4 user types, no ADMIN; control is tutor-only).
- **Push-only**, tutor→students; no authoritative state a client can rebuild from.

Aether supplies the missing foundation: an authoritative per-room ordered log, seq-cursor recovery, room
ownership with coordinated re-homing, and admin override as a first-class event.

## The shape of the system (one diagram)
```
browser ─ ONE WebSocket ─▶ gateway (stateless, multi-AZ) ─▶ room-runtime (owner, multi-AZ)
                                                                  │
                              Redis (coord · stream/ephem)  +  DynamoDB per-room log = TRUTH
                              (accelerators, disposable)        (regional, multi-AZ quorum)
```

## The 5 things to internalize
1. **One disposable WebSocket per client. Resilience = recovery, not a 2nd connection.** Every drop
   (wifi, sleep, owner death, AZ loss) is handled identically: `reconnect → RESUME{lastSeq} → catch up`.
   The UI binds to a `Connection` abstraction and never sees a drop.
2. **The per-room quorum log is the ONLY truth. Everything else is disposable.** Socket nodes and Redis
   hold nothing that can't be rebuilt from the log in <1s. That's why killing any of them is survivable.
3. **"Real" = quorum-committed.** Owner does **ack-after-persist** for durable events → a client's
   `lastSeq` never exceeds the log → recovery is lossless. Everything before the append is replayable;
   everything after is just propagation.
4. **Two QoS tiers, visible in the API.** `room.commit()` = durable/ordered/acked (slide, mute, admin).
   `room.broadcast()` = ephemeral/lossy/fast (cursors, presence). You can't send one as the other.
5. **The room is the atom** — unit of ownership, ordering, sharding, blast-radius. One owner per room
   (lease + conditional-write guard against split-brain). Owner/AZ dies → room **re-homes** automatically
   and rebuilds from the log. *A deploy is a failover* — so graceful drain matters.

## Headline decisions (rationale in §7 of the design doc)
- **DynamoDB** for the log (`partitionKey=roomId, sortKey=roomSeq`) — native per-room seek + conditional
  write = free split-brain guard. **Not Kinesis** (orders by shard, can't seek per-room).
- **Redis = accelerator, never truth** — split by role for blast-radius; degrade-to-DB / direct-push on
  failure; coordination fails **safe** (freeze, never split-brain).
- **Single cloud, single region (AWS Mumbai), multi-AZ.** No cross-region, no multi-cloud, no ECS-backup.
  All-India is served <100ms by one region + satisfies DPDP. We **consciously accept** the rare
  regional-outage risk; we keep the *seam* (log-adapter + home-concept) to add Hyderabad warm-standby
  later if ever needed.
- **Fan-out is always PUSH** (Redis Stream tail; owner direct-push if Redis is down). Dynamo is read
  event-driven, **never polled** — this is what scales to 100k rooms.

## Monorepo (Yarn 4 + Turborepo)
`protocol/` (the frozen contract + shared reducer — imported by client AND server) is the spine. Hard
rule: **no server package imported by a client package.** `room-core` = pure logic (most-tested, no I/O);
`gateway`/`room-runtime` = thin I/O wrappers; `client-sdk` hides all recovery; `test-kit`+`e2e` = chaos rig.

## Deploy (Devtron / EKS, Mumbai, 3 AZs)
Pods spread **hard** across AZs + PDBs (**fix the `minAvailable:""` trap**) + **N-1 AZ headroom**.
`room-runtime` = worker-shaped rolling; **graceful `preStop` releases the lease + snapshots** so deploys
re-home in <1s, not 6s. No `tsc` at startup; `512Mi`/`1Gi` resources (avoid the `0.2Gi` milli-bytes bug).

## What Phase 1 ships (no UI, no features, no media)
`protocol`, `room-core`, `log-adapter`, `coordination`, `client-sdk` (durable path), `gateway`,
`room-runtime`, and the **chaos harness**. **Done = the chaos suite is green:** kill gateway / owner / AZ,
partition, wipe Redis, lose quorum, reconnect-storm, roll a deploy → every client converges with
**no reload, no dupes, no lost durable events**. Fault injection is a first-class test input.

## First build artifact
Freeze **`protocol/`** — the event envelope (`{roomSeq, clientId, clientSeq, type, payload}`), the
commit/ack/resume wire messages, and the reducer signature. That unblocks parallel client + server work.

## Open decisions before/early in Phase 1
Sharding map (rooms→owners→shards + directory) · room granularity (session vs breakout vs weekly class) ·
snapshot cadence · lease detection window (the SLA dial) · admin-override semantics (force vs revoke).
