/**
 * Room — a client's live, self-healing view of one Aether room.
 *
 * `connect()` dials a {@link Transport}, sends a Join, and resolves once the gateway replies Joined
 * (client_id + snapshot + cursor). The Room folds each durable Event into a materialized key/value
 * state — the client-side mirror of the Go `roomcore` reducer (shared golden vectors keep them in
 * lockstep) — and notifies subscribers. `getState()`/`subscribe()` are the useState-like surface the
 * React hook (S5) builds on.
 *
 * Writing: `commit()` assigns a per-client monotonic `client_seq`, buffers the commit, and resolves
 * when the committed Event fans back to us — "fan-out is the ack". `(client_id, client_seq)` is the
 * dedup key, so a resend of an already-applied commit is a server-side no-op. `broadcast()` is the
 * lossy ephemeral tier.
 *
 * Recovery has two paths, both of which re-drive un-acked commits, and both exactly-once because of
 * the dedup key:
 *  - **Reconnect (S4a):** a dropped socket → re-dial with backoff → re-Join from the cursor → resend.
 *  - **In-place (S4b):** the socket stays up but the room's owner failed over — surfaced as
 *    `RoomStatus` FROZEN→LIVE. Un-acked commits are re-driven by a retry timer that fires regardless
 *    of FROZEN/LIVE. That "don't gate resends on LIVE" rule is load-bearing: the gateway relay only
 *    signals LIVE after it sees an event past the cursor, and that event may be our own commit — so
 *    waiting for LIVE before resending would deadlock (the property the Go DST commit-chaos gate
 *    proves). The timer breaks it. Only an explicit `close()` stops recovery and rejects what's live.
 *  - **Failed Join:** the gateway can reject a Join with an `Error` frame while leaving the socket
 *    OPEN (its no-owner path), so no drop is observed. A transient code re-dials through the backoff;
 *    `INVALID` is terminal and fails `connect()`.
 *
 * Commits go out strictly ONE AT A TIME (see `sendHead`) — the owner's per-client high-water dedup
 * makes an out-of-order arrival unrecoverable, so a queue is the only safe shape. A commit that can
 * never be acked (already deduped by the owner, so never re-fanned) fails with
 * {@link CommitUnconfirmedError} rather than retrying forever.
 */
import {
  ClientMessageSchema,
  emptyState,
  type EphemeralBody,
  type Event,
  type EventBody,
  fold,
  type Joined,
  type MaterializedState,
  type Nack,
  NackReason,
  type RoomStatus,
  RoomStatus_Status,
  type ServerMessage,
} from '@aether/protocol';
import { create, type MessageInitShape } from '@bufbuild/protobuf';

import { decodeServerMessage, encodeClientMessage } from './codec.js';
import type { Dialer, Transport } from './transport.js';

/** The `body` oneof init accepted by `create(ClientMessageSchema, { body })` (a concrete case). */
type ClientBody = NonNullable<MessageInitShape<typeof ClientMessageSchema>['body']>;

export interface RoomOptions {
  /** Mints a fresh transport per (re)connect — the Room re-dials through it on reconnect. */
  dial: Dialer;
  /** The room to join. */
  roomId: string;
  /**
   * A stable per-session value the SDK persists across reconnects; the gateway derives a stable
   * client_id from it, so recovery re-attaches to the same identity. Generated once if omitted.
   */
  sessionNonce?: string;
  /** Invoked on a server Error frame, or when a room_seq gap is detected (code `seq_gap`). */
  onError?: (code: string, message: string) => void;
  /** Invoked for each received Ephemeral (another client's broadcast). Lossy — best-effort only. */
  onEphemeral?: (originClientId: string, body: EphemeralBody) => void;
  /** Invoked when the room's live/frozen status changes (false = FROZEN / re-homing). */
  onStatus?: (live: boolean) => void;
  /** Backoff before reconnect attempt `n` (0-based), in ms. Default: exponential 100ms→5s cap. */
  reconnectDelayMs?: (attempt: number) => number;
  /** Interval to re-drive un-acked commits while any are outstanding, in ms. Default 5000; 0 disables. */
  commitRetryMs?: number;
  /**
   * How many times a single commit may be sent before it is rejected with a
   * {@link CommitUnconfirmedError}. Default 10; 0 means never give up (the pre-1.0 behaviour, which
   * can loop forever against a commit the owner has already deduped).
   */
  commitMaxAttempts?: number;
}

/** Notified after each applied state change; read the latest via {@link Room.getState}. */
export type StateListener = () => void;

/** Rejection of a {@link Room.commit} the server refused, carrying the wire {@link NackReason}. */
export class NackError extends Error {
  constructor(readonly reason: NackReason) {
    super(`commit rejected: ${NackReason[reason] ?? String(reason)}`);
    this.name = 'NackError';
  }
}

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
}

interface Outstanding extends Waiter {
  body: EventBody; // retained so recovery can re-send it
  attempts: number; // sends so far, so an unackable commit fails loudly instead of looping forever
}

/**
 * A commit that was re-sent {@link RoomOptions.commitMaxAttempts} times without ever being acked.
 *
 * This is the honest answer to an ambiguity the wire protocol cannot currently resolve: if a
 * reconnect's Join returns a snapshot that already subsumes the commit, the owner dedups every
 * resend and never re-fans the event, so "fan-out is the ack" has no path left. `Joined` carries no
 * per-client dedup high-water, so the SDK cannot tell "already durable" from "never applied".
 * The write is in an UNKNOWN state — the app must reconcile against room state, not retry blindly.
 */
export class CommitUnconfirmedError extends Error {
  constructor(readonly clientSeq: bigint) {
    super(
      `commit ${clientSeq} was never acknowledged after the maximum number of attempts; ` +
        `it may or may not be durable — reconcile against room state`,
    );
    this.name = 'CommitUnconfirmedError';
  }
}

const defaultBackoff = (attempt: number): number => Math.min(5000, 100 * 2 ** attempt);

export class Room {
  private readonly dial: Dialer;
  private readonly roomId: string;
  private readonly nonce: string;
  private readonly onError: ((code: string, message: string) => void) | undefined;
  private readonly onEphemeral: ((originClientId: string, body: EphemeralBody) => void) | undefined;
  private readonly onStatus: ((live: boolean) => void) | undefined;
  private readonly reconnectDelayMs: (attempt: number) => number;
  private readonly commitRetryMs: number;
  private readonly commitMaxAttempts: number;

  private transport: Transport | undefined;
  private state: MaterializedState = emptyState();
  private cursor = 0n; // highest applied room_seq — the resume point
  private version = 0; // bumped on every state change — the useSyncExternalStore snapshot (S5)
  private clientIdValue: string | undefined;
  private clientSeq = 0n; // last assigned per-client commit sequence
  private liveValue = false;

  private closedByUser = false;
  private joinPending = false; // a Join is on the wire and no Joined has come back yet
  private inFlight: bigint | undefined; // the one commit currently on the wire (see sendHead)
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;

  private connecting: Promise<void> | undefined; // the one connect() promise; makes connect() idempotent
  private joinWaiter: Waiter | undefined;
  private readonly pings = new Map<string, Waiter>();
  private readonly outstanding = new Map<bigint, Outstanding>(); // client_seq → un-acked commit
  private readonly listeners = new Set<StateListener>();

  constructor(opts: RoomOptions) {
    this.dial = opts.dial;
    this.roomId = opts.roomId;
    this.nonce = opts.sessionNonce ?? crypto.randomUUID();
    this.onError = opts.onError;
    this.onEphemeral = opts.onEphemeral;
    this.onStatus = opts.onStatus;
    this.reconnectDelayMs = opts.reconnectDelayMs ?? defaultBackoff;
    this.commitRetryMs = opts.commitRetryMs ?? 5000;
    this.commitMaxAttempts = opts.commitMaxAttempts ?? 10;
  }

  /**
   * Dial, Join, and resolve on the first Joined. Drops thereafter auto-reconnect (see class docs).
   *
   * Idempotent: repeat calls return the SAME promise rather than dialing again. A second dial used
   * to overwrite `this.transport`, orphaning a fully-wired socket that nobody would ever close — and
   * when that orphan later dropped, its `onTransportClose` cleared the pointer to the *healthy*
   * connection, black-holing writes and reporting FROZEN over a live link. It also overwrote
   * `joinWaiter`, so the first caller's promise could never settle.
   */
  connect(): Promise<void> {
    if (this.closedByUser) return Promise.reject(new Error('room closed'));
    if (this.connecting) return this.connecting;
    this.connecting = new Promise<void>((resolve, reject) => {
      this.joinWaiter = { resolve, reject };
    });
    void this.openConnection();
    return this.connecting;
  }

  /** The materialized room state (last-write-wins key/value). Treat as read-only. */
  getState(): ReadonlyMap<string, Uint8Array> {
    return this.state;
  }

  /** The gateway-assigned client_id (stable across reconnects of this session), once joined. */
  clientId(): string | undefined {
    return this.clientIdValue;
  }

  /** The highest room_seq applied — the resume cursor. */
  currentSeq(): bigint {
    return this.cursor;
  }

  /** Whether the room is currently LIVE (true) or FROZEN / re-homing (false). */
  isLive(): boolean {
    return this.liveValue;
  }

  /** A monotonically increasing state version; changes iff {@link getState} changed. */
  getVersion(): number {
    return this.version;
  }

  /** Subscribe to state changes; returns an unsubscribe. */
  subscribe(fn: StateListener): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  /**
   * Commit a durable event. Resolves when the committed Event fans back to us (fan-out is the ack);
   * rejects with a {@link NackError} if the server refuses it (except `UNAVAILABLE`, which is
   * transient — the commit stays buffered and is re-driven by recovery). Assigns the next per-client
   * `client_seq`, which together with `client_id` is the server's exactly-once dedup key.
   */
  commit(body: EventBody): Promise<void> {
    // A closed Room can never re-drive anything: send() is a no-op with no transport and armRetry()
    // refuses to arm, so parking a waiter here would leave the promise unsettled forever. Reject
    // before burning a client_seq. Note `closedByUser` is also set by a terminal INVALID Join, so
    // this covers an app whose connect() failed and which never called close() itself.
    if (this.closedByUser) return Promise.reject(new Error('room closed'));
    this.clientSeq += 1n;
    const seq = this.clientSeq;
    return new Promise<void>((resolve, reject) => {
      this.outstanding.set(seq, { body, resolve, reject, attempts: 0 });
      this.sendHead(); // queued behind any earlier un-acked commit — see sendHead
      this.armRetry();
    });
  }

  /** Fire-and-forget an ephemeral broadcast (cursors, presence): lossy, unordered, never acked. */
  broadcast(body: EphemeralBody): void {
    this.send({ case: 'broadcast', value: { roomId: this.roomId, body } });
  }

  /**
   * App-level keepalive/RTT probe: resolves when the matching Pong returns, rejects if the connection
   * drops first. `id` must be unique among in-flight pings (a duplicate rejects rather than orphaning
   * the earlier waiter), and there must be a live transport.
   */
  ping(id: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.pings.has(id)) {
        reject(new Error(`ping id ${id} already in flight`));
        return;
      }
      if (!this.transport) {
        reject(new Error('ping: not connected'));
        return;
      }
      this.pings.set(id, { resolve, reject });
      this.send({ case: 'ping', value: { id } });
    });
  }

  /** Stop reconnecting, close the connection, and reject anything in flight. Terminal. */
  close(): void {
    this.closedByUser = true;
    this.clearTimer('reconnectTimer');
    this.clearTimer('retryTimer');
    this.transport?.close();
    this.dropTransport();
    const err = new Error('room closed');
    this.joinWaiter?.reject(err);
    this.joinWaiter = undefined;
    rejectAll(this.pings.values(), err);
    this.pings.clear();
    rejectAll(this.outstanding.values(), err);
    this.outstanding.clear();
  }

  // ===== connection lifecycle =====

  private async openConnection(): Promise<void> {
    if (this.closedByUser) return;
    // Defence in depth: never assign over a live transport without closing it. connect() is now
    // idempotent and the reconnect path is guarded by reconnectTimer, so this should be unreachable —
    // but an orphaned socket stays wired to our handlers, and its eventual drop would clear the
    // pointer to the healthy one.
    this.transport?.close();
    const t = this.dial();
    this.transport = t;
    try {
      await t.open({
        onMessage: (d) => this.handle(decodeServerMessage(d)),
        onClose: (r) => this.onTransportClose(r),
      });
      this.sendJoin(this.cursor); // 0 on first connect, cursor on resume
    } catch {
      this.dropTransport(); // a failed dial must not leave a dead transport behind
      this.scheduleReconnect();
    }
  }

  private onTransportClose(reason?: Error): void {
    // Pings are transient RTT probes — fail them. Outstanding commits SURVIVE for the resend on
    // reconnect (the whole point of recovery). A user close() is handled in close(), not here.
    this.dropTransport();
    this.setLive(false);
    rejectAll(this.pings.values(), reason ?? new Error('connection closed'));
    this.pings.clear();
    this.scheduleReconnect();
  }

  /**
   * Forget the current transport. Load-bearing for `ping()`, whose liveness check is
   * `this.transport`: leaving a dead transport in place made a ping issued while disconnected pass
   * the guard, park a waiter that never settled, and poison that ping id for the Room's lifetime.
   * Also clears the in-flight marker so the next connection re-sends the head commit.
   */
  private dropTransport(): void {
    this.transport = undefined;
    this.inFlight = undefined;
  }

  private scheduleReconnect(): void {
    if (this.closedByUser || this.reconnectTimer) return;
    const delay = this.reconnectDelayMs(this.reconnectAttempt++);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.openConnection();
    }, delay);
  }

  // ===== message handling =====

  private handle(m: ServerMessage): void {
    switch (m.body.case) {
      case 'joined':
        this.onJoined(m.body.value);
        break;
      case 'event':
        this.onEvent(m.body.value);
        break;
      case 'nack':
        this.onNack(m.body.value);
        break;
      case 'roomStatus':
        this.onRoomStatus(m.body.value);
        break;
      case 'ephemeral':
        if (m.body.value.roomId === this.roomId && m.body.value.body) {
          this.onEphemeral?.(m.body.value.originClientId, m.body.value.body);
        }
        break;
      case 'pong': {
        const w = this.pings.get(m.body.value.id);
        if (w) {
          this.pings.delete(m.body.value.id);
          w.resolve();
        }
        break;
      }
      case 'error':
        this.onError?.(m.body.value.code, m.body.value.message);
        this.onServerError(m.body.value.code, m.body.value.message);
        break;
      default:
        break;
    }
  }

  /**
   * A server `Error` frame arriving while our Join is still unanswered means the Join FAILED — and
   * the gateway leaves the socket wide open when it happens (`handleJoin` replies Error and returns
   * without starting a relay), so no `onClose` fires and nothing else would ever retry. Left alone
   * the Room sat un-joined forever on a healthy-looking socket, `connect()` never settled, and every
   * later commit drew `Nack{NOT_JOINED}`.
   *
   * The gateway only emits two codes, and they split cleanly:
   *   UNAVAILABLE — no reachable owner / snapshot fetch failed: transient, so drop this connection
   *                 and let the reconnect backoff ride out the re-home.
   *   INVALID     — malformed frame, missing or mismatched session_nonce: retrying cannot help, so
   *                 fail `connect()` and stop.
   * An unknown code is treated as transient: retrying is recoverable, giving up is not.
   */
  private onServerError(code: string, message: string): void {
    if (!this.joinPending || this.closedByUser) return;
    this.joinPending = false;

    if (code === 'INVALID') {
      this.closedByUser = true; // terminal: stop recovery, this session can never join
      this.clearTimer('reconnectTimer');
      this.clearTimer('retryTimer');
      this.transport?.close();
      this.dropTransport();
      const err = new Error(`join rejected: ${code}: ${message}`);
      this.joinWaiter?.reject(err);
      this.joinWaiter = undefined;
      rejectAll(this.pings.values(), err);
      this.pings.clear();
      rejectAll(this.outstanding.values(), err);
      this.outstanding.clear();
      return;
    }

    // Transient: tear the connection down so the normal reconnect path retries with backoff.
    this.transport?.close();
    this.dropTransport();
    this.setLive(false);
    this.scheduleReconnect();
  }

  private onJoined(j: Joined): void {
    this.clientIdValue = j.clientId;
    this.joinPending = false;
    this.reconnectAttempt = 0; // a successful join resets backoff
    if (j.snapshot) {
      // Fresh join, or a deep resume where our cursor fell below the log's floor: adopt the snapshot
      // wholesale as the new base. Copy each value (protobuf-es decodes `bytes` as a view aliasing the
      // frame buffer) — matching the reducer's `fold`, so snapshot and folded state behave identically.
      this.state = emptyState();
      for (const [k, v] of Object.entries(j.snapshot.state?.entries ?? {}))
        this.state.set(k, v.slice());
      // The cursor is exactly the snapshot's materialized point. We deliberately do NOT advance it to
      // j.current_seq: current_seq can be the log head, ahead of the snapshot, and the relay streams
      // the events between them — jumping the cursor there would silently drop that backfill.
      this.cursor = j.snapshot.roomSeq;
    }
    // No snapshot ⇒ a resume from our cursor: keep the state we already have; the gateway streams the
    // events after `cursor` to backfill.
    this.setLive(true);
    this.bump();
    this.joinWaiter?.resolve(); // resolves the first connect(); a no-op on later reconnects
    this.joinWaiter = undefined;
    this.resendOutstanding(); // re-drive un-acked commits through the (re)connected owner
  }

  private onEvent(e: Event): void {
    if (e.roomId !== this.roomId) return;
    // Fan-out is the ack: our own committed event returning resolves the outstanding commit. This runs
    // BEFORE the replay/gap gates below on purpose — the ack signals DURABILITY (the commit is in the
    // log), not local materialization. So an already-applied commit replayed during recovery still
    // acks even though its room_seq ≤ cursor, and a deep-resume that re-fans a subsumed commit resolves
    // it too. Local state may briefly lag the ack in a gap; the cursor-resume backfill reconciles it.
    if (e.originClientId === this.clientIdValue) this.ackCommit(e.originClientSeq);

    const expected = this.cursor + 1n;
    if (e.roomSeq < expected) return; // already applied — an idempotent replay
    if (e.roomSeq > expected) {
      // A gap: the log skipped ahead. Applying out of order would diverge from the Go reducer, so we
      // surface it and wait — a reconnect re-Joins from the cursor and the gateway backfills.
      this.onError?.('seq_gap', `expected room_seq ${expected}, got ${e.roomSeq}`);
      return;
    }
    if (e.body) fold(this.state, e.body);
    this.cursor = e.roomSeq;
    this.bump();
  }

  private onNack(n: Nack): void {
    if (n.roomId !== this.roomId) return;
    const o = this.outstanding.get(n.clientSeq);
    if (!o) return;
    if (this.inFlight === n.clientSeq) this.inFlight = undefined;
    // Both reasons the gateway can actually produce are TRANSIENT, so both stay buffered for the
    // retry timer / reconnect to re-drive:
    //   UNAVAILABLE — no reachable owner, i.e. mid-re-home.
    //   NOT_JOINED  — this connection has no relay for the room yet (its Join failed or is still in
    //                 flight). Rejecting here turned a failover the design is built to survive into
    //                 a permanent write failure for a commit the server never refused on the merits.
    // Every other reason is a real refusal → terminal.
    if (n.reason === NackReason.UNAVAILABLE || n.reason === NackReason.NOT_JOINED) {
      this.armRetry();
      return;
    }
    this.outstanding.delete(n.clientSeq);
    this.stopRetryIfDrained();
    o.reject(new NackError(n.reason));
    this.sendHead();
  }

  private onRoomStatus(rs: RoomStatus): void {
    if (rs.roomId !== this.roomId) return;
    const live = rs.status === RoomStatus_Status.LIVE;
    this.setLive(live);
    // On the LIVE edge, re-drive immediately (a fast path over the retry timer).
    if (live) this.resendOutstanding();
  }

  private ackCommit(clientSeq: bigint): void {
    const o = this.outstanding.get(clientSeq);
    if (o) {
      this.outstanding.delete(clientSeq);
      if (this.inFlight === clientSeq) this.inFlight = undefined;
      this.stopRetryIfDrained();
      o.resolve();
      this.sendHead(); // the next queued commit may now go out
    }
  }

  /**
   * Send the lowest un-acked commit — and only that one.
   *
   * Strict one-at-a-time is load-bearing, not throttling. The owner dedups on a per-client
   * HIGH-WATER mark, so once client_seq N is applied every seq < N becomes permanently unappliable.
   * Letting N+1 reach a new owner while N is still buffered (say N drew a transient UNAVAILABLE
   * mid-re-home) burns a hole: N's resends are then silently swallowed as duplicates, no Event ever
   * fans back, and N's data never reaches the log — a lost write under an exactly-once contract.
   * Keeping exactly one commit on the wire makes that hole unconstructible, and mirrors the
   * one-op-in-flight-per-client contract the Go DST already models.
   */
  private sendHead(): void {
    if (this.inFlight !== undefined) return;
    const head = this.outstanding.entries().next();
    if (head.done) return;
    const [seq, o] = head.value;

    // A commit the owner has already deduped can never be acked by fan-out (see
    // CommitUnconfirmedError). Give up loudly rather than resending forever.
    if (this.commitMaxAttempts > 0 && o.attempts >= this.commitMaxAttempts) {
      this.outstanding.delete(seq);
      o.reject(new CommitUnconfirmedError(seq));
      this.stopRetryIfDrained();
      this.sendHead(); // unblock whatever queued behind it
      return;
    }

    o.attempts += 1;
    this.inFlight = seq;
    this.sendCommit(seq, o.body);
  }

  /** Re-drive the head commit (recovery / retry timer): drop the in-flight marker, then re-send. */
  private resendOutstanding(): void {
    this.inFlight = undefined;
    this.sendHead();
  }

  // ===== commit retry timer (in-place, drop-less recovery) =====

  private armRetry(): void {
    if (this.closedByUser || this.retryTimer || this.commitRetryMs <= 0) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      if (this.outstanding.size === 0) return;
      this.resendOutstanding();
      this.armRetry(); // keep re-driving while anything is un-acked
    }, this.commitRetryMs);
  }

  private stopRetryIfDrained(): void {
    if (this.outstanding.size === 0) this.clearTimer('retryTimer');
  }

  // ===== send helpers =====

  private send(body: ClientBody): void {
    try {
      this.transport?.send(encodeClientMessage(create(ClientMessageSchema, { body })));
    } catch {
      // Best-effort: a send racing a transport close/reopen is fine — recovery re-drives it.
    }
  }

  private sendJoin(fromSeq: bigint): void {
    this.joinPending = true;
    this.send({ case: 'join', value: { roomId: this.roomId, fromSeq, sessionNonce: this.nonce } });
  }

  private sendCommit(clientSeq: bigint, body: EventBody): void {
    this.send({ case: 'commit', value: { roomId: this.roomId, clientSeq, body } });
  }

  private setLive(live: boolean): void {
    if (this.liveValue === live) return;
    this.liveValue = live;
    this.onStatus?.(live);
  }

  private clearTimer(which: 'reconnectTimer' | 'retryTimer'): void {
    const t = this[which];
    if (t !== undefined) {
      clearTimeout(t);
      this[which] = undefined;
    }
  }

  private bump(): void {
    this.version++;
    for (const fn of this.listeners) fn();
  }
}

/** Reject a batch of pending waiters with the same error. */
function rejectAll(waiters: Iterable<Waiter>, err: Error): void {
  for (const w of waiters) w.reject(err);
}
