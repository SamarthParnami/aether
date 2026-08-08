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
 * Recovery (S4a): a dropped connection is NOT fatal. The Room re-dials with backoff and re-Joins from
 * its cursor (`from_seq = cursor`) — the gateway resumes the event stream (or hands back a fresh
 * snapshot on a deep resume) — then re-sends every still-outstanding commit. Because commits dedup on
 * `(client_id, client_seq)`, replaying ones that already applied before the drop is exactly-once, not
 * a double-apply — the same property the Go DST commit-chaos gate proves. Only an explicit `close()`
 * stops reconnection and rejects what's in flight. (In-place FROZEN/LIVE recovery without a socket
 * drop, and the retry-timer backstop, are S4b.)
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
  /** Backoff before reconnect attempt `n` (0-based), in ms. Default: exponential 100ms→5s cap. */
  reconnectDelayMs?: (attempt: number) => number;
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
}

const defaultBackoff = (attempt: number): number => Math.min(5000, 100 * 2 ** attempt);

export class Room {
  private readonly dial: Dialer;
  private readonly roomId: string;
  private readonly nonce: string;
  private readonly onError: ((code: string, message: string) => void) | undefined;
  private readonly onEphemeral: ((originClientId: string, body: EphemeralBody) => void) | undefined;
  private readonly reconnectDelayMs: (attempt: number) => number;

  private transport: Transport | undefined;
  private state: MaterializedState = emptyState();
  private cursor = 0n; // highest applied room_seq — the resume point
  private version = 0; // bumped on every state change — the useSyncExternalStore snapshot (S5)
  private clientIdValue: string | undefined;
  private clientSeq = 0n; // last assigned per-client commit sequence

  private closedByUser = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

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
    this.reconnectDelayMs = opts.reconnectDelayMs ?? defaultBackoff;
  }

  /** Dial, Join, and resolve on the first Joined. Drops thereafter auto-reconnect (see class docs). */
  connect(): Promise<void> {
    const joined = new Promise<void>((resolve, reject) => {
      this.joinWaiter = { resolve, reject };
    });
    void this.openConnection();
    return joined;
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
   * transient — the commit stays buffered and is re-sent on recovery). Assigns the next per-client
   * `client_seq`, which together with `client_id` is the server's exactly-once dedup key.
   */
  commit(body: EventBody): Promise<void> {
    this.clientSeq += 1n;
    const seq = this.clientSeq;
    return new Promise<void>((resolve, reject) => {
      this.outstanding.set(seq, { body, resolve, reject });
      this.sendCommit(seq, body);
    });
  }

  /** Fire-and-forget an ephemeral broadcast (cursors, presence): lossy, unordered, never acked. */
  broadcast(body: EphemeralBody): void {
    this.send({ case: 'broadcast', value: { roomId: this.roomId, body } });
  }

  /** App-level keepalive/RTT probe: resolves when the matching Pong returns. */
  ping(id: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.pings.set(id, { resolve, reject });
      this.send({ case: 'ping', value: { id } });
    });
  }

  /** Stop reconnecting, close the connection, and reject anything in flight. Terminal. */
  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.transport?.close();
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
    const t = this.dial();
    this.transport = t;
    try {
      await t.open({
        onMessage: (d) => this.handle(decodeServerMessage(d)),
        onClose: (r) => this.onTransportClose(r),
      });
      this.sendJoin(this.cursor); // 0 on first connect, cursor on resume
    } catch {
      this.scheduleReconnect();
    }
  }

  private onTransportClose(reason?: Error): void {
    // Pings are transient RTT probes — fail them. Outstanding commits SURVIVE for the resend on
    // reconnect (the whole point of recovery). A user close() is handled in close(), not here.
    rejectAll(this.pings.values(), reason ?? new Error('connection closed'));
    this.pings.clear();
    this.scheduleReconnect();
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
        break;
      // roomStatus (FROZEN/LIVE) drives in-place recovery in S4b.
      default:
        break;
    }
  }

  private onJoined(j: Joined): void {
    this.clientIdValue = j.clientId;
    this.reconnectAttempt = 0; // a successful join resets backoff
    if (j.snapshot) {
      // Fresh join, or a deep resume where our cursor fell below the log's floor: adopt the snapshot
      // wholesale as the new base.
      this.state = emptyState();
      for (const [k, v] of Object.entries(j.snapshot.state?.entries ?? {})) this.state.set(k, v);
      this.cursor = j.snapshot.roomSeq;
    }
    // No snapshot ⇒ a resume from our cursor: keep the state we already have; the gateway streams the
    // events after `cursor` to backfill.
    if (j.currentSeq > this.cursor) this.cursor = j.currentSeq;
    this.bump();
    this.joinWaiter?.resolve(); // resolves the first connect(); a no-op on later reconnects
    this.joinWaiter = undefined;
    this.resendOutstanding(); // re-drive un-acked commits through the (re)connected owner
  }

  private onEvent(e: Event): void {
    if (e.roomId !== this.roomId) return;
    // Fan-out is the ack: our own committed event returning resolves the outstanding commit — whether
    // a live delivery or a replay during recovery (so an already-applied commit still acks).
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
    // UNAVAILABLE = no reachable owner / re-homing: transient. Keep it buffered so recovery re-sends
    // it. Every other reason is terminal → reject.
    if (n.reason === NackReason.UNAVAILABLE) return;
    this.outstanding.delete(n.clientSeq);
    o.reject(new NackError(n.reason));
  }

  private ackCommit(clientSeq: bigint): void {
    const o = this.outstanding.get(clientSeq);
    if (o) {
      this.outstanding.delete(clientSeq);
      o.resolve();
    }
  }

  private resendOutstanding(): void {
    for (const [seq, o] of this.outstanding) this.sendCommit(seq, o.body);
  }

  // ===== send helpers =====

  private send(body: ClientBody): void {
    this.transport?.send(encodeClientMessage(create(ClientMessageSchema, { body })));
  }

  private sendJoin(fromSeq: bigint): void {
    this.send({ case: 'join', value: { roomId: this.roomId, fromSeq, sessionNonce: this.nonce } });
  }

  private sendCommit(clientSeq: bigint, body: EventBody): void {
    this.send({ case: 'commit', value: { roomId: this.roomId, clientSeq, body } });
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
