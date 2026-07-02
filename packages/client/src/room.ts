/**
 * Room — a client's live view of one Aether room over a single connection.
 *
 * `connect()` dials a {@link Transport}, sends a Join, and resolves once the gateway replies Joined
 * (client_id + snapshot + cursor). From there the Room folds each durable Event into a materialized
 * key/value state — the client-side mirror of the Go `roomcore` reducer (shared golden vectors keep
 * the two in lockstep) — and notifies subscribers. `getState()`/`subscribe()` are the useState-like
 * surface the React hook (S5) builds on.
 *
 * This PR (S2) covers Join → snapshot → live Events + request/response Ping. Committing (S3) and
 * reconnect/recovery (S4) layer on; until S4 a dropped connection just rejects in-flight promises
 * (no auto-reconnect yet), and a room_seq gap is surfaced, never applied out of order.
 */
import {
  ClientMessageSchema,
  emptyState,
  type Event,
  fold,
  type Joined,
  type MaterializedState,
  type ServerMessage,
} from '@aether/protocol';
import { create } from '@bufbuild/protobuf';

import { decodeServerMessage, encodeClientMessage } from './codec.js';
import type { Dialer, Transport } from './transport.js';

export interface RoomOptions {
  /** Mints the transport for this connection (S4 re-dials through it on reconnect). */
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
}

/** Notified after each applied state change; read the latest via {@link Room.getState}. */
export type StateListener = () => void;

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
}

export class Room {
  private readonly dial: Dialer;
  private readonly roomId: string;
  private readonly nonce: string;
  private readonly onError: ((code: string, message: string) => void) | undefined;

  private transport: Transport | undefined;
  private state: MaterializedState = emptyState();
  private cursor = 0n; // highest applied room_seq
  private version = 0; // bumped on every state change — the useSyncExternalStore snapshot (S5)
  private clientIdValue: string | undefined;

  private joinWaiter: Waiter | undefined;
  private readonly pings = new Map<string, Waiter>();
  private readonly listeners = new Set<StateListener>();

  constructor(opts: RoomOptions) {
    this.dial = opts.dial;
    this.roomId = opts.roomId;
    this.nonce = opts.sessionNonce ?? crypto.randomUUID();
    this.onError = opts.onError;
  }

  /** Dial, Join from a fresh cursor, and resolve when the gateway replies Joined. */
  async connect(): Promise<void> {
    const t = this.dial();
    this.transport = t;
    await t.open({
      onMessage: (d) => this.handle(decodeServerMessage(d)),
      onClose: (r) => this.handleClose(r),
    });
    const joined = new Promise<void>((resolve, reject) => {
      this.joinWaiter = { resolve, reject };
    });
    this.sendJoin(0n);
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

  /** App-level keepalive/RTT probe: resolves when the matching Pong returns. */
  ping(id: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.pings.set(id, { resolve, reject });
      this.transport?.send(
        encodeClientMessage(create(ClientMessageSchema, { body: { case: 'ping', value: { id } } })),
      );
    });
  }

  /** Close the connection and reject anything in flight. */
  close(): void {
    this.transport?.close();
    this.rejectPending(new Error('room closed'));
  }

  // ===== internals =====

  private sendJoin(fromSeq: bigint): void {
    this.transport?.send(
      encodeClientMessage(
        create(ClientMessageSchema, {
          body: { case: 'join', value: { roomId: this.roomId, fromSeq, sessionNonce: this.nonce } },
        }),
      ),
    );
  }

  private handle(m: ServerMessage): void {
    switch (m.body.case) {
      case 'joined':
        this.onJoined(m.body.value);
        break;
      case 'event':
        this.onEvent(m.body.value);
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
      // nack / ephemeral / roomStatus arrive with the commit (S3) and recovery (S4) layers.
      default:
        break;
    }
  }

  private onJoined(j: Joined): void {
    this.clientIdValue = j.clientId;
    this.state = emptyState();
    this.cursor = 0n;
    if (j.snapshot) {
      for (const [k, v] of Object.entries(j.snapshot.state?.entries ?? {})) this.state.set(k, v);
      this.cursor = j.snapshot.roomSeq;
    }
    if (j.currentSeq > this.cursor) this.cursor = j.currentSeq;
    this.bump();
    this.joinWaiter?.resolve();
    this.joinWaiter = undefined;
  }

  private onEvent(e: Event): void {
    if (e.roomId !== this.roomId) return;
    const expected = this.cursor + 1n;
    if (e.roomSeq < expected) return; // already applied — an idempotent replay
    if (e.roomSeq > expected) {
      // A gap: the log skipped ahead. Applying out of order would diverge from the Go reducer, so we
      // surface it and wait — the cursor-resume path (S4) is what actually backfills the gap.
      this.onError?.('seq_gap', `expected room_seq ${expected}, got ${e.roomSeq}`);
      return;
    }
    if (e.body) fold(this.state, e.body);
    this.cursor = e.roomSeq;
    this.bump();
  }

  private handleClose(reason?: Error): void {
    // S4 turns this into a reconnect; for now a drop just fails anything awaiting.
    this.rejectPending(reason ?? new Error('connection closed'));
  }

  private rejectPending(err: Error): void {
    this.joinWaiter?.reject(err);
    this.joinWaiter = undefined;
    for (const w of this.pings.values()) w.reject(err);
    this.pings.clear();
  }

  private bump(): void {
    this.version++;
    for (const fn of this.listeners) fn();
  }
}
