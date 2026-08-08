/**
 * Transport — the byte-frame seam under the Aether SDK.
 *
 * The SDK speaks the WebSocket envelope (length-implicit binary Protobuf frames) over an abstract
 * duplex byte-frame channel, NOT a concrete `WebSocket`. This is the TS counterpart of the Go
 * gateway's `frameConn` seam: production uses {@link webSocketTransport}, while every behaviour test
 * runs over {@link memoryTransportPair} with a scripted server — no real socket, deterministic,
 * fast. A Transport is single-use (like a WebSocket, it can't reopen); reconnection creates a fresh
 * one via a {@link Dialer}, mirroring the gateway relay re-dialing its owner.
 */

/** Callbacks the SDK registers when it opens a transport. */
export interface TransportHandlers {
  /** A binary frame arrived from the peer. */
  onMessage(data: Uint8Array): void;
  /** The connection dropped (peer close, network error, or transport failure). Fires at most once. */
  onClose(reason?: Error): void;
}

/** A single-use duplex byte-frame channel to the gateway. */
export interface Transport {
  /** Open the channel and register handlers. Resolves when ready to {@link send}; rejects on connect failure. */
  open(handlers: TransportHandlers): Promise<void>;
  /** Send one binary frame. Throws if the transport is closed. */
  send(data: Uint8Array): void;
  /** Close locally. Idempotent; does NOT fire the local `onClose` (the SDK initiated it). */
  close(): void;
}

/** Mints a fresh Transport per (re)connect attempt — the SDK never reuses a closed one. */
export type Dialer = () => Transport;

// ===== Production transport: browser / Node-22 global WebSocket =====

export interface WebSocketTransportOptions {
  /** Override the WebSocket constructor (defaults to `globalThis.WebSocket`) — for injection/tests. */
  WebSocketCtor?: typeof WebSocket;
}

/**
 * A {@link Transport} over a real WebSocket. Uses the global `WebSocket` (browsers and Node ≥22),
 * with `binaryType='arraybuffer'` so frames arrive as bytes. Non-binary frames are dropped (the SDK
 * envelope is binary-only, matching the gateway's `errNonBinaryFrame` handling).
 *
 * A single connect attempt yields exactly ONE outcome: `open()` resolves on `onopen`, or rejects on a
 * pre-open `error`/`close` — never both, and a pre-open failure does NOT also fire `onClose` (which is
 * reserved for a drop *after* the connection was live). Post-close `send()` throws, matching the
 * {@link Transport.send} contract and the in-memory transport.
 *
 * An SDK-initiated `close()` is likewise silent: the socket's own `close` event is suppressed, so
 * `onClose` means "the peer or the network dropped us" and nothing else — the same semantics
 * {@link memoryTransportPair} has always had. Without that suppression `room.close()` bounced back
 * through `onTransportClose`, delivering an `onStatus(false)` to an app that had just torn the room
 * down, and every behaviour test (which runs on the memory pair) validated different semantics from
 * the transport that actually ships.
 */
export function webSocketTransport(url: string, opts: WebSocketTransportOptions = {}): Transport {
  const Ctor = opts.WebSocketCtor ?? globalThis.WebSocket;
  let ws: WebSocket | undefined;
  let opened = false;
  let closed = false;
  let closedByUs = false;
  return {
    open: (h) =>
      new Promise<void>((resolve, reject) => {
        // Single-use, like the WebSocket it wraps: a transport that was closed (or already opened)
        // must not quietly construct a second socket that nothing will ever close.
        if (closed) {
          reject(new Error('transport: closed before open'));
          return;
        }
        if (ws) {
          reject(new Error('transport: already opened'));
          return;
        }
        const sock = new Ctor(url);
        sock.binaryType = 'arraybuffer';
        ws = sock;
        sock.onopen = () => {
          opened = true;
          resolve();
        };
        sock.onmessage = (ev: MessageEvent) => {
          if (ev.data instanceof ArrayBuffer) h.onMessage(new Uint8Array(ev.data));
        };
        sock.onclose = () => {
          closed = true;
          if (closedByUs) return; // we initiated it — don't report our own close back to the SDK
          // After a live connection → a drop (onClose); before open → settle open() as a failure.
          if (opened) h.onClose();
          else reject(new Error('websocket closed before open'));
        };
        // Pre-open error rejects open() once; post-open, onclose is the drop path (this reject is a no-op).
        sock.onerror = () => {
          if (!opened) reject(new Error('websocket error'));
        };
      }),
    send: (data) => {
      if (closed || !ws) throw new Error('transport: send before open or after close');
      ws.send(data);
    },
    close: () => {
      closed = true;
      closedByUs = true;
      ws?.close();
    },
  };
}

// ===== Test transport: in-memory duplex pair with a pull-based server end =====

/** The scripted-server control end of a {@link memoryTransportPair}. */
export interface ServerEnd {
  /**
   * Await the next frame the client sent. Resolves `null` once the client end closed, or if
   * `signal` aborts first.
   *
   * Pass a `signal` for "assert nothing was sent" checks rather than racing this promise against a
   * timer: an abandoned `recv()` used to leave an un-cancellable waiter parked in the queue, which
   * then consumed the NEXT frame and handed it to a promise nobody was holding — silently losing a
   * frame a real socket would have delivered. An aborted `recv()` withdraws its waiter instead.
   */
  recv(opts?: { signal?: AbortSignal }): Promise<Uint8Array | null>;
  /** Push a frame to the client (delivered on a later microtask, like a real network turn). */
  send(data: Uint8Array): void;
  /** Drop the connection; the client's `onClose` fires with `reason`. Idempotent. */
  close(reason?: Error): void;
}

/**
 * A minimal async FIFO of frames: the client pushes, the server end pulls. A pending `pull` resolves
 * the instant a frame arrives; after {@link close} every pending and future pull resolves `null`.
 */
class FrameQueue {
  private items: Uint8Array[] = [];
  private waiters: ((v: Uint8Array | null) => void)[] = [];
  private closed = false;

  push(data: Uint8Array): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w(data);
    else this.items.push(data);
  }

  pull(signal?: AbortSignal): Promise<Uint8Array | null> {
    const item = this.items.shift();
    if (item !== undefined) return Promise.resolve(item);
    if (this.closed) return Promise.resolve(null);
    if (signal?.aborted) return Promise.resolve(null);
    return new Promise((resolve) => {
      const waiter = (v: Uint8Array | null): void => {
        signal?.removeEventListener('abort', onAbort);
        resolve(v);
      };
      // Withdraw the waiter on abort, so the next frame is BUFFERED for a later pull instead of
      // being handed to a promise nobody is holding.
      const onAbort = (): void => {
        const i = this.waiters.indexOf(waiter);
        if (i !== -1) this.waiters.splice(i, 1);
        resolve(null);
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const w of this.waiters.splice(0)) w(null);
  }
}

/**
 * An in-memory {@link Transport} paired with a {@link ServerEnd} for scripting a gateway in tests.
 * Client→server frames flow through a {@link FrameQueue} (pull with `server.recv()`); server→client
 * frames are delivered to `onMessage` on a microtask, so a `send` from within an `onMessage` handler
 * can't re-enter. This is the SDK's equivalent of the Go `framePipe` used by the gateway DST suite.
 */
export function memoryTransportPair(): { transport: Transport; server: ServerEnd } {
  const clientToServer = new FrameQueue();
  let handlers: TransportHandlers | undefined;
  let closed = false;

  const closeBoth = (reason: Error | undefined, notifyClient: boolean) => {
    if (closed) return;
    closed = true;
    clientToServer.close();
    // Defer onClose onto a microtask so any server→client frames sent just before close() (also
    // queued on microtasks) are delivered FIRST — a real socket flushes buffered frames before the
    // close event, and `server.send(frame); server.close()` must not silently lose `frame`.
    if (notifyClient) queueMicrotask(() => handlers?.onClose(reason));
  };

  const transport: Transport = {
    open: (h) => {
      handlers = h;
      return Promise.resolve();
    },
    send: (data) => {
      if (closed) throw new Error('transport: send after close');
      clientToServer.push(data);
    },
    close: () => closeBoth(undefined, false), // SDK-initiated: don't fire onClose back at the SDK
  };

  const server: ServerEnd = {
    recv: (o) => clientToServer.pull(o?.signal),
    // A frame accepted before close() is delivered even if close() runs before its microtask (see
    // closeBoth) — symmetric with client→server frames, which recv() still drains after close.
    send: (data) => {
      if (closed) return;
      queueMicrotask(() => handlers?.onMessage(data));
    },
    close: (reason) => closeBoth(reason ?? new Error('server closed'), true),
  };

  return { transport, server };
}
