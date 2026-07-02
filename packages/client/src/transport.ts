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
 * envelope is binary-only, matching the gateway's `errNonBinaryFrame` handling). `onerror` is not
 * surfaced separately: the spec guarantees a following `onclose`, so close is the single failure path.
 */
export function webSocketTransport(url: string, opts: WebSocketTransportOptions = {}): Transport {
  const Ctor = opts.WebSocketCtor ?? globalThis.WebSocket;
  let ws: WebSocket | undefined;
  return {
    open: (h) =>
      new Promise<void>((resolve, reject) => {
        const sock = new Ctor(url);
        sock.binaryType = 'arraybuffer';
        ws = sock;
        sock.onopen = () => resolve();
        sock.onmessage = (ev: MessageEvent) => {
          if (ev.data instanceof ArrayBuffer) h.onMessage(new Uint8Array(ev.data));
        };
        sock.onclose = () => h.onClose();
        // Pre-open errors reject open(); post-open, onclose is the failure path (reject is then a no-op).
        sock.onerror = () => reject(new Error('websocket error'));
      }),
    send: (data) => {
      if (!ws) throw new Error('transport: send before open');
      ws.send(data);
    },
    close: () => ws?.close(),
  };
}

// ===== Test transport: in-memory duplex pair with a pull-based server end =====

/** The scripted-server control end of a {@link memoryTransportPair}. */
export interface ServerEnd {
  /** Await the next frame the client sent, or `null` once the client end closed. */
  recv(): Promise<Uint8Array | null>;
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

  pull(): Promise<Uint8Array | null> {
    const item = this.items.shift();
    if (item !== undefined) return Promise.resolve(item);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => this.waiters.push(resolve));
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
    if (notifyClient) handlers?.onClose(reason);
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
    recv: () => clientToServer.pull(),
    send: (data) => {
      if (closed) return;
      queueMicrotask(() => {
        if (!closed) handlers?.onMessage(data);
      });
    },
    close: (reason) => closeBoth(reason ?? new Error('server closed'), true),
  };

  return { transport, server };
}
