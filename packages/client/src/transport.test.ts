import { ClientMessageSchema, type ServerMessage, ServerMessageSchema } from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { decodeServerMessage, encodeClientMessage } from './codec.js';
import { memoryTransportPair, webSocketTransport } from './transport.js';

describe('memory transport + codec', () => {
  it('carries an encoded ClientMessage from the client to the server end', async () => {
    const { transport, server } = memoryTransportPair();
    await transport.open({ onMessage: () => {}, onClose: () => {} });

    transport.send(
      encodeClientMessage(
        create(ClientMessageSchema, {
          body: { case: 'join', value: { roomId: 'r', sessionNonce: 'n' } },
        }),
      ),
    );

    const frame = await server.recv();
    if (frame === null) throw new Error('expected a frame, got EOF');
    const cm = fromBinary(ClientMessageSchema, frame);
    expect(cm.body.case).toBe('join');
    if (cm.body.case === 'join') expect(cm.body.value.roomId).toBe('r');
  });

  it('delivers a server frame to the client onMessage (decoded via the codec)', async () => {
    const { transport, server } = memoryTransportPair();
    const received: ServerMessage[] = [];
    await transport.open({
      onMessage: (d) => received.push(decodeServerMessage(d)),
      onClose: () => {},
    });

    server.send(
      toBinary(
        ServerMessageSchema,
        create(ServerMessageSchema, { body: { case: 'pong', value: { id: 'hi' } } }),
      ),
    );

    await vi.waitFor(() => expect(received).toHaveLength(1));
    const [msg] = received;
    expect(msg?.body.case).toBe('pong');
  });

  it('server close surfaces to the client onClose and drains recv to null', async () => {
    const { transport, server } = memoryTransportPair();
    let fired = false;
    let reason: Error | undefined;
    await transport.open({
      onMessage: () => {},
      onClose: (r) => {
        fired = true;
        reason = r;
      },
    });

    server.close(new Error('boom'));

    await vi.waitFor(() => expect(fired).toBe(true));
    expect(reason?.message).toBe('boom');
    expect(await server.recv()).toBeNull();
  });

  it('client close stops the server recv without firing onClose back at the client', async () => {
    const { transport, server } = memoryTransportPair();
    let closes = 0;
    await transport.open({
      onMessage: () => {},
      onClose: () => {
        closes++;
      },
    });

    transport.close();

    expect(await server.recv()).toBeNull();
    expect(closes).toBe(0);
  });

  it('an aborted recv() withdraws its waiter instead of eating the next frame', async () => {
    // The "assert nothing was sent" idiom. Previously the abandoned waiter stayed parked and
    // consumed the NEXT frame, handing it to a promise nobody held — a silent frame loss.
    const { transport, server } = memoryTransportPair();
    await transport.open({ onMessage: () => {}, onClose: () => {} });

    expect(await server.recv({ signal: AbortSignal.timeout(5) })).toBeNull(); // nothing sent yet

    transport.send(new Uint8Array([42]));
    const frame = await server.recv();
    expect(frame?.[0]).toBe(42); // still delivered, not swallowed by the abandoned recv
  });

  it('send after close throws', async () => {
    const { transport } = memoryTransportPair();
    await transport.open({ onMessage: () => {}, onClose: () => {} });
    transport.close();
    expect(() => transport.send(new Uint8Array([1]))).toThrow(/after close/);
  });

  it('delivers a frame sent immediately before close (no loss on close)', async () => {
    const { transport, server } = memoryTransportPair();
    const got: Uint8Array[] = [];
    let closed = false;
    await transport.open({
      onMessage: (d) => got.push(d),
      onClose: () => {
        closed = true;
      },
    });

    server.send(new Uint8Array([7]));
    server.close(); // the pre-close frame must still arrive, and before onClose

    await vi.waitFor(() => expect(closed).toBe(true));
    expect(got).toHaveLength(1);
    expect(got[0]?.[0]).toBe(7);
  });
});

/** A minimal stand-in for the browser/Node WebSocket, driven by the test via its on* handlers. */
class FakeWebSocket {
  static last: FakeWebSocket | undefined;
  binaryType = 'blob';
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: Uint8Array[] = [];
  closed = false;
  constructor(readonly url: string) {
    FakeWebSocket.last = this;
  }
  send(data: Uint8Array): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
    this.onclose?.();
  }
}
const fakeCtor = FakeWebSocket as unknown as typeof WebSocket;

describe('webSocketTransport', () => {
  it('wires open → binary-only message → send → close', async () => {
    const t = webSocketTransport('ws://x', { WebSocketCtor: fakeCtor });
    const got: Uint8Array[] = [];
    let closes = 0;
    const opening = t.open({ onMessage: (d) => got.push(d), onClose: () => closes++ });
    const ws = FakeWebSocket.last;
    if (!ws) throw new Error('WebSocket ctor was not called');

    ws.onopen?.();
    await opening;
    expect(ws.binaryType).toBe('arraybuffer');

    ws.onmessage?.({ data: new Uint8Array([1, 2, 3]).buffer }); // binary → delivered
    ws.onmessage?.({ data: 'text' }); // non-binary → dropped
    expect(got).toHaveLength(1);
    expect(Array.from(got[0] ?? [])).toEqual([1, 2, 3]);

    t.send(new Uint8Array([9]));
    expect(ws.sent).toHaveLength(1);

    // SDK-initiated close is SILENT, exactly as memoryTransportPair behaves (see the `closes === 0`
    // assertion above). onClose means "the peer dropped us", never "we hung up".
    t.close();
    expect(closes).toBe(0);
    expect(ws.closed).toBe(true);
    expect(() => t.send(new Uint8Array([9]))).toThrow(/after close/);
  });

  it('fires onClose when the PEER drops a live connection', async () => {
    const t = webSocketTransport('ws://x', { WebSocketCtor: fakeCtor });
    let closes = 0;
    const opening = t.open({ onMessage: () => {}, onClose: () => closes++ });
    const ws = FakeWebSocket.last;
    if (!ws) throw new Error('WebSocket ctor was not called');

    ws.onopen?.();
    await opening;
    ws.onclose?.(); // the peer/network dropped us — this IS a drop
    expect(closes).toBe(1);
  });

  it('open() after close() rejects without constructing a socket', async () => {
    const t = webSocketTransport('ws://x', { WebSocketCtor: fakeCtor });
    FakeWebSocket.last = undefined;
    t.close();

    await expect(t.open({ onMessage: () => {}, onClose: () => {} })).rejects.toThrow(
      /closed before open/,
    );
    // Previously this constructed a live socket nothing would ever close, while send() threw forever.
    expect(FakeWebSocket.last).toBeUndefined();
  });

  it('a second open() rejects without leaking the first socket', async () => {
    const t = webSocketTransport('ws://x', { WebSocketCtor: fakeCtor });
    const opening = t.open({ onMessage: () => {}, onClose: () => {} });
    const first = FakeWebSocket.last;
    if (!first) throw new Error('WebSocket ctor was not called');
    first.onopen?.();
    await opening;

    await expect(t.open({ onMessage: () => {}, onClose: () => {} })).rejects.toThrow(
      /already opened/,
    );
    expect(FakeWebSocket.last).toBe(first);
  });

  it('rejects open() exactly once on a pre-open failure, without firing onClose', async () => {
    const t = webSocketTransport('ws://x', { WebSocketCtor: fakeCtor });
    let closes = 0;
    const opening = t.open({ onMessage: () => {}, onClose: () => closes++ });
    const ws = FakeWebSocket.last;
    if (!ws) throw new Error('WebSocket ctor was not called');

    ws.onerror?.(); // pre-open error rejects open()…
    await expect(opening).rejects.toThrow(/websocket error/);
    ws.onclose?.(); // …and the trailing close must NOT also fire onClose
    expect(closes).toBe(0);
  });
});
