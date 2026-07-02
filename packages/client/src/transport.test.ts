import { ClientMessageSchema, type ServerMessage, ServerMessageSchema } from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { decodeServerMessage, encodeClientMessage } from './codec.js';
import { memoryTransportPair } from './transport.js';

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

  it('send after close throws', async () => {
    const { transport } = memoryTransportPair();
    await transport.open({ onMessage: () => {}, onClose: () => {} });
    transport.close();
    expect(() => transport.send(new Uint8Array([1]))).toThrow(/after close/);
  });
});
