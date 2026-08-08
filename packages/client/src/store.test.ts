import { ClientMessageSchema, type ServerMessage, ServerMessageSchema } from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { Room } from './room.js';
import { createRoomStore } from './store.js';
import { memoryTransportPair, type ServerEnd } from './transport.js';

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | undefined) =>
  b === undefined ? undefined : new TextDecoder().decode(b);

function sendServer(server: ServerEnd, msg: ServerMessage): void {
  server.send(toBinary(ServerMessageSchema, msg));
}
async function recvJoin(server: ServerEnd): Promise<void> {
  const frame = await server.recv();
  if (frame === null) throw new Error('expected the Join');
  fromBinary(ClientMessageSchema, frame);
}
function joined(cursor: bigint, entries: Record<string, string>): ServerMessage {
  const e: Record<string, Uint8Array> = {};
  for (const [k, v] of Object.entries(entries)) e[k] = enc(v);
  return create(ServerMessageSchema, {
    body: {
      case: 'joined',
      value: {
        roomId: 'r',
        clientId: 'c1',
        currentSeq: cursor,
        snapshot: { roomSeq: cursor, state: { entries: e } },
      },
    },
  });
}
function eventMsg(roomSeq: bigint, key: string, value: string): ServerMessage {
  return create(ServerMessageSchema, {
    body: {
      case: 'event',
      value: {
        roomId: 'r',
        roomSeq,
        originClientId: 'other',
        originClientSeq: 0n,
        body: { kind: { case: 'kvSet', value: { key, value: enc(value) } } },
      },
    },
  });
}

describe('createRoomStore (useSyncExternalStore adapter)', () => {
  it('bumps the snapshot and notifies on state change, and is stable otherwise', async () => {
    const { transport, server } = memoryTransportPair();
    const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n' });
    const store = createRoomStore(room);

    const connected = room.connect();
    await recvJoin(server);
    sendServer(server, joined(2n, { slide: '7' }));
    await connected;

    let renders = 0;
    const unsubscribe = store.subscribe(() => renders++);
    const snapAfterJoin = store.getSnapshot();
    expect(dec(store.getState().get('slide'))).toBe('7');

    // A change bumps the snapshot and fires the subscriber exactly once.
    sendServer(server, eventMsg(3n, 'slide', '9'));
    await vi.waitFor(() => expect(renders).toBe(1));
    expect(store.getSnapshot()).not.toBe(snapAfterJoin);
    expect(store.getSnapshot()).toBe(store.getSnapshot()); // stable while nothing changes
    expect(dec(store.getState().get('slide'))).toBe('9');

    // After unsubscribe, further changes don't notify.
    unsubscribe();
    sendServer(server, eventMsg(4n, 'slide', 'X'));
    await vi.waitFor(() => expect(room.currentSeq()).toBe(4n));
    expect(renders).toBe(1);

    room.close();
  });
});
