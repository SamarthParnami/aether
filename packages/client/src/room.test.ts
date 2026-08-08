import {
  type ClientMessage,
  ClientMessageSchema,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { Room, type RoomOptions } from './room.js';
import { memoryTransportPair, type ServerEnd } from './transport.js';

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | undefined) =>
  b === undefined ? undefined : new TextDecoder().decode(b);

function sendServer(server: ServerEnd, msg: ServerMessage): void {
  server.send(toBinary(ServerMessageSchema, msg));
}

async function recvClient(server: ServerEnd): Promise<ClientMessage> {
  const frame = await server.recv();
  if (frame === null) throw new Error('expected a client frame, got EOF');
  return fromBinary(ClientMessageSchema, frame);
}

function joinedMsg(): ServerMessage {
  return create(ServerMessageSchema, {
    body: {
      case: 'joined',
      value: {
        roomId: 'r',
        clientId: 'c1',
        currentSeq: 2n,
        snapshot: { roomSeq: 2n, state: { entries: { slide: enc('7') } } },
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

/** Dial a Room and complete the Join handshake at cursor 2 with state {slide: "7"}. */
async function joinedRoom(opts?: Partial<RoomOptions>): Promise<{ room: Room; server: ServerEnd }> {
  const { transport, server } = memoryTransportPair();
  const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n', ...opts });
  const connected = room.connect();
  await recvClient(server); // consume the Join
  sendServer(server, joinedMsg());
  await connected;
  return { room, server };
}

describe('Room', () => {
  it('joins fresh (from_seq 0, session nonce) and materializes the snapshot', async () => {
    const { transport, server } = memoryTransportPair();
    const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n' });
    const connected = room.connect();

    const join = await recvClient(server);
    expect(join.body.case).toBe('join');
    if (join.body.case === 'join') {
      expect(join.body.value.roomId).toBe('r');
      expect(join.body.value.fromSeq).toBe(0n);
      expect(join.body.value.sessionNonce).toBe('n');
    }

    sendServer(server, joinedMsg());
    await connected;

    expect(room.clientId()).toBe('c1');
    expect(room.currentSeq()).toBe(2n);
    expect(dec(room.getState().get('slide'))).toBe('7');
  });

  it('generates a session nonce when none is supplied', async () => {
    const { transport, server } = memoryTransportPair();
    const room = new Room({ dial: () => transport, roomId: 'r' });
    void room.connect();
    const join = await recvClient(server);
    if (join.body.case !== 'join') throw new Error('expected join');
    expect(join.body.value.sessionNonce).not.toBe('');
  });

  it('folds a live Event into state and notifies subscribers once', async () => {
    const { room, server } = await joinedRoom();
    let notifications = 0;
    room.subscribe(() => notifications++);
    const versionBefore = room.getVersion();

    sendServer(server, eventMsg(3n, 'slide', '9'));

    await vi.waitFor(() => expect(room.currentSeq()).toBe(3n));
    expect(dec(room.getState().get('slide'))).toBe('9');
    expect(notifications).toBe(1);
    expect(room.getVersion()).toBe(versionBefore + 1);
  });

  it('ignores an already-applied Event (room_seq ≤ cursor) — idempotent replay', async () => {
    const { room, server } = await joinedRoom();
    let notifications = 0;
    room.subscribe(() => notifications++);

    sendServer(server, eventMsg(2n, 'slide', 'STALE')); // cursor is already 2
    sendServer(server, eventMsg(3n, 'slide', '9')); // a real one, to prove the stream still flows

    await vi.waitFor(() => expect(room.currentSeq()).toBe(3n));
    expect(dec(room.getState().get('slide'))).toBe('9'); // never 'STALE'
    expect(notifications).toBe(1); // only the seq-3 apply notified
  });

  it('surfaces a room_seq gap instead of applying out of order', async () => {
    const gaps: string[] = [];
    const { room, server } = await joinedRoom({
      onError: (code) => gaps.push(code),
    });

    sendServer(server, eventMsg(5n, 'slide', 'ahead')); // cursor 2, expected 3 — a gap

    await vi.waitFor(() => expect(gaps).toContain('seq_gap'));
    expect(room.currentSeq()).toBe(2n); // unchanged
    expect(dec(room.getState().get('slide'))).toBe('7'); // unchanged
  });

  it('resolves ping() when the matching Pong returns', async () => {
    const { room, server } = await joinedRoom();
    const ponged = room.ping('probe');

    const msg = await recvClient(server);
    expect(msg.body.case).toBe('ping');
    if (msg.body.case === 'ping') expect(msg.body.value.id).toBe('probe');

    sendServer(
      server,
      create(ServerMessageSchema, { body: { case: 'pong', value: { id: 'probe' } } }),
    );
    await expect(ponged).resolves.toBeUndefined();
  });

  it('rejects an in-flight ping when the connection drops (RTT probes are transient)', async () => {
    const { room, server } = await joinedRoom();
    const ponged = room.ping('probe');
    await recvClient(server); // consume the ping
    server.close(new Error('dropped'));
    await expect(ponged).rejects.toThrow(/dropped/);
    room.close(); // stop the reconnect loop this drop kicked off (see recovery.test.ts)
  });
});
