import {
  type ClientMessage,
  ClientMessageSchema,
  type EventBody,
  EventBodySchema,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { Room } from './room.js';
import { type Dialer, memoryTransportPair, type ServerEnd } from './transport.js';

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | undefined) =>
  b === undefined ? undefined : new TextDecoder().decode(b);
const kvBody = (key: string, value: string): EventBody =>
  create(EventBodySchema, { kind: { case: 'kvSet', value: { key, value: enc(value) } } });

function sendServer(server: ServerEnd, msg: ServerMessage): void {
  server.send(toBinary(ServerMessageSchema, msg));
}
async function recvClient(server: ServerEnd): Promise<ClientMessage> {
  const frame = await server.recv();
  if (frame === null) throw new Error('expected a client frame, got EOF');
  return fromBinary(ClientMessageSchema, frame);
}

/** A dialer that mints a fresh in-memory pair per (re)connect and records each server end. */
function harness(): { dial: Dialer; servers: ServerEnd[] } {
  const servers: ServerEnd[] = [];
  const dial: Dialer = () => {
    const { transport, server } = memoryTransportPair();
    servers.push(server);
    return transport;
  };
  return { dial, servers };
}
function serverAt(servers: ServerEnd[], i: number): ServerEnd {
  const s = servers[i];
  if (s === undefined) throw new Error(`no server[${i}] yet`);
  return s;
}
const waitForServer = (servers: ServerEnd[], n: number) =>
  vi.waitFor(() => expect(servers.length).toBeGreaterThanOrEqual(n));

/** Joined carrying a snapshot (fresh join or deep resume). */
function joinedSnapshot(cursor: bigint, entries: Record<string, string>): ServerMessage {
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
/** Joined with no snapshot — a resume from the client's cursor; the stream backfills from there. */
function joinedResume(cursor: bigint): ServerMessage {
  return create(ServerMessageSchema, {
    body: { case: 'joined', value: { roomId: 'r', clientId: 'c1', currentSeq: cursor } },
  });
}
function ackEvent(roomSeq: bigint, originSeq: bigint, key: string, value: string): ServerMessage {
  return create(ServerMessageSchema, {
    body: {
      case: 'event',
      value: {
        roomId: 'r',
        roomSeq,
        originClientId: 'c1',
        originClientSeq: originSeq,
        body: kvBody(key, value),
      },
    },
  });
}

const fromSeqOf = (m: ClientMessage) => (m.body.case === 'join' ? m.body.value.fromSeq : -1n);
const commitSeqOf = (m: ClientMessage) => (m.body.case === 'commit' ? m.body.value.clientSeq : -1n);

describe('Room recovery', () => {
  it('reconnects, resumes from the cursor, and re-sends the outstanding commit (exactly-once)', async () => {
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    expect(fromSeqOf(await recvClient(serverAt(servers, 0)))).toBe(0n); // fresh join
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, { slide: '7' }));
    await connected;

    // Commit, then DROP before the ack arrives — the commit must survive.
    const done = room.commit(kvBody('slide', '9'));
    expect(commitSeqOf(await recvClient(serverAt(servers, 0)))).toBe(1n);
    serverAt(servers, 0).close(new Error('drop'));

    // Reconnect re-Joins from the cursor, and the buffered commit is re-sent on the new owner.
    await waitForServer(servers, 2);
    expect(fromSeqOf(await recvClient(serverAt(servers, 1)))).toBe(2n); // resumed, not from zero
    sendServer(serverAt(servers, 1), joinedResume(2n));
    expect(commitSeqOf(await recvClient(serverAt(servers, 1)))).toBe(1n); // same client_seq re-sent

    // The new owner acks it (dedup makes a re-apply exactly-once); state advances once.
    sendServer(serverAt(servers, 1), ackEvent(3n, 1n, 'slide', '9'));
    await expect(done).resolves.toBeUndefined();
    expect(room.currentSeq()).toBe(3n);
    expect(dec(room.getState().get('slide'))).toBe('9');

    room.close();
  });

  it('adopts a fresh snapshot on a deep resume', async () => {
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, { slide: '7' }));
    await connected;

    serverAt(servers, 0).close(new Error('drop'));
    await waitForServer(servers, 2);
    expect(fromSeqOf(await recvClient(serverAt(servers, 1)))).toBe(2n);

    // Cursor fell below the log floor → the gateway hands back a newer snapshot; the Room adopts it.
    sendServer(serverAt(servers, 1), joinedSnapshot(5n, { slide: 'X', title: 'hi' }));
    await vi.waitFor(() => expect(room.currentSeq()).toBe(5n));
    expect(dec(room.getState().get('slide'))).toBe('X');
    expect(dec(room.getState().get('title'))).toBe('hi');

    room.close();
  });

  it('close() stops reconnection and rejects outstanding commits', async () => {
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedResume(2n));
    await connected;

    const done = room.commit(kvBody('a', '1'));
    await recvClient(serverAt(servers, 0));
    room.close();

    await expect(done).rejects.toThrow(/room closed/);
    // No reconnect after a user close, even across a tick.
    await new Promise((r) => setTimeout(r, 0));
    expect(servers).toHaveLength(1);
  });
});
