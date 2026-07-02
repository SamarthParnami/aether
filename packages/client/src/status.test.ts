import {
  type ClientMessage,
  ClientMessageSchema,
  type EventBody,
  EventBodySchema,
  NackReason,
  RoomStatus_Status,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { Room, type RoomOptions } from './room.js';
import { memoryTransportPair, type ServerEnd } from './transport.js';

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
const commitSeqOf = (m: ClientMessage) => (m.body.case === 'commit' ? m.body.value.clientSeq : -1n);

function joinedResume(cursor: bigint): ServerMessage {
  return create(ServerMessageSchema, {
    body: { case: 'joined', value: { roomId: 'r', clientId: 'c1', currentSeq: cursor } },
  });
}
function statusMsg(status: RoomStatus_Status): ServerMessage {
  return create(ServerMessageSchema, {
    body: { case: 'roomStatus', value: { roomId: 'r', status } },
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

/** A single, never-dropped transport — so recovery must happen in-place (FROZEN/LIVE), not by reconnect. */
async function joinedRoom(opts: Partial<RoomOptions>): Promise<{ room: Room; server: ServerEnd }> {
  const { transport, server } = memoryTransportPair();
  const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n', ...opts });
  const connected = room.connect();
  await recvClient(server);
  sendServer(server, joinedResume(2n));
  await connected;
  return { room, server };
}

describe('Room in-place recovery (FROZEN/LIVE + retry timer)', () => {
  it('re-drives an un-acked commit over a still-open socket without waiting for LIVE', async () => {
    const statuses: boolean[] = [];
    const { room, server } = await joinedRoom({
      commitRetryMs: 5,
      onStatus: (l) => statuses.push(l),
    });

    const done = room.commit(kvBody('slide', '9'));
    expect(commitSeqOf(await recvClient(server))).toBe(1n);

    // The owner dies: the relay freezes and Nacks the in-flight commit UNAVAILABLE. Crucially LIVE has
    // NOT been signalled (no post-cursor event exists yet) — so a resend gated on LIVE would deadlock.
    sendServer(server, statusMsg(RoomStatus_Status.FROZEN));
    sendServer(
      server,
      create(ServerMessageSchema, {
        body: {
          case: 'nack',
          value: { roomId: 'r', clientSeq: 1n, reason: NackReason.UNAVAILABLE },
        },
      }),
    );

    // The retry timer re-drives the SAME client_seq regardless of FROZEN/LIVE — the deadlock-breaker.
    expect(commitSeqOf(await recvClient(server))).toBe(1n);

    // The new owner applies it and the relay goes LIVE; the resent commit acks exactly-once.
    sendServer(server, statusMsg(RoomStatus_Status.LIVE));
    sendServer(server, ackEvent(3n, 1n, 'slide', '9'));
    await expect(done).resolves.toBeUndefined();
    expect(dec(room.getState().get('slide'))).toBe('9');
    expect(statuses).toContain(false); // FROZEN was surfaced
    expect(statuses).toContain(true); // …and LIVE recovered
    expect(room.isLive()).toBe(true);

    room.close();
  });

  it('resends on the LIVE edge as a fast path (retry disabled)', async () => {
    // commitRetryMs: 0 → the ONLY resend trigger here is the LIVE edge.
    const { room, server } = await joinedRoom({ commitRetryMs: 0 });

    const done = room.commit(kvBody('a', '1'));
    expect(commitSeqOf(await recvClient(server))).toBe(1n);

    sendServer(server, statusMsg(RoomStatus_Status.FROZEN));
    sendServer(server, statusMsg(RoomStatus_Status.LIVE)); // LIVE edge → immediate resend
    expect(commitSeqOf(await recvClient(server))).toBe(1n);

    sendServer(server, ackEvent(3n, 1n, 'a', '1'));
    await expect(done).resolves.toBeUndefined();

    room.close();
  });

  it('stops the retry timer once every commit is acked', async () => {
    const { room, server } = await joinedRoom({ commitRetryMs: 5 });

    const done = room.commit(kvBody('a', '1'));
    expect(commitSeqOf(await recvClient(server))).toBe(1n);
    sendServer(server, ackEvent(3n, 1n, 'a', '1'));
    await expect(done).resolves.toBeUndefined();

    // Drained → no further resends. If the timer kept firing it would push another commit frame; race
    // recv against a quiet window (~6 retry intervals) — the window must win.
    const next = await Promise.race([
      server.recv(),
      new Promise<'quiet'>((r) => setTimeout(() => r('quiet'), 30)),
    ]);
    expect(next).toBe('quiet');

    room.close();
  });
});
