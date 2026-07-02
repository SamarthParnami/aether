import {
  type ClientMessage,
  ClientMessageSchema,
  type EphemeralBody,
  EphemeralBodySchema,
  type EventBody,
  EventBodySchema,
  NackReason,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { NackError, Room, type RoomOptions } from './room.js';
import { memoryTransportPair, type ServerEnd } from './transport.js';

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | undefined) =>
  b === undefined ? undefined : new TextDecoder().decode(b);
const kvBody = (key: string, value: string): EventBody =>
  create(EventBodySchema, { kind: { case: 'kvSet', value: { key, value: enc(value) } } });
const kvEph = (key: string, value: string): EphemeralBody =>
  create(EphemeralBodySchema, { kind: { case: 'kvSet', value: { key, value: enc(value) } } });

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
      value: { roomId: 'r', clientId: 'c1', currentSeq: 2n, snapshot: { roomSeq: 2n } },
    },
  });
}
/** An Event echoed back to us (origin = our client_id) — the fan-out ack for client_seq. */
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
function nackMsg(clientSeq: bigint, reason: NackReason): ServerMessage {
  return create(ServerMessageSchema, {
    body: { case: 'nack', value: { roomId: 'r', clientSeq, reason } },
  });
}

async function joinedRoom(opts?: Partial<RoomOptions>): Promise<{ room: Room; server: ServerEnd }> {
  const { transport, server } = memoryTransportPair();
  const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n', ...opts });
  const connected = room.connect();
  await recvClient(server);
  sendServer(server, joinedMsg());
  await connected;
  return { room, server };
}

describe('Room commit path', () => {
  it('resolves a commit when its Event fans back (fan-out is the ack) and folds it', async () => {
    const { room, server } = await joinedRoom();
    const done = room.commit(kvBody('slide', '9'));

    const sent = await recvClient(server);
    expect(sent.body.case).toBe('commit');
    if (sent.body.case === 'commit') {
      expect(sent.body.value.roomId).toBe('r');
      expect(sent.body.value.clientSeq).toBe(1n);
    }

    sendServer(server, ackEvent(3n, 1n, 'slide', '9'));
    await expect(done).resolves.toBeUndefined();
    expect(dec(room.getState().get('slide'))).toBe('9');
    expect(room.currentSeq()).toBe(3n);
  });

  it('assigns a monotonic client_seq per commit', async () => {
    const { room, server } = await joinedRoom();
    void room.commit(kvBody('a', '1')).catch(() => {});
    void room.commit(kvBody('b', '2')).catch(() => {});

    const seqOf = (m: ClientMessage) => (m.body.case === 'commit' ? m.body.value.clientSeq : -1n);
    expect(seqOf(await recvClient(server))).toBe(1n);
    expect(seqOf(await recvClient(server))).toBe(2n);
    room.close(); // drain the (un-acked) commits' retry timer
  });

  it('rejects a commit the server Nacks with a NackError carrying the reason', async () => {
    const { room, server } = await joinedRoom();
    const done = room.commit(kvBody('a', '1'));
    await recvClient(server);

    sendServer(server, nackMsg(1n, NackReason.PERMISSION_DENIED));
    await expect(done).rejects.toBeInstanceOf(NackError);
    await expect(done).rejects.toMatchObject({ reason: NackReason.PERMISSION_DENIED });
  });

  it('keeps a commit buffered on a transient UNAVAILABLE Nack, still ackable', async () => {
    const { room, server } = await joinedRoom();
    const done = room.commit(kvBody('a', '1'));
    await recvClient(server);

    // UNAVAILABLE = re-homing: must NOT reject. Prove it stays pending across a full tick…
    sendServer(server, nackMsg(1n, NackReason.UNAVAILABLE));
    let settled = false;
    void done.then(
      () => (settled = true),
      () => (settled = true),
    );
    await new Promise((r) => setTimeout(r, 0));
    expect(settled).toBe(false);

    // …and that the buffered commit still acks when its Event finally arrives.
    sendServer(server, ackEvent(3n, 1n, 'a', '1'));
    await expect(done).resolves.toBeUndefined();
  });

  it('sends a broadcast on the ephemeral tier (fire-and-forget)', async () => {
    const { room, server } = await joinedRoom();
    room.broadcast(kvEph('cursor', '5'));

    const msg = await recvClient(server);
    expect(msg.body.case).toBe('broadcast');
    if (msg.body.case === 'broadcast') expect(msg.body.value.roomId).toBe('r');
  });

  it('delivers a received Ephemeral to onEphemeral', async () => {
    const seen: string[] = [];
    const { server } = await joinedRoom({
      onEphemeral: (_id, body) => {
        if (body.kind.case === 'kvSet') seen.push(dec(body.kind.value.value) ?? '');
      },
    });

    sendServer(
      server,
      create(ServerMessageSchema, {
        body: {
          case: 'ephemeral',
          value: { roomId: 'r', originClientId: 'peer', body: kvEph('cursor', '5') },
        },
      }),
    );

    await vi.waitFor(() => expect(seen).toContain('5'));
  });

  it('rejects an in-flight commit when the room is closed by the user', async () => {
    // A transport drop does NOT reject a commit (recovery re-sends it — see recovery.test.ts); only
    // an explicit close() is terminal.
    const { room, server } = await joinedRoom();
    const done = room.commit(kvBody('a', '1'));
    await recvClient(server);
    room.close();
    await expect(done).rejects.toThrow(/room closed/);
  });

  it('ignores an Ephemeral with no body or a mismatched room', async () => {
    const seen: string[] = [];
    const { room, server } = await joinedRoom({
      onEphemeral: (_id, body) => {
        if (body.kind.case === 'kvSet') seen.push(dec(body.kind.value.value) ?? '');
      },
    });

    // No body → skipped.
    sendServer(
      server,
      create(ServerMessageSchema, {
        body: { case: 'ephemeral', value: { roomId: 'r', originClientId: 'peer' } },
      }),
    );
    // Wrong room → skipped.
    sendServer(
      server,
      create(ServerMessageSchema, {
        body: {
          case: 'ephemeral',
          value: { roomId: 'other', originClientId: 'peer', body: kvEph('c', '1') },
        },
      }),
    );
    // A valid one → delivered, proving the stream still flows past the skipped frames.
    sendServer(
      server,
      create(ServerMessageSchema, {
        body: {
          case: 'ephemeral',
          value: { roomId: 'r', originClientId: 'peer', body: kvEph('c', '9') },
        },
      }),
    );

    await vi.waitFor(() => expect(seen).toEqual(['9']));
    room.close();
  });

  it('ignores a Nack for an unknown client_seq (no throw, no effect on other commits)', async () => {
    const { room, server } = await joinedRoom();
    const done = room.commit(kvBody('a', '1')); // client_seq 1
    await recvClient(server);

    sendServer(server, nackMsg(999n, NackReason.INVALID)); // unknown seq → no-op
    sendServer(server, ackEvent(3n, 1n, 'a', '1')); // the real commit still acks
    await expect(done).resolves.toBeUndefined();
    room.close();
  });

  it('does not double-resolve on a duplicate fan-out of the same commit', async () => {
    const { room, server } = await joinedRoom();
    let resolves = 0;
    const done = room.commit(kvBody('a', '1')).then(() => resolves++);
    await recvClient(server);

    sendServer(server, ackEvent(3n, 1n, 'a', '1')); // ack
    sendServer(server, ackEvent(3n, 1n, 'a', '1')); // duplicate fan-out → must be a no-op
    await done;
    await new Promise((r) => setTimeout(r, 0));
    expect(resolves).toBe(1);
    room.close();
  });
});
