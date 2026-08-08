import {
  type ClientMessage,
  ClientMessageSchema,
  type EventBody,
  EventBodySchema,
  NackReason,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { CommitUnconfirmedError, Room } from './room.js';
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

function errorMsg(code: string, message: string): ServerMessage {
  return create(ServerMessageSchema, { body: { case: 'error', value: { code, message } } });
}
function nackMsg(clientSeq: bigint, reason: NackReason): ServerMessage {
  return create(ServerMessageSchema, {
    body: { case: 'nack', value: { roomId: 'r', clientSeq, reason } },
  });
}

const fromSeqOf = (m: ClientMessage) => (m.body.case === 'join' ? m.body.value.fromSeq : -1n);
const commitSeqOf = (m: ClientMessage) => (m.body.case === 'commit' ? m.body.value.clientSeq : -1n);
const nonceOf = (m: ClientMessage) => (m.body.case === 'join' ? m.body.value.sessionNonce : '');

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
    const rejoin = await recvClient(serverAt(servers, 1));
    expect(fromSeqOf(rejoin)).toBe(2n); // resumed, not from zero
    // The nonce MUST be the one we joined with: the gateway derives client_id = HMAC(principal,
    // session_nonce), so a fresh nonce here would mint a new client_id, miss the owner's
    // (client_id, client_seq) dedup high-water, and apply the re-sent commit A SECOND TIME.
    expect(nonceOf(rejoin)).toBe('n');
    sendServer(serverAt(servers, 1), joinedResume(2n));
    expect(room.clientId()).toBe('c1');
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
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {})); // fresh join establishes the cursor
    await connected;

    const done = room.commit(kvBody('a', '1'));
    await recvClient(serverAt(servers, 0));
    room.close();

    await expect(done).rejects.toThrow(/room closed/);
    // No reconnect after a user close, even across a tick.
    await new Promise((r) => setTimeout(r, 0));
    expect(servers).toHaveLength(1);
  });

  it('buffers a commit issued while disconnected and sends it on reconnect', async () => {
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {}));
    await connected;

    // Drop, THEN commit — while there is no live connection. The commit must not be lost.
    serverAt(servers, 0).close(new Error('drop'));
    const done = room.commit(kvBody('x', '1'));

    await waitForServer(servers, 2);
    expect(fromSeqOf(await recvClient(serverAt(servers, 1)))).toBe(2n);
    sendServer(serverAt(servers, 1), joinedResume(2n));
    expect(commitSeqOf(await recvClient(serverAt(servers, 1)))).toBe(1n); // sent on reconnect
    sendServer(serverAt(servers, 1), ackEvent(3n, 1n, 'x', '1'));
    await expect(done).resolves.toBeUndefined();

    room.close();
  });

  it('grows the reconnect backoff attempt across consecutive failed reconnects', async () => {
    const { dial, servers } = harness();
    const attempts: number[] = [];
    const room = new Room({
      dial,
      roomId: 'r',
      sessionNonce: 'n',
      reconnectDelayMs: (a) => {
        attempts.push(a);
        return 0;
      },
    });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(0n, {})); // first join resets the attempt counter
    await connected;

    // Each reconnect is dropped before its Joined, so the attempt counter is never reset → it grows.
    for (let i = 0; i < 3; i++) {
      serverAt(servers, i).close(new Error('drop'));
      await waitForServer(servers, i + 2);
      await recvClient(serverAt(servers, i + 1)); // consume the reconnect Join, send no Joined
    }

    expect(attempts).toEqual([0, 1, 2]); // reconnectDelayMs consulted with a growing attempt
    room.close();
  });

  it('reuses a generated session nonce across reconnects', async () => {
    // Same invariant as above, for the path where the SDK mints the nonce itself.
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    const first = nonceOf(await recvClient(serverAt(servers, 0)));
    expect(first).not.toBe('');
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {}));
    await connected;

    serverAt(servers, 0).close(new Error('drop'));
    await waitForServer(servers, 2);
    expect(nonceOf(await recvClient(serverAt(servers, 1)))).toBe(first);

    room.close();
  });

  it('retries the Join when the gateway answers it with a transient Error (socket stays open)', async () => {
    // The gateway's no-owner path replies Error{UNAVAILABLE} and returns WITHOUT starting a relay,
    // leaving the WebSocket open — so no onClose fires and nothing else would ever retry.
    const { dial, servers } = harness();
    const codes: string[] = [];
    const room = new Room({
      dial,
      roomId: 'r',
      sessionNonce: 'n',
      reconnectDelayMs: () => 0,
      onError: (c) => codes.push(c),
    });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), errorMsg('UNAVAILABLE', 'room has no reachable owner'));

    // A second connection is dialed and re-Joins; the room comes up once the owner is placed.
    await waitForServer(servers, 2);
    expect(fromSeqOf(await recvClient(serverAt(servers, 1)))).toBe(0n);
    sendServer(serverAt(servers, 1), joinedSnapshot(2n, { slide: '7' }));
    await expect(connected).resolves.toBeUndefined();
    expect(codes).toEqual(['UNAVAILABLE']); // still surfaced to the app
    expect(room.isLive()).toBe(true);

    room.close();
  });

  it('fails connect() and stops retrying when the Join is rejected as INVALID', async () => {
    // INVALID = malformed frame / bad or mismatched session_nonce. Retrying cannot help.
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), errorMsg('INVALID', 'session_nonce required'));

    await expect(connected).rejects.toThrow(/join rejected: INVALID/);
    await new Promise((r) => setTimeout(r, 0));
    expect(servers).toHaveLength(1); // no reconnect storm against a request that can never succeed
  });

  it('connect() is idempotent: a second call dials once more and both promises settle', async () => {
    // A second dial used to orphan a fully-wired socket. Worse, the orphan stayed attached to our
    // handlers, so its eventual drop cleared the pointer to the HEALTHY connection — writes went
    // nowhere and the app was told FROZEN over a live link.
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });

    const a = room.connect();
    const b = room.connect();

    await waitForServer(servers, 1);
    await new Promise((r) => setTimeout(r, 0));
    expect(servers).toHaveLength(1); // exactly one transport dialed

    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {}));
    await expect(a).resolves.toBeUndefined();
    await expect(b).resolves.toBeUndefined(); // the first caller's promise is not orphaned

    room.close();
  });

  it('rejects commit() and connect() on a room closed by a terminal INVALID join', async () => {
    // onServerError's INVALID branch marks the Room terminal WITHOUT the app calling close(), so a
    // later commit() had nothing left to re-drive it and parked forever.
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), errorMsg('INVALID', 'session_nonce required'));
    await expect(connected).rejects.toThrow(/join rejected: INVALID/);

    await expect(room.commit(kvBody('a', '1'))).rejects.toThrow(/room closed/);
    await expect(room.connect()).rejects.toThrow(/room closed/);
  });

  it('keeps a commit buffered on a transient NOT_JOINED Nack and re-drives it after the Join', async () => {
    // NOT_JOINED means this connection has no relay for the room yet — transient during a re-home,
    // not a refusal. Rejecting it turned a survivable failover into a permanent write failure.
    const { dial, servers } = harness();
    const room = new Room({
      dial,
      roomId: 'r',
      sessionNonce: 'n',
      reconnectDelayMs: () => 0,
      commitRetryMs: 5,
    });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {}));
    await connected;

    const done = room.commit(kvBody('a', '1'));
    expect(commitSeqOf(await recvClient(serverAt(servers, 0)))).toBe(1n);

    let settled = false;
    void done.then(
      () => (settled = true),
      () => (settled = true),
    );
    sendServer(serverAt(servers, 0), nackMsg(1n, NackReason.NOT_JOINED));
    await new Promise((r) => setTimeout(r, 0));
    expect(settled).toBe(false); // buffered, not rejected

    // The retry timer re-drives it on the same connection, and it acks normally.
    expect(commitSeqOf(await recvClient(serverAt(servers, 0)))).toBe(1n);
    sendServer(serverAt(servers, 0), ackEvent(3n, 1n, 'a', '1'));
    await expect(done).resolves.toBeUndefined();

    room.close();
  });

  it('rejects a ping issued while disconnected instead of hanging on a dead transport', async () => {
    const { dial, servers } = harness();
    const room = new Room({
      dial,
      roomId: 'r',
      sessionNonce: 'n',
      reconnectDelayMs: () => 60_000, // park the reconnect so we stay disconnected
    });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, {}));
    await connected;

    serverAt(servers, 0).close(new Error('drop'));
    await vi.waitFor(() => expect(room.isLive()).toBe(false));

    // Previously this parked a waiter that never settled AND poisoned the id forever.
    await expect(room.ping('hb')).rejects.toThrow(/not connected/);
    await expect(room.ping('hb')).rejects.toThrow(/not connected/); // id is reusable, not poisoned

    room.close();
  });

  it('gives up on a commit the owner can never re-fan, with an explicit unconfirmed outcome', async () => {
    // A commit subsumed by a reconnect's snapshot is deduped by the owner and never re-fanned, so
    // fan-out-is-the-ack has no path. Joined carries no per-client dedup high-water, so the SDK
    // cannot tell "already durable" from "never applied" — it must stop and say so.
    const { dial, servers } = harness();
    const room = new Room({
      dial,
      roomId: 'r',
      sessionNonce: 'n',
      reconnectDelayMs: () => 0,
      commitRetryMs: 1,
      commitMaxAttempts: 3,
    });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(0n, {}));
    await connected;

    const done = room.commit(kvBody('slide', '9'));
    // The server never acks — exactly what a deduped resend looks like on the wire.
    await expect(done).rejects.toBeInstanceOf(CommitUnconfirmedError);
    await expect(done).rejects.toThrow(/may or may not be durable/);

    room.close();
  });

  // NOTE: this pins the SDK's ack-before-gate rule — an Event whose room_seq is already ≤ our cursor
  // still acks — for the case where an owner DOES re-fan a subsumed commit. It is not a claim that
  // the Go owner guarantees that: roomruntime.Tail streams strictly room_seq > from_seq, so a commit
  // subsumed by the Join snapshot is normally never re-delivered. That path is the preceding
  // CommitUnconfirmedError test.
  it('resolves an outstanding commit subsumed by a deep-resume snapshot, if the owner re-fans it', async () => {
    const { dial, servers } = harness();
    const room = new Room({ dial, roomId: 'r', sessionNonce: 'n', reconnectDelayMs: () => 0 });
    const connected = room.connect();

    await waitForServer(servers, 1);
    await recvClient(serverAt(servers, 0));
    sendServer(serverAt(servers, 0), joinedSnapshot(2n, { slide: '7' }));
    await connected;

    const done = room.commit(kvBody('slide', '9')); // applies at owner as room_seq 3, but the ack is lost
    expect(commitSeqOf(await recvClient(serverAt(servers, 0)))).toBe(1n);
    serverAt(servers, 0).close(new Error('drop'));

    await waitForServer(servers, 2);
    expect(fromSeqOf(await recvClient(serverAt(servers, 1)))).toBe(2n);
    // Deep resume: our cursor fell below the floor → a fresh snapshot at 5 (already includes our write).
    sendServer(serverAt(servers, 1), joinedSnapshot(5n, { slide: '9' }));
    expect(commitSeqOf(await recvClient(serverAt(servers, 1)))).toBe(1n); // resent…
    // …the owner dedups (already applied) and RE-FANS the subsumed event; ack-before-gate resolves it
    // even though room_seq 3 ≤ cursor 5.
    sendServer(serverAt(servers, 1), ackEvent(3n, 1n, 'slide', '9'));
    await expect(done).resolves.toBeUndefined();
    expect(room.currentSeq()).toBe(5n); // cursor stays at the snapshot; the replay didn't move it

    room.close();
  });
});
