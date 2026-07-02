import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { ClientMessageSchema, type ServerMessage, ServerMessageSchema } from '@aether/protocol';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { Room } from './room.js';
import { memoryTransportPair, type ServerEnd } from './transport.js';

// The SAME golden fixtures the Go roomcore test and the @aether/protocol reducer test use — resolved
// relative to THIS file so a package-scoped run still finds them (a silently-skipped parity guard is
// the worst outcome). Here we drive the vectors END TO END through the SDK: each event arrives as a
// live durable Event over the wire, and the Room's materialized state must equal the golden expected.
// That extends the reducer-only parity to the SDK's actual fold-on-the-event-stream path.
interface GoldenSuite {
  cases: {
    name: string;
    events: { key: string; value: string }[];
    expected: Record<string, string>;
  }[];
}
const goldenPath = fileURLToPath(
  new URL('../../../testdata/golden/roomcore.json', import.meta.url),
);
const suite = JSON.parse(readFileSync(goldenPath, 'utf8')) as GoldenSuite;

const enc = (s: string) => new TextEncoder().encode(s);

function sendServer(server: ServerEnd, msg: ServerMessage): void {
  server.send(toBinary(ServerMessageSchema, msg));
}
async function recvJoin(server: ServerEnd): Promise<void> {
  const frame = await server.recv();
  if (frame === null) throw new Error('expected the Join');
  fromBinary(ClientMessageSchema, frame);
}
const joinedFresh = create(ServerMessageSchema, {
  body: { case: 'joined', value: { roomId: 'r', clientId: 'c1', currentSeq: 0n } },
});
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

/** Fresh-join, stream the events as durable Events, and read back the SDK's materialized state. */
async function materializeViaSDK(
  events: { key: string; value: string }[],
): Promise<Record<string, string>> {
  const { transport, server } = memoryTransportPair();
  const room = new Room({ dial: () => transport, roomId: 'r', sessionNonce: 'n' });
  const connected = room.connect();
  await recvJoin(server);
  sendServer(server, joinedFresh);
  await connected;

  let seq = 0n;
  for (const e of events) {
    seq += 1n;
    sendServer(server, eventMsg(seq, e.key, e.value));
  }
  await vi.waitFor(() => expect(room.currentSeq()).toBe(seq));

  const got: Record<string, string> = {};
  for (const [k, v] of room.getState()) got[k] = new TextDecoder().decode(v);
  room.close();
  return got;
}

describe('SDK ↔ Go parity (golden vectors through the event stream)', () => {
  it('has cases', () => {
    expect(suite.cases.length).toBeGreaterThan(0);
  });

  for (const tc of suite.cases) {
    it(tc.name, async () => {
      expect(await materializeViaSDK(tc.events)).toEqual(tc.expected);
    });
  }
});
