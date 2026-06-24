import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { ClientMessageSchema, ServerMessageSchema } from './gen/aether/v1/aether_pb.js';

describe('wire envelope', () => {
  it('round-trips a durable Commit with its dedup key', () => {
    const msg = create(ClientMessageSchema, {
      body: {
        case: 'commit',
        value: { roomId: 'room-1', clientSeq: 42n, body: {} },
      },
    });

    const back = fromBinary(ClientMessageSchema, toBinary(ClientMessageSchema, msg));

    expect(back.body.case).toBe('commit');
    if (back.body.case === 'commit') {
      expect(back.body.value.roomId).toBe('room-1');
      expect(back.body.value.clientSeq).toBe(42n);
    }
  });

  it('round-trips an Event carrying the origin dedup key (fan-out is the ack)', () => {
    const msg = create(ServerMessageSchema, {
      body: {
        case: 'event',
        value: {
          roomId: 'room-1',
          roomSeq: 416n,
          originClientId: 'client-A',
          originClientSeq: 42n,
        },
      },
    });

    const back = fromBinary(ServerMessageSchema, toBinary(ServerMessageSchema, msg));

    expect(back.body.case).toBe('event');
    if (back.body.case === 'event') {
      expect(back.body.value.roomSeq).toBe(416n);
      expect(back.body.value.originClientSeq).toBe(42n);
    }
  });
});
