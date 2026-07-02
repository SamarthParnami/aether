/**
 * Codec — the SDK's view of the wire envelope: encode the frames we send (ClientMessage), decode the
 * frames we receive (ServerMessage). Thin wrappers over Buf's binary serializer, kept in one place so
 * the transport and connection layers never touch `@bufbuild/protobuf` directly. The reverse
 * direction (decode ClientMessage / encode ServerMessage) is only ever needed by a scripted test
 * server, so it lives in the tests, not here.
 */
import {
  type ClientMessage,
  ClientMessageSchema,
  type ServerMessage,
  ServerMessageSchema,
} from '@aether/protocol';
import { fromBinary, toBinary } from '@bufbuild/protobuf';

/** Serialize a ClientMessage to a binary frame for the transport. */
export function encodeClientMessage(msg: ClientMessage): Uint8Array {
  return toBinary(ClientMessageSchema, msg);
}

/** Parse a binary frame from the transport into a ServerMessage. */
export function decodeServerMessage(data: Uint8Array): ServerMessage {
  return fromBinary(ServerMessageSchema, data);
}
