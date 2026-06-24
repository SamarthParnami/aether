/**
 * Aether wire protocol — generated Protobuf types (Buf) + protocol version.
 *
 * Generated code lives under ./gen (produced by `buf generate` / `task proto:gen`)
 * and is not checked in; CI generates it before building.
 */
export * from './gen/aether/v1/aether_pb.js';
export * from './gen/aether/v1/events_pb.js';

/** Bumped only on backward-incompatible wire changes (guarded by `buf breaking`). */
export const PROTOCOL_VERSION = 1 as const;
