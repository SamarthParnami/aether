/**
 * @aether/client — the TypeScript SDK for the Aether real-time state backbone.
 *
 * One disposable WebSocket per client; correctness via recovery (cursor resume + idempotent replay),
 * not redundant connections — the same contract the Go backbone enforces. This entrypoint currently
 * exposes the transport seam, wire codec, and the Room (Join → state); the commit and recovery
 * layers land in subsequent PRs (see the SDK PR plan).
 */
export * from './codec.js';
export * from './room.js';
export * from './transport.js';
