/**
 * @aether/client — the TypeScript SDK for the Aether real-time state backbone.
 *
 * One disposable WebSocket per client; correctness via recovery (cursor resume + idempotent replay),
 * not redundant connections — the same contract the Go backbone enforces. This entrypoint exposes
 * the transport seam, the wire codec, the Room (Join → state → commit → recovery), and the
 * `useSyncExternalStore` adapter.
 */
export * from './codec.js';
export * from './room.js';
export * from './store.js';
export * from './transport.js';
