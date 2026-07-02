/**
 * A React-`useSyncExternalStore`-ready adapter over a {@link Room}.
 *
 * The Room already exposes the three things an external store needs — `subscribe` (change
 * notifications), `getVersion` (a value that changes iff the state changed), and `getState` — so this
 * is a thin, dependency-free binding. It is the engine of the useState-like hook; the SDK stays
 * framework-agnostic (no React import), and a consumer wires the actual hook in two lines:
 *
 * ```ts
 * import { useMemo, useSyncExternalStore } from 'react';
 * import { createRoomStore, type Room } from '@aether/client';
 *
 * export function useAetherState(room: Room): ReadonlyMap<string, Uint8Array> {
 *   const store = useMemo(() => createRoomStore(room), [room]);
 *   useSyncExternalStore(store.subscribe, store.getSnapshot);
 *   return store.getState();
 * }
 * ```
 *
 * `getSnapshot` returns the version number (a primitive), so it is referentially stable when nothing
 * changed — the identity check `useSyncExternalStore` relies on to avoid needless re-renders, even
 * though the underlying state is a mutated `Map`.
 */
import type { Room } from './room.js';

export interface RoomStore {
  /** `useSyncExternalStore` subscribe: register a change callback; returns the unsubscribe. */
  subscribe(onStoreChange: () => void): () => void;
  /** `useSyncExternalStore` getSnapshot: a primitive that changes iff {@link getState} changed. */
  getSnapshot(): number;
  /** The current materialized room state — read this after a snapshot change. */
  getState(): ReadonlyMap<string, Uint8Array>;
}

/** Wrap a {@link Room} as a {@link RoomStore} for `useSyncExternalStore` (or any observer). */
export function createRoomStore(room: Room): RoomStore {
  return {
    subscribe: (onStoreChange) => room.subscribe(onStoreChange),
    getSnapshot: () => room.getVersion(),
    getState: () => room.getState(),
  };
}
