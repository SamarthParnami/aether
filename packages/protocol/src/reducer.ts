import type { EventBody } from './gen/aether/v1/events_pb.js';

/**
 * Materialized room state — the client-side mirror of the Go `roomcore` reducer.
 *
 * Phase 1 is a generic last-write-wins key/value map. The Go server and this TS reducer
 * are independent implementations kept in lockstep by the shared golden vectors
 * (testdata/golden/roomcore.json), so the two ends can never drift on what an event means.
 */
export type MaterializedState = Map<string, Uint8Array>;

/** A fresh, empty room state. */
export function emptyState(): MaterializedState {
  return new Map();
}

/** Fold one event body into state (mutating it). Last-write-wins for KeyValueSet. */
export function fold(state: MaterializedState, body: EventBody): void {
  if (body.kind.case === 'kvSet') {
    state.set(body.kind.value.key, body.kind.value.value);
  }
}

/** Rebuild state by folding events in order — the client-side reconstruction path. */
export function replay(events: EventBody[]): MaterializedState {
  const state = emptyState();
  for (const body of events) {
    fold(state, body);
  }
  return state;
}
