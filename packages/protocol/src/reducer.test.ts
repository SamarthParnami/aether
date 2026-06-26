import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { EventBodySchema } from './gen/aether/v1/events_pb.js';
import { emptyState, fold } from './reducer.js';

interface GoldenSuite {
  cases: {
    name: string;
    events: { key: string; value: string }[];
    expected: Record<string, string>;
  }[];
}

// The SAME fixtures the Go roomcore golden test uses. Resolve the path relative to THIS
// file (not process.cwd()) so the parity check keeps running even if the suite is run
// package-scoped — a silently-skipped guard would be the worst failure mode here.
const goldenPath = fileURLToPath(
  new URL('../../../testdata/golden/roomcore.json', import.meta.url),
);
const suite = JSON.parse(readFileSync(goldenPath, 'utf8')) as GoldenSuite;

describe('roomcore golden vectors (TS ↔ Go parity)', () => {
  it('has cases', () => {
    expect(suite.cases.length).toBeGreaterThan(0);
  });

  for (const tc of suite.cases) {
    it(tc.name, () => {
      const state = emptyState();
      for (const e of tc.events) {
        const body = create(EventBodySchema, {
          kind: { case: 'kvSet', value: { key: e.key, value: new TextEncoder().encode(e.value) } },
        });
        fold(state, body);
      }

      const got: Record<string, string> = {};
      for (const [k, v] of state) {
        got[k] = new TextDecoder().decode(v);
      }
      expect(got).toEqual(tc.expected);
    });
  }
});
