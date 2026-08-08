import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

const src = (pkg: string) =>
  fileURLToPath(new URL(`./packages/${pkg}/src/index.ts`, import.meta.url));

// Single root project for now. The unit / integration / chaos `test.projects`
// split lands when integration tests arrive (Testcontainers, later PRs).
export default defineConfig({
  // Resolve workspace packages to SOURCE. Their package.json `exports` map points at dist/, which
  // only exists after `yarn build` — tests must not depend on build state. The built artifacts are
  // covered separately by `yarn verify:dual-build`.
  resolve: {
    alias: {
      '@aether/protocol': src('protocol'),
      '@aether/client': src('client'),
    },
  },
  test: {
    include: ['packages/**/*.test.ts', 'apps/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
    },
  },
});
