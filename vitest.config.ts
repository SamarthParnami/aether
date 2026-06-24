import { defineConfig } from 'vitest/config';

// Single root project for now. The unit / integration / chaos `test.projects`
// split lands when integration tests arrive (Testcontainers, later PRs).
export default defineConfig({
  test: {
    include: ['packages/**/*.test.ts', 'apps/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
    },
  },
});
