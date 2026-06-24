// @ts-check
import eslint from '@eslint/js';
import prettier from 'eslint-config-prettier';
import simpleImportSort from 'eslint-plugin-simple-import-sort';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    ignores: ['**/dist/**', '**/node_modules/**', '**/gen/**', '**/.turbo/**', '**/coverage/**'],
  },

  // Type-checked linting for package/app source.
  {
    files: ['packages/**/*.ts', 'packages/**/*.tsx', 'apps/**/*.ts', 'apps/**/*.tsx'],
    extends: [eslint.configs.recommended, ...tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: { 'simple-import-sort': simpleImportSort },
    rules: {
      'simple-import-sort/imports': 'error',
      'simple-import-sort/exports': 'error',
    },
  },

  // Root config files (eslint.config.js, vitest.config.ts, …): no type info needed.
  {
    files: ['*.ts', '*.js', '*.mjs', '*.cjs'],
    extends: [eslint.configs.recommended, ...tseslint.configs.recommended],
  },

  prettier,
);
