// Stamp each build output directory with the module type Node should read it as.
//
// tsc emits `.js` for both formats, so without these markers Node would interpret dist/cjs/*.js
// as ESM (the package itself is "type": "module") and every require() would throw. Writing a
// nested package.json overrides `type` for that subtree — the same trick @bufbuild/protobuf uses.
import { mkdir, readdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
const packagesDir = join(repoRoot, 'packages');

const MARKERS = [
  ['esm', 'module'],
  ['cjs', 'commonjs'],
];

const packages = (await readdir(packagesDir, { withFileTypes: true }))
  .filter((e) => e.isDirectory())
  .map((e) => e.name);

for (const pkg of packages) {
  for (const [dir, type] of MARKERS) {
    const out = join(packagesDir, pkg, 'dist', dir);
    await mkdir(out, { recursive: true });
    await writeFile(join(out, 'package.json'), `${JSON.stringify({ type }, null, 2)}\n`);
  }
}
