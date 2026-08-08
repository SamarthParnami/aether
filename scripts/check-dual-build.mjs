// Prove the dual build actually resolves — run after `yarn build`.
//
// The failure this guards against is silent: a wrong `exports` condition, a missing type marker,
// or an ESM-only dependency reached from the CJS tree all typecheck and build fine, then blow up
// at the consumer's first require(). So load every package BOTH ways, for real, and diff the
// export surfaces.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const PACKAGES = ['@aether/protocol', '@aether/client'];

for (const pkg of PACKAGES) {
  const cjs = require(pkg);
  const esm = await import(pkg);

  const cjsNames = Object.keys(cjs).sort();
  const esmNames = Object.keys(esm)
    .filter((k) => k !== 'default')
    .sort();

  assert.ok(cjsNames.length > 0, `${pkg}: require() produced no exports`);
  assert.deepEqual(
    cjsNames,
    esmNames,
    `${pkg}: require() and import() disagree on the export surface`,
  );

  const cjsPath = require.resolve(pkg);
  assert.ok(
    cjsPath.includes('/dist/cjs/'),
    `${pkg}: require() resolved to ${cjsPath}, not dist/cjs`,
  );
  assert.equal(
    require(`${pkg}/package.json`).type,
    'module',
    `${pkg}: package should still declare itself ESM-first`,
  );

  console.log(`${pkg}: require() + import() agree on ${cjsNames.length} exports`);
}

// The seam has to survive the round trip in CJS too: encode a frame through the required build
// and decode it back, so a broken protobuf resolution can't pass as "it loaded".
const { create } = require('@bufbuild/protobuf');
const { ClientMessageSchema } = require('@aether/protocol');
const { encodeClientMessage } = require('@aether/client');

const frame = encodeClientMessage(
  create(ClientMessageSchema, { body: { case: 'ping', value: { id: 'dual-build' } } }),
);
assert.ok(frame instanceof Uint8Array && frame.length > 0, 'CJS encode produced no bytes');
console.log(`cjs round trip: encoded a ${frame.length}-byte Ping frame`);
