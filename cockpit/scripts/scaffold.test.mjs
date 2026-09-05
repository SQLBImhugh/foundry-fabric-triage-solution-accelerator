// scripts/scaffold.test.mjs — guards the pack seeding contract.
//
// Run with `npm test` (Vitest; see vitest.config.ts).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { test } from 'vitest';

import { classifySeed, seedDigest } from './scaffold.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
// Normalize to LF so fixtures are identical on a CRLF checkout.
const read = (rel) => readFileSync(join(ROOT, rel), 'utf8').replace(/\r\n/g, '\n');

const BASE_APP = read('src/App.tsx');

const analyticsPack = JSON.parse(read('.agents/skills/analytics/pack.json'));
const seedEntry = (to) => analyticsPack.copy.find((e) => e.to === to);

// The exact wiring `.agents/skills/authentication/SKILL.md` tells you to write.
const AUTH_WIRED_MAIN = `import { createRoot } from 'react-dom/client';

import App from '@/App';
import { AuthProvider } from '@/hooks/AuthContext';
import { bootstrapAuth } from '@/services/bootstrap';

import './main.css';

const authService = bootstrapAuth();

createRoot(document.getElementById('root')!).render(
  <AuthProvider authService={authService}>
    <App />
  </AuthProvider>
);
`;

test('a missing destination is seeded', () => {
  assert.equal(classifySeed(seedEntry('src/App.tsx'), undefined), 'write');
});

// Three tests were removed here, deliberately. They read src/App.tsx and
// src/main.tsx off disk as "the pristine base starter" and asserted the
// analytics pack could still seed over them. That holds only while the app is
// an unmodified scaffold; this repo's cockpit replaced both files, which is
// what the scaffold exists to let you do, so the assertions became unfailable
// noise ("analytics pack.json is missing the digest of the current
// src/App.tsx"). They were red in the two commits that added the cockpit,
// masked because build:fabric ran `tsc -b --noCheck` and nobody ran the suite.
// The remaining tests exercise classifySeed's real logic against inline
// fixtures and do not care what the app looks like.

test('an auth-wired main.tsx is preserved, not clobbered', () => {
  // Regression: the old substring guard keyed on "./main.css", which the
  // authentication skill's own output keeps — so it declared this file pristine
  // and destroyed the auth wiring.
  assert.ok(AUTH_WIRED_MAIN.includes('./main.css'));
  assert.equal(classifySeed(seedEntry('src/main.tsx'), AUTH_WIRED_MAIN), 'preserve');
});

test('any unrecognized content is preserved', () => {
  assert.equal(classifySeed(seedEntry('src/App.tsx'), '// my own app\n'), 'preserve');
});

test('--force-seeds overwrites a customized file', () => {
  assert.equal(
    classifySeed(seedEntry('src/main.tsx'), AUTH_WIRED_MAIN, { force: true }),
    'write',
  );
});

test('a legacy seedReplaceIfContains entry preserves rather than guesses', () => {
  const legacy = { from: 'kit/app/App.tsx', to: 'src/App.tsx', seedReplaceIfContains: 'HomePage' };
  assert.equal(classifySeed(legacy, BASE_APP), 'preserve');
  assert.equal(classifySeed(legacy, undefined), 'write');
});

test('re-applying a pack over its own seed is a silent no-op', () => {
  const kitApp = read('.agents/skills/analytics/kit/app/App.tsx');
  assert.equal(
    classifySeed(seedEntry('src/App.tsx'), kitApp, { sourceText: kitApp }),
    'current',
  );
});

test('digests ignore line-ending and BOM differences', () => {
  assert.equal(seedDigest(BASE_APP), seedDigest(`\uFEFF${BASE_APP.replace(/\n/g, '\r\n')}`));
});
