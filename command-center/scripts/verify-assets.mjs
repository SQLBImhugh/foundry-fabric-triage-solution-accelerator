import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'

const root = new URL('../', import.meta.url)
const index = await readFile(new URL('dist/index.html', root), 'utf8')
const assets = await readdir(new URL('dist/assets/', root))
const css = (await Promise.all(assets.filter((name) => name.endsWith('.css')).map((name) =>
  readFile(new URL(`dist/assets/${name}`, root), 'utf8'),
))).join('\n')
const scripts = (await Promise.all(assets.filter((name) => name.endsWith('.js')).map((name) =>
  readFile(new URL(`dist/assets/${name}`, root), 'utf8'),
))).join('\n')
for (const name of ['triage-logo.png', 'favicon.png']) {
  const original = await readFile(new URL(`public/${name}`, root))
  const built = await readFile(new URL(`dist/${name}`, root))
  assert(original.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])), `${name} is not a PNG`)
  assert(original.equals(built), `${name} differs from its source`)
}
assert(index.includes('/favicon.png'), 'The supplied artwork is not used for the browser icon')
assert(scripts.includes('/triage-logo.png'), 'The supplied artwork is not used by the application')
for (const [name, licenseName, copyrightHolder] of [
  ['SourceSans3-Variable.ttf', 'SourceSans3-OFL.txt', 'Adobe'],
  ['IBMPlexMono-Regular.ttf', 'IBMPlexMono-OFL.txt', 'IBM Corp.'],
]) {
  const original = await readFile(new URL(`public/fonts/${name}`, root))
  const built = await readFile(new URL(`dist/fonts/${name}`, root))
  assert(original.length > 1000, `${name} is not a complete font asset`)
  assert.equal(original.readUInt32BE(0), 0x00010000, `${name} is not a TrueType font`)
  assert.equal(createHash('sha256').update(original).digest('hex'), createHash('sha256').update(built).digest('hex'), `${name} differs from its source`)
  assert(index.includes(`/fonts/${name}`), `${name} is not preloaded`)
  assert(css.includes(`/fonts/${name}`), `${name} is not used by the production stylesheet`)
  const originalLicense = await readFile(new URL(`public/fonts/${licenseName}`, root), 'utf8')
  const builtLicense = await readFile(new URL(`dist/fonts/${licenseName}`, root), 'utf8')
  assert.equal(builtLicense, originalLicense, `${licenseName} differs from its source`)
  assert(builtLicense.includes('SIL OPEN FONT LICENSE Version 1.1') && builtLicense.includes(copyrightHolder), `${name} is missing its distribution license`)
}
assert(!/DejaVu|Georgia/.test(css), 'The production stylesheet still uses the replaced serif font stack')
assert(!/@import[^;]*https?:|url\(["']?https?:/i.test(css), 'The production stylesheet requests an external asset')
assert(index.includes('data-theme="dark"'), 'Dark is not the default theme')
console.log(`Verified licensed, self-hosted Source Sans 3 and IBM Plex Mono in ${join('dist', 'fonts')}.`)
console.log('Verified the supplied triage logo and browser icon in the production assets.')
