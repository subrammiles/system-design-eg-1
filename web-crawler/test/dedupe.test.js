// Integration test for content dedupe (SHA-256 + SimHash/LSH) against a real Redis.
// Opt-in, because it FLUSHES the selected database:
//   TEST_REDIS_URL=redis://localhost:6391/15 npm test
import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import Redis from 'ioredis';
import { ContentDedupe } from '../src/parser/contentDedupe.js';
import { hamming, simhash } from '../src/shared/simhash.js';

const url = process.env.TEST_REDIS_URL;
let redis;
let available = false;

before(async () => {
  if (!url) return;
  redis = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => null });
  redis.on('error', () => {});
  try {
    await redis.connect();
    available = true;
  } catch {
    console.log(`# dedupe tests skipped: no Redis at ${url}`);
  }
});

after(async () => {
  if (available) await redis.flushdb();
  redis?.disconnect();
});

// In-memory stand-in for the DynamoDB content_hashes table.
function fakeRepo() {
  const owners = new Map();
  return {
    async claimContent(hash, owner) {
      if (!owners.has(hash)) owners.set(hash, owner);
      return owners.get(hash);
    },
  };
}

const VOCAB = 'alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon'.split(' ');
const text = (seed, n = 200) => {
  let x = seed;
  return Array.from({ length: n }, () => VOCAB[(x = (x * 1103515245 + 12345) % 2 ** 31) % VOCAB.length]).join(' ');
};

test('exact duplicate, near duplicate and unique pages', async (t) => {
  if (!available) return t.skip('set TEST_REDIS_URL');
  await redis.flushdb();
  const d = new ContentDedupe({ redis, repo: fakeRepo(), maxDistance: 3 });
  const base = text(7);

  assert.equal((await d.check('A', base)).duplicateOf, null);
  const exact = await d.check('B', `  ${base.toUpperCase()} `); // same after normalization
  assert.deepEqual([exact.kind, exact.duplicateOf], ['exact', 'A']);

  // Small edit at the end: changes few shingles.
  const near = await d.check('C', `${base} omega`);
  assert.equal(near.kind, 'near', `distance ${near.distance}`);
  assert.equal(near.duplicateOf, 'A');
  assert.equal(await redis.hget('simhash:fp', 'C'), null, 'duplicates leave the index');

  assert.equal((await d.check('D', text(999))).duplicateOf, null);
  assert.equal((await d.check('A', base)).duplicateOf, null, 'recrawl of the same URL is not its own duplicate');
});

test('concurrent near-identical pages: exactly one stays unique', async (t) => {
  if (!available) return t.skip('set TEST_REDIS_URL');
  let checked = 0;
  for (let round = 0; round < 40; round++) {
    const base = text(100 + round);
    // Only pairs that ARE within the threshold must collapse to one unique page.
    if (hamming(simhash(base), simhash(`${base} omega`)) > 3) continue;
    checked++;
    await redis.flushdb();
    const d = new ContentDedupe({ redis, repo: fakeRepo(), maxDistance: 3 });
    const results = await Promise.all([d.check('X', base), d.check('Y', `${base} omega`)]);
    const unique = results.filter((r) => !r.duplicateOf);
    assert.equal(unique.length, 1, `round ${round}: ${JSON.stringify(results.map((r) => r.duplicateOf))}`);
  }
  assert.ok(checked >= 10, `only ${checked} rounds within threshold`);
});
