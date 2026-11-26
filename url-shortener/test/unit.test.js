import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decode, encode } from '../src/shared/base62.js';
import { makeScrambler, RangeIdGenerator } from '../src/shared/idGenerator.js';
import { normalizeUrl, validateAlias } from '../src/shared/urlUtils.js';
import { LocalLru, NOT_FOUND } from '../src/shared/cache.js';
import { metricsFor, parseUserAgent } from '../src/shared/clickEvents.js';
import { AlreadyExistsError } from '../src/shared/urlRepository.js';
import { ConflictError, Shortener } from '../src/url-service/shortener.js';
import { Resolver } from '../src/redirect-service/resolver.js';

test('base62 round-trips and pads', () => {
  for (const n of [0, 1, 61, 62, 125, 3843, 56_800_235_583, 2 ** 53]) {
    assert.equal(decode(encode(n)), BigInt(n));
  }
  assert.equal(encode(62), '10');
  assert.equal(encode(5, 7).length, 7);
});

test('scrambler is a bijection and hides sequence', () => {
  const s = makeScrambler(7);
  const seen = new Set();
  for (let i = 0n; i < 5000n; i++) {
    const y = s.scramble(i);
    assert.ok(y < s.space);
    assert.equal(s.unscramble(y), i);
    seen.add(y);
  }
  assert.equal(seen.size, 5000);
  assert.notEqual(encode(s.scramble(1n), 7).slice(0, 5), encode(s.scramble(2n), 7).slice(0, 5));
});

test('range generator reserves blocks and never repeats under concurrency', async () => {
  let counter = 0n;
  let calls = 0;
  const gen = new RangeIdGenerator({
    rangeSize: 10,
    codeLength: 7,
    reserveRange: async () => {
      calls++;
      await new Promise((r) => setTimeout(r, 5));
      counter += 10n;
      return counter;
    },
  });
  const codes = await Promise.all(Array.from({ length: 95 }, () => gen.nextCode()));
  assert.equal(new Set(codes).size, 95);
  assert.ok(codes.every((c) => c.length === 7));
  assert.equal(calls, 10); // ceil(95 / 10)
});

test('normalizeUrl makes equivalent urls identical', () => {
  const a = normalizeUrl('Example.com');
  assert.equal(a, 'https://example.com/');
  assert.equal(normalizeUrl('https://EXAMPLE.com:443/'), a);
  assert.equal(
    normalizeUrl('https://example.com/p/?b=2&utm_source=x&a=1#frag'),
    'https://example.com/p?a=1&b=2',
  );
  assert.throws(() => normalizeUrl('javascript:alert(1)'));
  assert.throws(() => normalizeUrl('ftp://example.com'));
  assert.throws(() => normalizeUrl('http://short.ly/abc', { ownHost: 'short.ly' }), /points to this shortener/);
});

test('validateAlias rejects reserved, malformed and blocked aliases', () => {
  assert.equal(validateAlias('sale50'), 'sale50');
  assert.throws(() => validateAlias('admin'), /reserved/);
  assert.throws(() => validateAlias('a'), /3-32/);
  assert.throws(() => validateAlias('bad/slash'));
});

test('LocalLru evicts oldest and expires entries', () => {
  const lru = new LocalLru({ maxEntries: 2, ttlMs: 1000 });
  lru.set('a', 1);
  lru.set('b', 2);
  lru.get('a');
  lru.set('c', 3);
  assert.equal(lru.get('b'), undefined);
  assert.equal(lru.get('a'), 1);
  lru.set('d', 4, -1);
  assert.equal(lru.get('d'), undefined);
});

test('user agent parsing and metrics', () => {
  const iphone = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit Version/17.0 Mobile Safari/604.1';
  assert.deepEqual(parseUserAgent(iphone), { device: 'mobile', browser: 'safari' });
  assert.equal(parseUserAgent('curl/8.0').device, 'bot');
  const m = metricsFor({ ts: Date.UTC(2026, 10, 26), userAgent: iphone, country: 'IN', referrer: 'https://t.co/x' });
  assert.deepEqual(m, ['total', 'day#2026-11-26', 'device#mobile', 'browser#safari', 'country#IN', 'referrer#t.co']);
});

// ---- Shortener / Resolver with in-memory fakes ----

function fakeRepo() {
  const urls = new Map();
  const hashes = new Map();
  return {
    urls,
    async getByCode(c) { return urls.get(c) ?? null; },
    async insert(item) {
      if (urls.has(item.short_code)) throw new AlreadyExistsError(item.short_code);
      urls.set(item.short_code, { ...item });
    },
    async getByHash(h) { return hashes.get(h) ?? null; },
    async claimHash(h, code) {
      if (hashes.has(h)) return false;
      hashes.set(h, { url_hash: h, short_code: code });
      return true;
    },
    async disable(c) { urls.get(c).status = 'disabled'; },
  };
}

function fakeCache() {
  const m = new Map();
  return {
    m,
    async get(c) { return m.get(c); },
    async set(c, r) { m.set(c, { long_url: r.long_url, expires_at: r.expires_at ?? null }); },
    async setNotFound(c) { m.set(c, NOT_FOUND); },
    async invalidate(c) { m.delete(c); },
  };
}

function makeShortener(repo = fakeRepo(), cache = fakeCache(), codes) {
  let i = 0;
  const idGenerator = { nextCode: async () => (codes ? codes[i++] : `c${String(i++).padStart(6, '0')}`) };
  return { repo, cache, s: new Shortener({ repo, cache, idGenerator, baseUrl: 'http://short.ly' }) };
}

test('shorten creates, caches and dedupes the same long url', async () => {
  const { s, cache } = makeShortener();
  const a = await s.shorten({ url: 'example.com/x' });
  assert.equal(a.created, true);
  assert.equal(a.short_url, `http://short.ly/${a.short_code}`);
  assert.ok(cache.m.has(a.short_code));
  const b = await s.shorten({ url: 'https://EXAMPLE.com/x/' });
  assert.equal(b.created, false);
  assert.equal(b.short_code, a.short_code);
});

test('custom alias conflicts return 409; generated code skips taken alias', async () => {
  const { s } = makeShortener(undefined, undefined, ['sale50', 'abc1234']);
  await s.shorten({ url: 'a.com', customAlias: 'sale50' });
  await assert.rejects(s.shorten({ url: 'b.com', customAlias: 'sale50' }), ConflictError);
  const r = await s.shorten({ url: 'c.com' }); // generator first yields "sale50" -> collision -> retry
  assert.equal(r.short_code, 'abc1234');
});

test('safety check disables malicious links asynchronously', async () => {
  const repo = fakeRepo();
  const cache = fakeCache();
  const { s } = makeShortener(repo, cache);
  s.safetyChecker = { check: async () => ({ safe: false, reason: 'malware' }) };
  const r = await s.shorten({ url: 'evil.example' });
  await new Promise((res) => setImmediate(res));
  await new Promise((res) => setImmediate(res));
  assert.equal(repo.urls.get(r.short_code).status, 'disabled');
  assert.equal(cache.m.has(r.short_code), false);
});

test('resolver: cache hit, db fallback, negative cache, expiry', async () => {
  const repo = fakeRepo();
  const cache = fakeCache();
  const now = Math.floor(Date.now() / 1000);
  await repo.insert({ short_code: 'ok', long_url: 'https://a.com/', status: 'active' });
  await repo.insert({ short_code: 'old', long_url: 'https://b.com/', status: 'active', expires_at: now - 1 });
  await repo.insert({ short_code: 'off', long_url: 'https://c.com/', status: 'disabled' });
  const r = new Resolver({ repo, cache });

  assert.equal((await r.resolve('ok')).long_url, 'https://a.com/');
  assert.ok(cache.m.has('ok')); // populated on miss
  assert.equal(await r.resolve('old'), null);
  assert.equal(await r.resolve('off'), null);
  assert.equal(await r.resolve('missing'), null);
  assert.equal(cache.m.get('missing'), NOT_FOUND);
});

test('resolver coalesces concurrent misses into one DB read', async () => {
  const repo = fakeRepo();
  await repo.insert({ short_code: 'hot', long_url: 'https://viral.com/', status: 'active' });
  let reads = 0;
  const get = repo.getByCode;
  repo.getByCode = async (c) => {
    reads++;
    await new Promise((res) => setTimeout(res, 5));
    return get(c);
  };
  const r = new Resolver({ repo, cache: fakeCache() });
  await Promise.all(Array.from({ length: 50 }, () => r.resolve('hot')));
  assert.equal(reads, 1);
});
