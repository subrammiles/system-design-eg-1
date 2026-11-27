// Integration test for the frontier Lua scripts against a real Redis.
// Opt-in, because it FLUSHES the selected database:
//   TEST_REDIS_URL=redis://localhost:6390/15 npm test
import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import Redis from 'ioredis';
import { Frontier, makeItem } from '../src/shared/frontier.js';

const url = process.env.TEST_REDIS_URL;
let redis;
let available = false;

before(async () => {
  if (!url) {
    console.log('# frontier tests skipped: set TEST_REDIS_URL to run them (flushes that DB)');
    return;
  }
  redis = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => null });
  redis.on('error', () => {});
  try {
    await redis.connect();
    await redis.flushdb();
    available = true;
  } catch {
    console.log(`# frontier tests skipped: no Redis at ${url}`);
  }
});

after(async () => {
  if (available) await redis.flushdb();
  redis?.disconnect();
});

const item = (host, path, p = 5) =>
  makeItem({ h: `${host}${path}`, u: `http://${host}${path}`, d: 0, p, host });

test('lease: one host at a time, priority order, politeness delay after release', async (t) => {
  if (!available) return t.skip('no redis');
  await redis.flushdb();
  const f = new Frontier(redis, { leaseTimeoutMs: 5000 });
  await f.add(item('a.com', '/low', 1));
  await f.add(item('a.com', '/high', 9));
  await f.add(item('b.com', '/x'));

  const first = await f.lease();
  const second = await f.lease();
  assert.notEqual(first.host, second.host, 'two leases must be different hosts');
  const leasedA = [first, second].find((l) => l.host === 'a.com');
  assert.equal(leasedA.item.u, 'http://a.com/high', 'highest priority first');
  assert.equal(await f.lease(), null, 'both hosts are leased -> nothing eligible');

  await f.release('a.com', leasedA.lease, 300);
  assert.equal(await f.lease(), null, 'a.com still inside its crawl delay');
  await new Promise((r) => setTimeout(r, 350));
  const third = await f.lease();
  assert.equal(third.item.u, 'http://a.com/low');
});

test('pause stops leasing; expired leases are reclaimed', async (t) => {
  if (!available) return t.skip('no redis');
  await redis.flushdb();
  const f = new Frontier(redis, { leaseTimeoutMs: 50 });
  await f.add(item('c.com', '/1'));
  await f.pause();
  assert.equal(await f.lease(), null);
  await f.resume();

  const l = await f.lease();
  assert.equal(l.item.u, 'http://c.com/1');
  // Worker "crashes" and never releases.
  await new Promise((r) => setTimeout(r, 80));
  assert.equal(await f.reclaimExpired(), 1);
  const again = await f.lease();
  assert.equal(again.item.u, 'http://c.com/1');
});

test('delayed retries and recrawls are promoted when due', async (t) => {
  if (!available) return t.skip('no redis');
  await redis.flushdb();
  const f = new Frontier(redis, { leaseTimeoutMs: 5000 });
  const obj = JSON.parse(item('d.com', '/r'));
  await f.retryLater(obj, Date.now() - 1);
  await f.scheduleRecrawl(JSON.parse(item('e.com', '/s')), Date.now() + 60_000);
  assert.equal(await f.promoteDue('delayed'), 1);
  assert.equal(await f.promoteDue('recrawl'), 0, 'recrawl not due yet');
  const l = await f.lease();
  assert.equal(l.item.u, 'http://d.com/r');
  assert.equal(l.item.r, 1, 'retry count incremented');
});
