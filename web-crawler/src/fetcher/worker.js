// Fetcher workers: lease URL (politeness enforced) -> robots.txt -> HTTP GET -> S3 -> fetch-results.
//
// Fetching is decoupled from parsing via the fetch-results stream, so the two scale
// independently (fetching is network-bound, parsing is CPU-bound).
import { config } from '../shared/config.js';
import { createDynamo, createRedis, createS3, sleep } from '../shared/clients.js';
import { Discovery } from '../shared/discovery.js';
import { Frontier } from '../shared/frontier.js';
import { PageStore } from '../shared/pageStore.js';
import { nextRecrawl } from '../shared/recrawl.js';
import { isAllowed } from '../shared/robots.js';
import { publish } from '../shared/streams.js';
import { Status, UrlRepository } from '../shared/urlRepository.js';
import { createHttpClient, TooLargeError } from './httpClient.js';
import { RobotsCache } from './robotsCache.js';

const { politeness, fetch: fetchCfg } = config;

const redis = createRedis();
const repo = new UrlRepository(createDynamo(), config.dynamo.tables);
const frontier = new Frontier(redis, { leaseTimeoutMs: politeness.leaseTimeoutMs });
const discovery = new Discovery({ redis, repo, frontier, scope: config.scope, dedupe: config.dedupe });
const pages = new PageStore(createS3(), config.s3.bucket);
const http = createHttpClient({ userAgent: config.userAgent, ...fetchCfg });
const robotsCache = new RobotsCache({
  redis, http, agent: config.robotsAgent, ttlSeconds: politeness.robotsTtlSeconds,
});

let running = true;
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => (running = false));

const clampDelay = (ms) => Math.min(politeness.maxDelayMs, Math.max(politeness.defaultDelayMs, ms));
const stat = (field) => redis.hincrby('stats:fetch', field, 1);

/** Crawl one leased URL. Returns the delay to apply to its host before the next request. */
async function crawl(item, host) {
  const hostKey = `host:${host}`;
  const robots = await robotsCache.get(new URL(item.u).origin, host);
  const delay = clampDelay(robots.crawlDelay != null ? robots.crawlDelay * 1000 : politeness.defaultDelayMs);
  await redis.hset(hostKey, 'delay', delay);

  if (!isAllowed(robots, item.u)) {
    await repo.update(item.h, { status: Status.BLOCKED_ROBOTS });
    await stat('robots_blocked');
    return delay;
  }

  const record = await repo.get(item.h);
  const conditional = {};
  if (record?.etag) conditional['if-none-match'] = record.etag;
  if (record?.last_modified) conditional['if-modified-since'] = record.last_modified;

  const fetchedAt = Date.now();
  let res;
  try {
    res = await http.get(item.u, conditional);
  } catch (err) {
    return handleError(item, err, delay);
  }

  const common = { http_status: res.status, last_crawled: fetchedAt };
  const s = res.status;

  if (s === 200 && res.body) {
    const key = await pages.put(item.h, fetchedAt, res.body, { url: item.u, status: s });
    await repo.update(item.h, { ...common, s3_key: key }, { fetch_count: 1 });
    await publish(redis, config.streams.fetchResults, {
      ...item,
      s3Key: key,
      fetchedAt,
      etag: res.headers.get('etag') ?? undefined,
      lastModified: res.headers.get('last-modified') ?? undefined,
    }, config.streams.maxLen);
    await redis.hset(hostKey, 'backoff', 0);
    await stat('fetched');
    return delay;
  }

  if (s === 200) {
    await repo.update(item.h, { ...common, status: Status.NOT_HTML, content_type: res.contentType });
    await stat('not_html');
    return delay;
  }

  if (s === 304) {
    // Unchanged since last crawl: cheap, and a signal to recrawl less often.
    const { interval, nextAt } = nextRecrawl({
      previousInterval: record?.recrawl_interval, changed: false, policy: config.recrawl,
    });
    await repo.update(item.h, {
      ...common, status: Status.NOT_MODIFIED, recrawl_interval: interval, next_crawl_at: nextAt,
    }, { fetch_count: 1 });
    await frontier.scheduleRecrawl(item, nextAt);
    await stat('not_modified');
    return delay;
  }

  if ([301, 302, 303, 307, 308].includes(s)) {
    const location = res.headers.get('location');
    const target = location
      ? await discovery.discover({ url: location, base: item.u, depth: item.d, priority: item.p, source: 'redirect' })
      : { result: 'no_location' };
    await repo.update(item.h, { ...common, status: Status.REDIRECTED, redirect_to: target.url ?? location });
    await stat('redirected');
    return delay;
  }

  if (s === 429 || s === 503) {
    // The site is asking us to slow down: back off the whole host, retry this URL later.
    const backoff = await redis.hincrby(hostKey, 'backoff', 1);
    const retryAfter = parseRetryAfter(res.headers.get('retry-after'));
    const hostDelay = Math.min(politeness.maxDelayMs, retryAfter ?? delay * 2 ** backoff);
    await scheduleRetry(item, common, hostDelay, `HTTP ${s}`);
    await stat('throttled');
    return hostDelay;
  }

  if (s === 404 || s === 410) {
    await repo.update(item.h, { ...common, status: Status.GONE });
    await stat('gone');
    return delay;
  }

  if (s >= 500) {
    await scheduleRetry(item, common, fetchCfg.retryBaseMs * 2 ** (item.r ?? 0), `HTTP ${s}`);
    await stat('server_error');
    return delay;
  }

  await repo.update(item.h, { ...common, status: Status.FAILED, error: `HTTP ${s}` });
  await stat('client_error');
  return delay;
}

async function scheduleRetry(item, fields, waitMs, error) {
  if ((item.r ?? 0) >= fetchCfg.maxRetries) {
    await repo.update(item.h, { ...fields, status: Status.FAILED, error: `${error} (gave up)` });
    await stat('gave_up');
    return;
  }
  await frontier.retryLater(item, Date.now() + waitMs);
  await repo.update(item.h, { ...fields, status: Status.RETRYING, error }, { retries: 1 });
}

async function handleError(item, err, delay) {
  const permanent = err instanceof TooLargeError || err.cause?.code === 'EPRIVATEADDR' ||
    err.code === 'EPRIVATEADDR' || err.cause?.code === 'ENOTFOUND';
  const message = err.cause?.code ?? err.name ?? 'error';
  if (permanent) {
    await repo.update(item.h, { status: Status.FAILED, error: `${message}: ${err.message}` });
    await stat('failed');
  } else {
    // Timeouts / resets: transient, exponential backoff.
    await scheduleRetry(item, {}, fetchCfg.retryBaseMs * 2 ** (item.r ?? 0), message);
    await stat('network_error');
  }
  return delay;
}

function parseRetryAfter(value) {
  if (!value) return null;
  const secs = Number(value);
  if (Number.isFinite(secs)) return secs * 1000;
  const at = Date.parse(value);
  return Number.isNaN(at) ? null : Math.max(0, at - Date.now());
}

async function loop(id) {
  while (running) {
    let leased;
    try {
      leased = await frontier.lease();
    } catch (err) {
      console.error(`[fetcher:${id}] lease failed:`, err.message);
      await sleep(1000);
      continue;
    }
    if (!leased) {
      await sleep(100 + Math.random() * 200);
      continue;
    }
    const { host, item, lease } = leased;
    let delay = politeness.defaultDelayMs;
    try {
      delay = await crawl(item, host);
    } catch (err) {
      // Unexpected failure (e.g. S3/DynamoDB down): retry with the same bounded policy.
      console.error(`[fetcher:${id}] ${item.u}:`, err.message);
      await scheduleRetry(item, {}, fetchCfg.retryBaseMs * 2 ** (item.r ?? 0), err.message)
        .catch((e) => console.error(`[fetcher:${id}] retry scheduling failed:`, e.message));
    } finally {
      await frontier.release(host, lease, delay).catch((err) => console.error('[fetcher] release failed', err.message));
    }
  }
}

console.log(`[fetcher] starting ${fetchCfg.concurrency} fetch loops`);
await Promise.all(Array.from({ length: fetchCfg.concurrency }, (_, i) => loop(i)));
await redis.quit();
