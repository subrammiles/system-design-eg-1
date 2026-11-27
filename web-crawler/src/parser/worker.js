// Parser workers: fetch-results stream -> parse -> content dedupe -> extract links -> discovery
//                                                                 -> schedule recrawl -> index-events
//
// Note on duplicates: a duplicate page's content is not indexed again, but its links ARE
// still followed - identical text can sit next to different outgoing links.
import { config } from '../shared/config.js';
import { createDynamo, createRedis, createS3 } from '../shared/clients.js';
import { Discovery } from '../shared/discovery.js';
import { Frontier } from '../shared/frontier.js';
import { PageStore } from '../shared/pageStore.js';
import { nextRecrawl } from '../shared/recrawl.js';
import { publish, runConsumer } from '../shared/streams.js';
import { Status, UrlRepository } from '../shared/urlRepository.js';
import { normalizeUrl } from '../shared/urls.js';
import { ContentDedupe } from './contentDedupe.js';
import { parseHtml } from './parseHtml.js';

const redis = createRedis();
const repo = new UrlRepository(createDynamo(), config.dynamo.tables);
const frontier = new Frontier(redis, { leaseTimeoutMs: config.politeness.leaseTimeoutMs });
const discovery = new Discovery({ redis, repo, frontier, scope: config.scope, dedupe: config.dedupe });
const pages = new PageStore(createS3(), config.s3.bucket);
const dedupe = new ContentDedupe({ redis, repo, maxDistance: config.dedupe.simhashMaxDistance });
const { streams } = config;

const stat = (field, n = 1) => redis.hincrby('stats:parse', field, n);

async function handle(msg) {
  const item = { h: msg.h, u: msg.u, d: msg.d, p: msg.p, host: msg.host };
  const html = await pages.get(msg.s3Key);
  const page = parseHtml(html, msg.u);
  const record = (await repo.get(item.h)) ?? {};

  const dup = await dedupe.check(item.h, page.text, record.simhash);
  const changed = record.content_hash !== dup.contentHash;

  // Link extraction (also for duplicates; skipped for meta nofollow).
  let queued = 0;
  if (!page.nofollow) {
    const childPriority = Math.max(0, item.p - 1);
    for (let i = 0; i < page.links.length; i += 25) {
      const results = await Promise.all(page.links.slice(i, i + 25).map((href) =>
        discovery.discover({ url: href, base: page.base, depth: item.d + 1, priority: childPriority, source: item.h })));
      queued += results.filter((r) => r.result === 'queued').length;
    }
  }

  // rel=canonical: the site tells us the "one true" URL for this content.
  let canonicalUrl;
  if (page.canonical) {
    canonicalUrl = normalizeUrl(page.canonical, msg.u) ?? undefined;
    if (canonicalUrl && canonicalUrl !== item.u) {
      await discovery.discover({ url: canonicalUrl, depth: item.d, priority: Math.min(10, item.p + 1), source: 'canonical' });
    }
  }

  if (page.needsRender) {
    await publish(redis, streams.renderQueue, { h: item.h, u: item.u, s3Key: msg.s3Key }, streams.maxLen);
    await stat('render_candidates');
  }

  const { interval, nextAt } = nextRecrawl({
    previousInterval: record.recrawl_interval, changed, policy: config.recrawl,
  });
  await frontier.scheduleRecrawl(item, nextAt);

  const isDuplicate = Boolean(dup.duplicateOf);
  await repo.update(item.h, {
    status: isDuplicate ? Status.DUPLICATE : Status.FETCHED,
    duplicate_of: dup.duplicateOf ?? undefined,
    duplicate_kind: dup.kind ?? undefined,
    title: page.title,
    content_hash: dup.contentHash,
    simhash: dup.simhash ?? undefined,
    outlinks: page.links.length,
    canonical_url: canonicalUrl,
    noindex: page.noindex,
    needs_render: page.needsRender,
    etag: msg.etag,
    last_modified: msg.lastModified,
    recrawl_interval: interval,
    next_crawl_at: nextAt,
  }, record.content_hash && changed ? { change_count: 1 } : {});

  if (!isDuplicate && !page.noindex && changed) {
    await publish(redis, streams.indexEvents, {
      h: item.h, u: item.u, title: page.title, s3Key: msg.s3Key, contentHash: dup.contentHash,
    }, streams.maxLen);
  }

  await stat(isDuplicate ? `duplicate_${dup.kind}` : 'unique');
  await stat('links_queued', queued);
}

let running = true;
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => (running = false));

console.log('[parser] consuming', streams.fetchResults);
await runConsumer({
  redis,
  stream: streams.fetchResults,
  group: streams.parserGroup,
  consumer: process.env.CONSUMER_NAME ?? `parser-${process.pid}-${Math.random().toString(36).slice(2, 6)}`,
  deadLetter: streams.deadLetter,
  handler: handle,
  isRunning: () => running,
});
await redis.quit();
