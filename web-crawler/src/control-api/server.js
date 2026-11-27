// Control plane: add seeds, pause/resume, inspect crawl state.
//
//   POST /seeds                 {urls: [...], priority?}   -> normalized + deduped into the frontier
//   POST /control/pause | /control/resume
//   GET  /stats                 counters, frontier size, in-flight, recrawl backlog, stream lag
//   GET  /urls?url=...          crawl record for one URL
//   GET  /urls/:hash/content    stored raw HTML (from S3)
//   GET  /hosts/:host           politeness state, robots.txt, queue length
//   GET  /health
import express from 'express';
import { config } from '../shared/config.js';
import { createDynamo, createRedis, createS3 } from '../shared/clients.js';
import { Discovery } from '../shared/discovery.js';
import { Frontier } from '../shared/frontier.js';
import { PageStore } from '../shared/pageStore.js';
import { UrlRepository } from '../shared/urlRepository.js';
import { normalizeUrl, urlHash } from '../shared/urls.js';

const redis = createRedis();
const repo = new UrlRepository(createDynamo(), config.dynamo.tables);
const frontier = new Frontier(redis, { leaseTimeoutMs: config.politeness.leaseTimeoutMs });
const discovery = new Discovery({ redis, repo, frontier, scope: config.scope, dedupe: config.dedupe });
const pages = new PageStore(createS3(), config.s3.bucket);

const app = express();
app.use(express.json({ limit: '1mb' }));
const wrap = (fn) => (req, res, next) => fn(req, res).catch(next);

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/seeds', wrap(async (req, res) => {
  const { urls, priority = 8 } = req.body ?? {};
  if (!Array.isArray(urls) || !urls.length || urls.length > 10_000) {
    return res.status(400).json({ error: 'urls must be a non-empty array (max 10000)' });
  }
  const p = Math.max(0, Math.min(10, Number(priority) || 0));
  const results = [];
  for (const url of urls) {
    results.push({ input: url, ...(await discovery.discover({ url: String(url), depth: 0, priority: p, source: 'seed' })) });
  }
  res.status(202).json({ results });
}));

app.post('/control/pause', wrap(async (_req, res) => {
  await frontier.pause();
  res.json({ paused: true });
}));

app.post('/control/resume', wrap(async (_req, res) => {
  await frontier.resume();
  res.json({ paused: false });
}));

const toNumbers = (obj) => Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, Number(v)]));

app.get('/stats', wrap(async (_req, res) => {
  const r = await redis.multi()
    .hgetall('stats')
    .hgetall('stats:discovery')
    .hgetall('stats:fetch')
    .hgetall('stats:parse')
    .hgetall('stats:janitor')
    .zcard('hosts:due')
    .zcard('inflight')
    .zcard('delayed')
    .zcard('recrawl')
    .exists('crawl:paused')
    .exec();
  const [core, disc, fetch, parse, janitor, hostsDue, inflight, delayed, recrawl, paused] = r.map(([, v]) => v);
  let parserLag = null;
  try {
    const groups = await redis.xinfo('GROUPS', config.streams.fetchResults);
    const g = groups.find((x) => x[1] === config.streams.parserGroup);
    if (g) parserLag = { pending: g[g.indexOf('pending') + 1], lag: g[g.indexOf('lag') + 1] };
  } catch { /* stream not created yet */ }
  res.json({
    paused: paused === 1,
    frontier: { queued: Number(core.queued ?? 0), hostsWithWork: hostsDue, inflight, retryBacklog: delayed, recrawlScheduled: recrawl },
    discovery: toNumbers(disc),
    fetch: toNumbers(fetch),
    parse: toNumbers(parse),
    janitor: toNumbers(janitor),
    parserLag,
  });
}));

app.get('/urls', wrap(async (req, res) => {
  const normalized = normalizeUrl(String(req.query.url ?? ''));
  if (!normalized) return res.status(400).json({ error: 'valid ?url= required' });
  const record = await repo.get(urlHash(normalized));
  if (!record) return res.status(404).json({ error: 'not discovered', url: normalized });
  res.json(record);
}));

app.get('/urls/:hash/content', wrap(async (req, res) => {
  const record = await repo.get(req.params.hash);
  if (!record?.s3_key) return res.status(404).json({ error: 'no stored content' });
  res.type('text/plain').send(await pages.get(record.s3_key));
}));

app.get('/hosts/:host', wrap(async (req, res) => {
  const host = req.params.host.toLowerCase();
  const [state, robots, budget] = await Promise.all([
    frontier.hostState(host),
    redis.get(`robots:${host}`),
    redis.get(`budget:${host}`),
  ]);
  res.json({ host, ...state, robots: robots ? JSON.parse(robots) : null, urlsEnqueued: Number(budget ?? 0) });
}));

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[control-api]', err);
  res.status(500).json({ error: 'internal error' });
});

app.listen(config.controlApiPort, () => console.log(`[control-api] listening on :${config.controlApiPort}`));
