// Redirect Service - read path (~10x the traffic of the write path).
//
//   GET /:code  -> 302 Location: <long_url>
//
// 302 (not 301) so browsers hit us on every click and analytics stay accurate.
// Click events are published asynchronously; the redirect never waits on them.
import express from 'express';
import { config } from '../shared/config.js';
import { createDynamo, createRedis } from '../shared/clients.js';
import { LocalLru, UrlCache } from '../shared/cache.js';
import { ClickEventPublisher } from '../shared/clickEvents.js';
import { UrlRepository } from '../shared/urlRepository.js';
import { Resolver } from './resolver.js';

const docClient = createDynamo();
const redis = createRedis();

const resolver = new Resolver({
  repo: new UrlRepository({ docClient, tables: config.dynamo.tables }),
  cache: new UrlCache({
    redis,
    local: new LocalLru({ maxEntries: config.cache.localMaxEntries, ttlMs: config.cache.localTtlMs }),
    redisTtlSeconds: config.cache.redisTtlSeconds,
    negativeTtlSeconds: config.cache.negativeTtlSeconds,
  }),
});
const events = new ClickEventPublisher({
  redis,
  stream: config.analytics.stream,
  maxLen: config.analytics.streamMaxLen,
});

const app = express();
app.set('trust proxy', true);
app.disable('x-powered-by');

app.get('/health', (_req, res) => res.json({ status: 'ok' }));
app.get('/favicon.ico', (_req, res) => res.status(204).end());

const CODE_RE = /^[a-zA-Z0-9_-]{1,32}$/;

app.get('/:code', async (req, res, next) => {
  const { code } = req.params;
  if (!CODE_RE.test(code)) return res.status(404).send('Not found');

  try {
    const target = await resolver.resolve(code);
    if (!target) return res.status(404).send('Not found');

    // Fire-and-forget analytics event.
    events.publish({
      shortCode: code,
      ip: req.ip,
      userAgent: req.get('User-Agent') ?? '',
      referrer: req.get('Referer') ?? '',
      // Country from the CDN/edge (CloudFront, Cloudflare) - no GeoIP DB here.
      country: req.get('CloudFront-Viewer-Country') ?? req.get('CF-IPCountry') ?? 'unknown',
    });

    res.set('Cache-Control', 'private, max-age=0');
    res.redirect(302, target.long_url);
  } catch (err) {
    next(err);
  }
});

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[redirect-service]', err);
  res.status(500).send('Internal error');
});

app.listen(config.redirectServicePort, () => {
  console.log(`[redirect-service] listening on :${config.redirectServicePort}`);
});
