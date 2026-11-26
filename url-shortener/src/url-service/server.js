// URL Service - write path.
//
//   POST /api/shorten            create a short URL (rate limited, idempotent)
//   GET  /api/urls/:code         link metadata + aggregated analytics
//   GET  /api/urls/:code/qr      QR code PNG for the short URL
//   GET  /health
import express from 'express';
import QRCode from 'qrcode';
import { config } from '../shared/config.js';
import { createDynamo, createRedis } from '../shared/clients.js';
import { UrlCache } from '../shared/cache.js';
import { RangeIdGenerator, dynamoRangeReserver } from '../shared/idGenerator.js';
import { rateLimiter } from '../shared/rateLimiter.js';
import { UrlRepository } from '../shared/urlRepository.js';
import { StatsRepository } from '../shared/statsRepository.js';
import { DomainBlocklistChecker, Shortener } from './shortener.js';

const docClient = createDynamo();
const redis = createRedis();
const { tables } = config.dynamo;

const repo = new UrlRepository({ docClient, tables });
const stats = new StatsRepository({ docClient, tables });
const cache = new UrlCache({
  redis,
  local: null, // the write path doesn't need an L1 cache
  redisTtlSeconds: config.cache.redisTtlSeconds,
  negativeTtlSeconds: config.cache.negativeTtlSeconds,
});
const idGenerator = new RangeIdGenerator({
  reserveRange: dynamoRangeReserver(docClient, tables.counters, config.ids.rangeSize),
  rangeSize: config.ids.rangeSize,
  codeLength: config.ids.codeLength,
});
const shortener = new Shortener({
  repo,
  idGenerator,
  cache,
  baseUrl: config.baseUrl,
  safetyChecker: new DomainBlocklistChecker(
    (process.env.BLOCKED_DOMAINS ?? 'malware.test,phishing.test').split(',').filter(Boolean),
  ),
});

const app = express();
app.set('trust proxy', true); // behind API gateway / load balancer
app.use(express.json({ limit: '16kb' }));

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

const limit = rateLimiter({
  redis,
  windowSeconds: config.rateLimit.windowSeconds,
  maxRequests: config.rateLimit.maxRequests,
  keyPrefix: 'rl:shorten',
});

// Idempotency-Key header: retries of the same request return the same result
// instead of creating duplicate links.
async function idempotency(req, res, next) {
  const idemKey = req.get('Idempotency-Key');
  if (!idemKey) return next();
  if (idemKey.length > 128) return res.status(400).json({ error: 'Idempotency-Key too long' });

  const key = `idem:${idemKey}`;
  const claimed = await redis.set(key, 'pending', 'EX', 60, 'NX');
  if (!claimed) {
    const stored = await redis.get(key);
    if (!stored || stored === 'pending') {
      return res.status(409).json({ error: 'request with this Idempotency-Key is in progress' });
    }
    const { status, body } = JSON.parse(stored);
    return res.status(status).set('Idempotent-Replay', 'true').json(body);
  }
  res.locals.idempotencyKey = key;
  next();
}

app.post('/api/shorten', limit, idempotency, async (req, res, next) => {
  const idemKey = res.locals.idempotencyKey;
  try {
    const { url, customAlias, expiresInSeconds } = req.body ?? {};
    const result = await shortener.shorten({
      url,
      customAlias,
      expiresInSeconds,
      userId: req.get('X-User-Id') ?? undefined, // set by the API gateway after auth
    });
    const status = result.created ? 201 : 200;
    if (idemKey) await redis.set(idemKey, JSON.stringify({ status, body: result }), 'EX', 24 * 3600);
    res.status(status).json(result);
  } catch (err) {
    if (idemKey) await redis.del(idemKey).catch(() => {});
    next(err);
  }
});

app.get('/api/urls/:code', async (req, res, next) => {
  try {
    const item = await repo.getByCode(req.params.code);
    if (!item) return res.status(404).json({ error: 'not found' });
    res.json({
      ...shortener.toResponse(item),
      created_at: item.created_at,
      status: item.status,
      clicks: item.clicks ?? 0,
      unique_visitors: await redis.pfcount(`uv:${item.short_code}`),
      analytics: await stats.getStats(item.short_code),
    });
  } catch (err) {
    next(err);
  }
});

app.get('/api/urls/:code/qr', async (req, res, next) => {
  try {
    const item = await repo.getByCode(req.params.code);
    if (!item) return res.status(404).json({ error: 'not found' });
    const png = await QRCode.toBuffer(shortener.shortUrl(item.short_code), { width: 300, margin: 1 });
    res.type('png').set('Cache-Control', 'public, max-age=86400').send(png);
  } catch (err) {
    next(err);
  }
});

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ error: 'invalid JSON body' });
  const status = err.status ?? 500;
  if (status >= 500) console.error('[url-service]', err);
  res.status(status).json({ error: status >= 500 ? 'internal error' : err.message });
});

app.listen(config.urlServicePort, () => {
  console.log(`[url-service] listening on :${config.urlServicePort}`);
});
