# URL Shortener (Bitly / TinyURL style)

Runnable implementation of the URL shortener system design: a long URL becomes a short
unique link (`short.ly/aB3x9K`), and requests to it are redirected quickly.

- **Stack:** Node.js 22 + Express, DynamoDB (primary store), Redis (cache, rate limit, event stream)
- **Scale target:** 100M creates/day (~1.2K rps), 1B redirects/day (~11.6K rps, ~58K rps peak). Reads outnumber writes about 10:1.

## Architecture

```
                       ┌──────────────────────── write path ───────────────────────┐
Client ─► API Gateway ─► URL Service (:3000) ─► ID range allocator ─► DynamoDB `urls`
                       │      │  rate limit / idempotency (Redis)       ▲
                       │      └─ write-through ─► Redis cache           │ miss
                       │                              ▲                 │
                       └─► Redirect Service (:8080) ──┴─ L1 local LRU ──┘
                                   │ 302 Location: long_url
                                   │ (async, fire-and-forget)
                                   ▼
                       Redis Stream `click-events` ─► Analytics Worker ─► DynamoDB `click_stats`
                                                                       └► Redis HyperLogLog (unique visitors)
```

| Design component | Code |
|---|---|
| URL Service (create flow) | [src/url-service/shortener.js](src/url-service/shortener.js), [server.js](src/url-service/server.js) |
| Redirect Service (redirect flow) | [src/redirect-service/resolver.js](src/redirect-service/resolver.js), [server.js](src/redirect-service/server.js) |
| Analytics Service + pipeline | [src/analytics-worker/worker.js](src/analytics-worker/worker.js), [src/shared/clickEvents.js](src/shared/clickEvents.js) |
| Base62 encoding | [src/shared/base62.js](src/shared/base62.js) |
| Distributed ID generation (range allocation) | [src/shared/idGenerator.js](src/shared/idGenerator.js) |
| Cache layer (Redis + hot-key local cache) | [src/shared/cache.js](src/shared/cache.js) |
| NoSQL data model | [src/shared/urlRepository.js](src/shared/urlRepository.js), [scripts/create-tables.js](scripts/create-tables.js) |
| URL normalization, alias validation, loop prevention | [src/shared/urlUtils.js](src/shared/urlUtils.js) |
| Rate limiting | [src/shared/rateLimiter.js](src/shared/rateLimiter.js) |

## Key design decisions

**ID generation: range allocation + Base62.** DynamoDB has no AUTO_INCREMENT. Each app
server atomically reserves a block of 1000 IDs (`ADD value :1000` on a `counters` item)
and hands them out from memory, so there's one DB call per 1000 links and no hot counter.
Each ID goes through a bijective affine permutation over `[0, 62^7)` before Base62
encoding. Codes stay unique, but `short.ly/1`, `short.ly/2` are not enumerable: adjacent
IDs give `UuB8Si0`, `wOjWKFR`. 7 characters give 62^7 ≈ 3.5 trillion codes.

**Custom aliases share the `urls` table.** All writes use
`ConditionExpression: attribute_not_exists(short_code)`. An alias can never overwrite a
generated code, and the reverse can't happen either. If a generated code hits an existing
alias, the service takes the next ID. `is_custom` marks the namespace.

**Deduplication.** URLs are normalized first: lowercase host, default port, fragment and
`utm_*`/`gclid` params removed, query sorted, trailing slash removed. `sha256(normalized)`
is then stored in `url_hash`, so the same URL returns the same short link. Aliases and
expiring links are not deduplicated.

**Read path.** L1 is an in-process LRU (60s), which absorbs viral hot keys before they
reach Redis. L2 is Redis (24h TTL, capped at the link's expiry). A miss goes to DynamoDB
`GetItem` (O(1) by partition key). Other protections:
- Unknown codes are negatively cached for 60s.
- Concurrent misses for one code are combined into a single DB read (stampede protection).
- Expiry is re-checked on every read, because DynamoDB TTL deletion is eventual.

**302, not 301.** A 301 is cached by browsers, so later clicks never reach the service and
analytics are lost. The service always returns 302.

**Analytics never block the redirect.** The redirect does an `XADD` to a Redis Stream
without waiting for it. The worker reads through a consumer group. Delivery is
at-least-once:
- Each event has an `eventId`, deduplicated with `SET NX`.
- Unacked events from crashed consumers are reclaimed with `XAUTOCLAIM`.
- Counters use atomic `ADD`.
- Unique visitors use a HyperLogLog of hashed IP+UA, so no raw IPs are stored.

In production, swap the stream for Kafka, Kinesis or SQS and the aggregates for a warehouse.

**Abuse prevention:**
- 100 req/min/IP fixed-window rate limit on create. It fails open if Redis is down.
- `Idempotency-Key` header support.
- Reserved and offensive aliases are rejected.
- Links that point back at the shortener are rejected (prevents redirect loops).
- An async safety check disables links to blocked domains and evicts them from cache. It's a stub for Google Safe Browsing.

## Data model (DynamoDB, on-demand)

| Table | Key | Attributes |
|---|---|---|
| `urls` | PK `short_code` | `long_url`, `created_at`, `expires_at` (TTL), `is_custom`, `status`, `clicks`, `user_id` |
| `url_hash` | PK `url_hash` | `short_code` |
| `counters` | PK `name` | `value` (ID allocator) |
| `click_stats` | PK `short_code`, SK `metric` | `count`. `metric` = `total`, `day#2026-11-26`, `device#mobile`, `browser#chrome`, `country#IN`, `referrer#t.co` |

DynamoDB partitions by `hash(short_code)`, which gives the even `hash % N` sharding the
design calls for, without hot alphabetical ranges.

## Run it

```bash
docker compose up -d --build        # redis, dynamodb-local, tables, 3 services
# port clash? REDIS_HOST_PORT=6390 DYNAMO_HOST_PORT=8001 docker compose up -d --build
```

```bash
# create
curl -XPOST localhost:3000/api/shorten -H 'content-type: application/json' \
  -d '{"url":"https://www.example.com/products/shoes/sneakers/white"}'
# -> {"short_code":"UuB8Si0","short_url":"http://localhost:8080/UuB8Si0",...,"created":true}

# custom alias with expiry, idempotent
curl -XPOST localhost:3000/api/shorten -H 'content-type: application/json' \
  -H 'Idempotency-Key: 7f1c' -d '{"url":"https://shop.com/sale","customAlias":"sale50","expiresInSeconds":86400}'

# redirect
curl -i localhost:8080/UuB8Si0            # 302 Location: https://www.example.com/...

# analytics + QR
curl localhost:3000/api/urls/UuB8Si0      # clicks, unique_visitors, by day/device/browser/country/referrer
curl localhost:3000/api/urls/UuB8Si0/qr > qr.png
```

### API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/api/shorten` | Body `{url, customAlias?, expiresInSeconds?}`. Returns 201 for a new link, 200 for a deduplicated one, 400 for invalid input, 409 if the alias is taken, 429 if rate limited |
| `GET` | `/:code` | Redirect service: 302, or 404 if the link is missing, expired or disabled |
| `GET` | `/api/urls/:code` | Metadata and aggregated analytics |
| `GET` | `/api/urls/:code/qr` | QR code PNG |

### Tests

```bash
npm install && npm test   # unit tests with in-memory fakes, no infra needed
```

To run the services outside Docker against local Redis/DynamoDB, run
`npm run create-tables`, then `npm run url-service`, `npm run redirect-service` and
`npm run analytics-worker`. Settings are in [src/shared/config.js](src/shared/config.js)
and can be overridden with env vars.

## Production notes / not implemented here

- A CDN in front of the redirect service, with a short edge TTL for the hottest links.
- Multi-region: DynamoDB Global Tables plus regional Redis. Range allocation stays safe if each region uses its own counter item or ID offset.
- A real GeoIP lookup. Country currently comes from the CDN header (`CloudFront-Viewer-Country` / `CF-IPCountry`).
- Auth at the API gateway (`X-User-Id` is trusted as already set), and private or password-protected links.
- Metrics (latency, error rate, cache hit ratio) via Prometheus/Grafana. Backups via DynamoDB PITR.
- After a disable, a redirect node's L1 cache can serve the old target for up to `LOCAL_CACHE_TTL_MS` (60s). A pub/sub invalidation would close that gap.
