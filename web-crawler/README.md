# Distributed Web Crawler

Runnable implementation of the web crawler system design. The crawler:
- starts from seed URLs and fetches pages politely (robots.txt, per-host rate limits);
- extracts links and avoids duplicate URLs and duplicate content;
- stores pages and recrawls them at a rate that tracks how often they change.

**Stack:** Node.js 22, Redis (frontier, scheduler state, streams, Bloom filter), DynamoDB
(URL metadata, content fingerprints), and S3 (raw pages). Locally these run as Redis,
DynamoDB Local and s3mock, all in docker-compose.

**Scale target from the design:** 1B pages in 10 days, which is about 100M pages/day,
about 1,150 pages/s and about 126 MB/s, or roughly 110 TB of raw HTML. At about
1 request/s per host, that means about 1,150 different hosts being crawled in parallel at
any moment. The domain-aware scheduler below is what makes that possible.

## Architecture

```
 Seeds (control API)
      │
      ▼
 URL normalization ─► scope / trap filters ─► seen? (LRU → Bloom → DynamoDB) ─► budget + pattern cap
      ▲                                                                              │
      │                                                                              ▼
      │                         ┌──────────── Distributed URL Frontier (Redis) ─────────────┐
      │                         │ fq:{host}   per-host queues (priority, then FIFO)         │
      │                         │ hosts:due   host → next allowed fetch time                │
      │                         │ inflight    leases (crash recovery)                       │
      │                         └───────────────────────────────────────────────────────────┘
      │                                           │ lease(): atomic Lua = scheduler + dispatcher
      │                                           ▼
      │                       Fetchers ×N ── robots.txt cache, DNS cache, keep-alive, SSRF guard
      │                          │  │  └─ 3xx → discovery   429/503 → host backoff   5xx → retry w/ backoff
      │                          │  └────► S3 (gzip pages)
      │                          ▼
      │                 fetch-results stream (consumer group)
      │                          ▼
      └──── links ──── Parsers ×N ── content dedupe: SHA-256 → SimHash + LSH bands
                                 ├──► DynamoDB crawl_urls (status, hashes, recrawl interval)
                                 ├──► index-events stream  (for a separate indexer)
                                 ├──► render-queue stream  (JS-only pages → headless browser)
                                 └──► recrawl ZSET ──► Janitor ──► frontier
                                                      (also: due retries, expired leases)
```

| Design component | Code |
|---|---|
| Seed URL service / control plane | [src/control-api/server.js](src/control-api/server.js) |
| URL normalization, trap heuristics | [src/shared/urls.js](src/shared/urls.js) |
| URL dedupe (Bloom filter + DynamoDB) | [src/shared/discovery.js](src/shared/discovery.js) |
| URL frontier, scheduler, politeness | [src/shared/frontier.js](src/shared/frontier.js) |
| robots.txt | [src/shared/robots.js](src/shared/robots.js), [src/fetcher/robotsCache.js](src/fetcher/robotsCache.js) |
| Fetchers (HTTP, DNS cache, retries) | [src/fetcher/worker.js](src/fetcher/worker.js), [httpClient.js](src/fetcher/httpClient.js) |
| Fetch result queue | [src/shared/streams.js](src/shared/streams.js) |
| HTML parser, link extractor | [src/parser/parseHtml.js](src/parser/parseHtml.js), [worker.js](src/parser/worker.js) |
| Content dedupe (SHA-256, SimHash, LSH) | [src/parser/contentDedupe.js](src/parser/contentDedupe.js), [src/shared/simhash.js](src/shared/simhash.js) |
| Page storage (S3, gzip) | [src/shared/pageStore.js](src/shared/pageStore.js) |
| Recrawl policy + scheduler | [src/shared/recrawl.js](src/shared/recrawl.js), [src/janitor/worker.js](src/janitor/worker.js) |

## Key design decisions

### Politeness: per-host queues plus one "due" set, not one queue per website
One SQS queue per site doesn't scale to 100M sites, and several workers reading the same
site queue would still hammer that site. One global FIFO has no notion of hosts at all.
This design uses one Redis ZSET per host (`fq:{host}`) and a single `hosts:due` ZSET that
maps each host to the time it may next be fetched.

A fetcher calls `lease()`, a single atomic Lua script that:
1. picks a host whose time has come;
2. pops that host's highest-priority URL;
3. marks the host leased.

While a host is leased, no other worker can fetch from it. `release()` then sets
`next = now + delay`. Three consequences:
- Politeness holds across any number of workers.
- The delay counts from when the fetch finishes, so a slow site gets fewer concurrent requests, not more.
- If a worker dies mid-fetch, its lease expires and the janitor re-queues the URL.

The delay per host is `max(default, robots.txt Crawl-delay)`. A 429 or 503 grows it
(using `Retry-After`, otherwise exponential backoff).

### URL dedupe: the Bloom filter is an optimization, DynamoDB is the source of truth
URLs are normalized conservatively:
- lowercase host, default port and fragment removed;
- tracking params removed and query params sorted;
- path case is kept.

The `url_hash` (sha256) is then checked in three layers:
1. A per-process LRU.
2. A Redis Bloom filter. "Definitely new" skips the DB read. "Maybe seen" is confirmed with a cheap `GetItem`, so false positives never drop pages.
3. A DynamoDB conditional insert (`attribute_not_exists`), which is the authoritative check.

Most extracted links (nav bars, footers) are already seen, so most checks cost a Redis call
instead of a failed conditional write.

### Content dedupe: layered, never all-against-all
- **SHA-256** of the normalized text gives an exact-duplicate check: a conditional put on `content_hashes`, O(1).
- **SimHash** (64-bit, word 3-shingles) gives a fingerprint where near duplicates differ in only a few bits.
- **LSH bands** find the candidates. The 64 bits are split into **k+1 bands**, so by the pigeonhole principle any page within Hamming distance k shares at least one band exactly. The parser compares only against documents sharing a band, never against every document.
- **Race safety:** a page indexes itself with an increasing sequence number *before* searching, and only an earlier entry can count as "the original". Two near-identical pages parsed at the same moment therefore yield exactly one unique page. This is covered by a test.
- **Duplicates still have their links extracted.** Identical text can sit next to different outgoing links.

### Spider traps and crawl budget
These are rejected at discovery time:
- depth beyond `MAX_DEPTH`;
- paths more than 12 segments deep;
- a path segment repeated 3 or more times (`/a/b/a/b/a`);
- more than 6 query params;
- non-HTML extensions, and side-effect paths like `/logout` or `/cart`.

There's also a **URL-pattern cap**: digits become `{n}` and query values become `*`, so
`/calendar/2026/11` becomes `/calendar/{n}/{n}`, and each pattern has a per-host limit. A
**per-host budget** caps how many URLs are enqueued for any one host.

### Freshness: adaptive recrawl
- A page that changed has its interval halved; an unchanged one has it doubled, bounded to [1h, 30d] by default, with ±10% jitter.
- Recrawls send `If-None-Match` / `If-Modified-Since`, so an unchanged page costs a 304 with no body.
- Recrawls enter the same frontier as new URLs, with their own priority.

### Failure handling
| Failure | Handling |
|---|---|
| Fetcher crash | Lease expires and the janitor re-queues the URL (`inflight` ZSET) |
| Timeout, reset, 5xx | Retry with exponential backoff, max 3 retries, then `FAILED` |
| 429 / 503 | Respect `Retry-After`, slow down the whole host, retry the URL later |
| 404 / 410 | Marked `GONE`, no retry |
| 3xx | Target goes back through discovery: dedupe, robots, politeness |
| robots.txt 5xx or unreachable | Treated as disallow-all, re-checked in 5 min (RFC 9309) |
| Parser crash | Message stays un-acked, is reclaimed with `XAUTOCLAIM`, and goes to the dead-letter queue after 5 attempts |
| Duplicate message delivery | Every step is idempotent: conditional writes, set-adds, ZADDs |

### Security
- **SSRF guard:** fetchers refuse private, loopback and link-local IPs (for example cloud metadata at 169.254.169.254). The check runs at DNS-resolution time.
- **Limits:** 10 s timeout and a 2 MB body cap on every fetch.
- **No JavaScript is executed.** JS-only pages are flagged and sent to `render-queue` for a sandboxed headless browser.

## Data model

| Store | Key | Contents |
|---|---|---|
| DynamoDB `crawl_urls` | `url_hash` | `url`, `host`, `depth`, `status`, `priority`, `http_status`, `content_hash`, `simhash`, `s3_key`, `title`, `etag`, `last_modified`, `recrawl_interval`, `next_crawl_at`, `change_count`, `duplicate_of`, `canonical_url`, `redirect_to`, `error` |
| DynamoDB `content_hashes` | `content_hash` | `url_hash` of the first page with that exact content |
| S3 | `pages/ab/cd/<url_hash>/<fetched_at>.html.gz` | gzip HTML snapshot (in production, batch into WARC files) |
| Redis | see [frontier.js](src/shared/frontier.js) | frontier, host state, leases, retries, recrawl schedule, Bloom filter, LSH bands, counters |

Statuses: `PENDING`, `FETCHED`, `NOT_MODIFIED`, `DUPLICATE`, `REDIRECTED`, `BLOCKED_ROBOTS`, `NOT_HTML`, `GONE`, `RETRYING`, `FAILED`.

## Run it

```bash
docker compose up -d --build
# Port clash? REDIS_HOST_PORT=6391 DYNAMO_HOST_PORT=8001 S3_HOST_PORT=9091 docker compose up -d --build

curl -XPOST localhost:4000/seeds -H 'content-type: application/json' \
  -d '{"urls":["http://site-a:5000/","http://site-b:5000/"]}'

curl localhost:4000/stats                                     # counters, frontier, lag
curl 'localhost:4000/urls?url=http://site-a:5000/mirror/about' # one URL's crawl record
curl localhost:4000/hosts/site-a:5000                          # politeness state + robots.txt
curl -XPOST localhost:4000/control/pause                       # and /control/resume
docker compose up -d --scale fetcher=5 --scale parser=3        # scale fetch and parse separately
```

The compose file crawls only a bundled **test site**, [test-site/server.js](test-site/server.js).
It serves two hosts, `site-a` and `site-b`, with pages built for each concern in this
design. To crawl the real web, remove `ALLOWED_HOSTS` and `ALLOW_PRIVATE_NETWORKS` and
restore the default delays. Also set `USER_AGENT` to include a real contact URL.

### What a demo crawl shows (verified)

| Test page | Result |
|---|---|
| `/about?ref=twitter`, `/about/?utm_source=mail#team` | Normalized to `/about` and fetched once |
| `/mirror/about` | `DUPLICATE` (exact) |
| `/canonical?size=l&color=blue` → `rel=canonical` → `/canonical` | Canonical discovered, `DUPLICATE` (near) |
| `/private/secret` vs `/private/public-note` | `BLOCKED_ROBOTS` vs fetched (longest-match `Allow`) |
| robots.txt | Fetched once per host. Requests to site-a were at least 217 ms apart (Crawl-delay 0.2 s) |
| `/calendar/2026/1` → next month, forever | Stopped by the URL-pattern cap (`trap_pattern`) |
| `/flaky` (500, 500, 200) | Fetched after 2 retries |
| `/slow-down` (429, `Retry-After: 2`) | Host backed off, then fetched |
| `/old-page` (301) | `REDIRECTED`. `/new-page` was discovered and fetched |
| `/missing` | `GONE` |
| `/spa` (empty `<div id="app">`) | `needs_render`, sent to `render-queue` |
| `/nofollow-page` | `noindex`, and its links were not followed. `rel=nofollow` links were skipped too |
| `/logo.png`, `/logout`, `mailto:` | Filtered out |
| `/about` recrawl | Sent with `If-None-Match`, got 304 `NOT_MODIFIED`, interval doubled from 60 s to 120 s |

### Tests

```bash
npm install && npm test                            # unit tests, no infra needed
TEST_REDIS_URL=redis://localhost:6391/15 npm test  # + frontier Lua & dedupe race tests (FLUSHES that DB)
```

## Tuning notes and known limits

- **Near-duplicate threshold.** The default is `SIMHASH_MAX_DISTANCE=3` (4 bands of 16 bits), the standard 64-bit setting. On ~200-word pages, a one-word edit gives a median distance of 3 but can reach 6–8, while unrelated pages measured 19 or more. The test site's `/products/shoes-a` vs `shoes-b` pair (one word changed) is at distance 6, so it is *not* flagged at k=3. Raising k to 6 catches it, but uses 7 narrower bands, and therefore much larger candidate sets at web scale. Longer pages are more stable.
- **Single Redis primary.** The Lua scripts build keys dynamically. To scale out, shard hosts across N Redis instances by `hash(host)`. Each fetcher group leases from its own shards, and per-host politeness stays local to one shard. Kafka or SQS could replace the streams.
- **Not built here:**
  - the indexer that consumes `index-events` (OpenSearch / inverted index);
  - the headless renderer for `render-queue`;
  - WARC batching;
  - geo-distributed fetchers;
  - priority from link-graph signals (PageRank);
  - metrics export (Prometheus).
- **Rough cost from the design doc, for 1B pages/month:** about $20K–40K. Storage and index dominate; compression cuts HTML size by 60–80%.
