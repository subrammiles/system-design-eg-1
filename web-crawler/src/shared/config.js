// Central configuration; every value can be overridden with an env var.
const env = (k, d) => process.env[k] ?? d;
const num = (k, d) => Number(env(k, d));
const list = (k, d = '') => env(k, d).split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

export const config = {
  userAgent: env('USER_AGENT', 'SDCrawler/1.0 (+https://example.com/bot)'),
  robotsAgent: env('ROBOTS_AGENT', 'sdcrawler'), // token matched against robots.txt User-agent

  controlApiPort: num('CONTROL_API_PORT', 4000),
  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),

  dynamo: {
    endpoint: env('DYNAMO_ENDPOINT', 'http://localhost:8000'),
    region: env('AWS_REGION', 'us-east-1'),
    tables: {
      urls: env('TABLE_URLS', 'crawl_urls'),           // url_hash -> crawl state
      contentHashes: env('TABLE_CONTENT', 'content_hashes'), // sha256(content) -> first url_hash
    },
  },

  s3: {
    endpoint: env('S3_ENDPOINT', 'http://localhost:9090'),
    bucket: env('S3_BUCKET', 'crawled-pages'),
    accessKeyId: env('S3_ACCESS_KEY', 'minioadmin'),
    secretAccessKey: env('S3_SECRET_KEY', 'minioadmin'),
  },

  politeness: {
    defaultDelayMs: num('DEFAULT_HOST_DELAY_MS', 1000), // ~1 request / second / host
    maxDelayMs: num('MAX_HOST_DELAY_MS', 60_000),
    leaseTimeoutMs: num('LEASE_TIMEOUT_MS', 60_000),     // host locked while a fetch is in flight
    robotsTtlSeconds: num('ROBOTS_TTL_SECONDS', 24 * 3600),
  },

  fetch: {
    concurrency: num('FETCH_CONCURRENCY', 20), // parallel fetch loops per fetcher process
    timeoutMs: num('FETCH_TIMEOUT_MS', 10_000),
    maxBytes: num('FETCH_MAX_BYTES', 2 * 1024 * 1024),
    maxRetries: num('FETCH_MAX_RETRIES', 3),
    retryBaseMs: num('RETRY_BASE_MS', 30_000), // retry n waits retryBase * 2^n
    dnsTtlMs: num('DNS_CACHE_TTL_MS', 5 * 60_000),
    allowPrivateNetworks: env('ALLOW_PRIVATE_NETWORKS', 'false') === 'true', // SSRF guard
  },

  scope: {
    allowedHosts: list('ALLOWED_HOSTS'), // empty = whole web
    maxDepth: num('MAX_DEPTH', 10),
    hostBudget: num('HOST_CRAWL_BUDGET', 10_000),       // max URLs enqueued per host
    maxUrlsPerPattern: num('MAX_URLS_PER_PATTERN', 500), // spider-trap guard
  },

  dedupe: {
    bloomBits: num('BLOOM_BITS', 2 ** 27), // 16 MB bitmap
    bloomHashes: num('BLOOM_HASHES', 7),
    simhashMaxDistance: num('SIMHASH_MAX_DISTANCE', 3), // <=3 of 64 bits differ -> near duplicate
  },

  recrawl: {
    initialSeconds: num('RECRAWL_INITIAL_SECONDS', 24 * 3600),
    minSeconds: num('RECRAWL_MIN_SECONDS', 3600),
    maxSeconds: num('RECRAWL_MAX_SECONDS', 30 * 24 * 3600),
  },

  streams: {
    fetchResults: 'fetch-results',
    parserGroup: 'parsers',
    indexEvents: 'index-events',   // consumed by a (separate) indexing pipeline
    renderQueue: 'render-queue',   // JS-heavy pages for a headless-browser renderer
    deadLetter: 'dead-letter',
    maxLen: num('STREAM_MAXLEN', 100_000),
  },
};
