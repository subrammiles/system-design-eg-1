// Central configuration. Every value can be overridden with an env var.
const env = (key, fallback) => process.env[key] ?? fallback;
const num = (key, fallback) => Number(env(key, fallback));

export const config = {
  // Public domain used to build short links: https://short.ly/aB3x9K
  baseUrl: env('BASE_URL', 'http://localhost:8080'),

  urlServicePort: num('URL_SERVICE_PORT', 3000),
  redirectServicePort: num('REDIRECT_SERVICE_PORT', 8080),

  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),

  dynamo: {
    endpoint: env('DYNAMO_ENDPOINT', 'http://localhost:8000'), // unset in real AWS
    region: env('AWS_REGION', 'us-east-1'),
    tables: {
      urls: env('TABLE_URLS', 'urls'),               // short_code -> long_url
      urlHash: env('TABLE_URL_HASH', 'url_hash'),    // hash(long_url) -> short_code (dedupe)
      counters: env('TABLE_COUNTERS', 'counters'),   // ID range allocator
      clickStats: env('TABLE_CLICK_STATS', 'click_stats'), // aggregated analytics
    },
  },

  ids: {
    // Each app server reserves a block of IDs and hands them out from memory.
    rangeSize: num('ID_RANGE_SIZE', 1000),
    // Fixed short-code length. 62^7 ~= 3.5 trillion codes.
    codeLength: num('CODE_LENGTH', 7),
  },

  cache: {
    redisTtlSeconds: num('CACHE_TTL_SECONDS', 24 * 60 * 60), // Redis TTL = 24h
    localTtlMs: num('LOCAL_CACHE_TTL_MS', 60 * 1000),        // in-process hot-key cache
    localMaxEntries: num('LOCAL_CACHE_MAX', 10_000),
    negativeTtlSeconds: num('NEGATIVE_CACHE_TTL_SECONDS', 60), // cache "not found"
  },

  rateLimit: {
    windowSeconds: num('RATE_LIMIT_WINDOW_SECONDS', 60),
    maxRequests: num('RATE_LIMIT_MAX', 100), // 100 requests / minute / IP
  },

  analytics: {
    stream: env('CLICK_STREAM', 'click-events'),
    group: env('CLICK_GROUP', 'analytics-processors'),
    consumer: env('CLICK_CONSUMER', `worker-${process.pid}`),
    streamMaxLen: num('CLICK_STREAM_MAXLEN', 1_000_000),
    dedupeTtlSeconds: num('CLICK_DEDUPE_TTL_SECONDS', 24 * 60 * 60),
  },
};
