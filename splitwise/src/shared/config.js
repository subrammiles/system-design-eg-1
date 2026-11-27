// Central configuration; every value can be overridden with an env var.
const env = (k, d) => process.env[k] ?? d;
const num = (k, d) => Number(env(k, d));

export const config = {
  port: num('PORT', 3000),
  authSecret: env('AUTH_SECRET', 'dev-only-secret-change-me'),
  databaseUrl: env('DATABASE_URL', 'postgres://splitwise:splitwise@localhost:5432/splitwise'),
  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),

  limits: {
    maxGroupMembers: num('MAX_GROUP_MEMBERS', 500),
    maxParticipants: num('MAX_PARTICIPANTS', 500),
    maxAmountMinor: num('MAX_AMOUNT_MINOR', 1e13), // well inside Number.MAX_SAFE_INTEGER
    optimalSimplifyMax: num('OPTIMAL_SIMPLIFY_MAX', 15), // exact min-transfers up to this many non-zero members
  },

  cache: {
    balancesTtlSeconds: num('BALANCES_CACHE_TTL', 3600),
  },

  events: {
    stream: env('EVENT_STREAM', 'group-events'),
    deadLetter: env('DEAD_LETTER_STREAM', 'group-events-dlq'),
    maxLen: num('EVENT_STREAM_MAXLEN', 1_000_000),
  },

  outbox: { pollMs: num('OUTBOX_POLL_MS', 100), batchSize: num('OUTBOX_BATCH', 200) },
  reconcileIntervalMs: num('RECONCILE_INTERVAL_MS', 60_000),
};
