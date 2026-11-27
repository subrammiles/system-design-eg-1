// Central configuration; every value can be overridden with an env var.
const env = (k, d) => process.env[k] ?? d;
const num = (k, d) => Number(env(k, d));

export const config = {
  authSecret: env('AUTH_SECRET', 'dev-only-secret-change-me'),

  gateway: {
    id: env('GATEWAY_ID', `gw-${process.pid}`),
    port: num('GATEWAY_PORT', 8081),
    heartbeatMs: num('HEARTBEAT_MS', 25_000),      // ping interval; dead after one missed pong
    presenceTtlSeconds: num('PRESENCE_TTL_SECONDS', 60),
    maxFrameBytes: num('MAX_FRAME_BYTES', 64 * 1024),
    sendRatePerSecond: num('SEND_RATE_PER_SECOND', 20), // per connection
  },

  messaging: {
    port: num('MESSAGING_PORT', 3001),
    url: env('MESSAGING_URL', 'http://localhost:3001'), // used by gateways (internal calls)
    maxBodyChars: num('MAX_BODY_CHARS', 4096),
    syncPageSize: num('SYNC_PAGE_SIZE', 500),
  },

  postgres: {
    connectionString: env('DATABASE_URL', 'postgres://chat:chat@localhost:5432/chat'),
    max: num('PG_POOL_MAX', 20),
  },

  cassandra: {
    contactPoints: env('CASSANDRA_CONTACT_POINTS', 'localhost').split(','),
    localDataCenter: env('CASSANDRA_DC', 'datacenter1'),
    keyspace: env('CASSANDRA_KEYSPACE', 'chat'),
    replicationFactor: num('CASSANDRA_RF', 1), // 3 in production
    bucketSize: num('BUCKET_SIZE', 1000),      // sequence-range buckets: seq 0-999 -> bucket 0
  },

  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),

  kafka: {
    brokers: env('KAFKA_BROKERS', 'localhost:9092').split(','),
    clientId: env('KAFKA_CLIENT_ID', 'chat'),
    topics: {
      messageEvents: 'message-events',   // message_created, receipt (key = conversation_id)
      fanoutChunks: 'fanout-chunks',     // large-group recipient batches
      pushNotifications: 'push-notifications',
    },
    partitions: num('KAFKA_PARTITIONS', 6),
  },

  delivery: {
    largeGroupThreshold: num('LARGE_GROUP_THRESHOLD', 200), // beyond this: chunked fan-out, no receipts
    chunkSize: num('FANOUT_CHUNK_SIZE', 200),
  },

  outbox: {
    pollMs: num('OUTBOX_POLL_MS', 50),
    batchSize: num('OUTBOX_BATCH', 200),
  },

  s3: {
    endpoint: env('S3_ENDPOINT', 'http://localhost:9090'),
    publicEndpoint: env('S3_PUBLIC_ENDPOINT', env('S3_ENDPOINT', 'http://localhost:9090')),
    bucket: env('S3_BUCKET', 'chat-media'),
    region: env('AWS_REGION', 'us-east-1'),
    maxUploadBytes: num('MAX_UPLOAD_BYTES', 100 * 1024 * 1024),
  },
};
