// Infrastructure clients.
//   PostgreSQL - users, devices, conversations, membership, sequence allocation, idempotency, outbox
//   Cassandra  - message history
//   Redis      - presence, device->gateway registry, gateway pub/sub, caches, rate limits
//   Kafka      - durable events (message_created, receipts, fan-out chunks, push)
//   S3         - media
import cassandra from 'cassandra-driver';
import Redis from 'ioredis';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import { config } from './config.js';

export function createPg() {
  const pool = new pg.Pool({ connectionString: config.postgres.connectionString, max: config.postgres.max });
  pool.on('error', (err) => console.error('[pg]', err.message));
  return pool;
}

export function createCassandra({ withKeyspace = true } = {}) {
  return new cassandra.Client({
    contactPoints: config.cassandra.contactPoints,
    localDataCenter: config.cassandra.localDataCenter,
    ...(withKeyspace && { keyspace: config.cassandra.keyspace }),
    queryOptions: { consistency: cassandra.types.consistencies.localQuorum, prepare: true },
  });
}

export function createRedis(name = 'redis') {
  const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 3 });
  redis.on('error', (err) => console.error(`[${name}]`, err.message));
  return redis;
}

export const createKafka = () =>
  new Kafka({ clientId: config.kafka.clientId, brokers: config.kafka.brokers, logLevel: logLevel.WARN });

export function createS3(endpoint = config.s3.endpoint) {
  return new S3Client({
    region: config.s3.region,
    endpoint,
    forcePathStyle: true,
    credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
  });
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function retry(name, fn, attempts = 60, delayMs = 2000) {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= attempts) throw err;
      console.log(`[${name}] not ready (${err.message.slice(0, 80)}), retrying...`);
      await sleep(delayMs);
    }
  }
}
