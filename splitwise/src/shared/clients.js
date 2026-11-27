import Redis from 'ioredis';
import pg from 'pg';
import { config } from './config.js';

// Money is BIGINT (minor units) in PostgreSQL. node-postgres returns int8 as a string by
// default; our amounts are bounded far below 2^53, so parse them to safe JS integers.
pg.types.setTypeParser(20, (v) => {
  const n = Number(v);
  if (!Number.isSafeInteger(n)) throw new Error(`int8 out of safe range: ${v}`);
  return n;
});

// DATE as a plain 'YYYY-MM-DD' string (no timezone shifting).
pg.types.setTypeParser(1082, (v) => v);

export function createPg() {
  const pool = new pg.Pool({ connectionString: config.databaseUrl, max: 20 });
  pool.on('error', (err) => console.error('[pg]', err.message));
  return pool;
}

export function createRedis(name = 'redis') {
  const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 3 });
  redis.on('error', (err) => console.error(`[${name}]`, err.message));
  return redis;
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function retry(name, fn, attempts = 60, delayMs = 1000) {
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

export async function withTx(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
