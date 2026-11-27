// Transactional outbox -> Redis Stream `group-events` (stand-in for Kafka).
// Events were committed atomically with the financial change; this publishes them and deletes
// them only after the stream accepted them (at-least-once; consumers dedupe by event_id).
// One active publisher (PostgreSQL advisory-lock leader election) keeps commit order.
import { config } from '../shared/config.js';
import { createPg, createRedis, retry, sleep, withTx } from '../shared/clients.js';

const LOCK_ID = 4_242_001;
const pool = createPg();
const redis = createRedis();

const lockSession = await retry('postgres', () => pool.connect());
console.log('[outbox] waiting for leadership...');
for (;;) {
  if ((await lockSession.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_ID])).rows[0].ok) break;
  await sleep(2000);
}
console.log('[outbox] leader: publishing');

for (;;) {
  try {
    const n = await withTx(pool, async (c) => {
      const { rows } = await c.query('SELECT id, payload FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED', [config.outbox.batchSize]);
      if (!rows.length) return 0;
      const pipe = redis.pipeline();
      for (const r of rows) pipe.xadd(config.events.stream, 'MAXLEN', '~', config.events.maxLen, '*', 'data', JSON.stringify(r.payload));
      const results = await pipe.exec();
      const failed = results.find(([err]) => err);
      if (failed) throw failed[0];
      await c.query('DELETE FROM outbox WHERE id = ANY($1)', [rows.map((r) => r.id)]);
      return rows.length;
    });
    if (n < config.outbox.batchSize) await sleep(config.outbox.pollMs);
  } catch (err) {
    console.error('[outbox] publish failed, retrying:', err.message);
    await sleep(1000);
  }
}
