// Transactional outbox -> Kafka.
//
// The Messaging Service writes events into `outbox` in the same transaction as the message.
// This worker publishes them to Kafka and deletes them only after Kafka acknowledged.
// A crash between publish and delete re-publishes the batch (at-least-once); consumers are
// idempotent (clients dedupe by message_id, receipts are monotonic watermarks).
//
// Ordering: exactly ONE publisher is active at a time (PostgreSQL advisory-lock leader
// election, standbys take over on failure), so events leave in commit order; Kafka keys by
// conversation_id keep per-chat order inside a partition.
import { config } from '../shared/config.js';
import { createKafka, createPg, retry, sleep } from '../shared/clients.js';

const LOCK_ID = 7_331_001;
const pg = createPg();
const producer = createKafka().producer({ idempotent: true, maxInFlightRequests: 1 });
await retry('kafka', () => producer.connect());

let running = true;
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => (running = false));

// Dedicated session holds the advisory lock for as long as this process lives.
const lockSession = await retry('postgres', () => pg.connect());
console.log('[outbox] waiting for leadership...');
while (running) {
  const { rows } = await lockSession.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_ID]);
  if (rows[0].ok) break;
  await sleep(2000);
}
console.log('[outbox] leader: publishing');

async function publishBatch() {
  const client = await pg.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      'SELECT id, topic, key, payload FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED', [config.outbox.batchSize]);
    if (!rows.length) {
      await client.query('COMMIT');
      return 0;
    }
    const byTopic = new Map();
    for (const r of rows) {
      if (!byTopic.has(r.topic)) byTopic.set(r.topic, []);
      byTopic.get(r.topic).push({ key: r.key, value: JSON.stringify(r.payload) });
    }
    await producer.sendBatch({
      acks: -1, // all in-sync replicas
      topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
    });
    await client.query('DELETE FROM outbox WHERE id = ANY($1)', [rows.map((r) => r.id)]);
    await client.query('COMMIT');
    return rows.length;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

let published = 0;
setInterval(() => {
  if (published) console.log(`[outbox] published ${published} events`);
  published = 0;
}, 10_000).unref();

while (running) {
  try {
    const n = await publishBatch();
    published += n;
    if (n < config.outbox.batchSize) await sleep(config.outbox.pollMs);
  } catch (err) {
    console.error('[outbox] publish failed, will retry:', err.message);
    await sleep(1000);
  }
}
await producer.disconnect();
lockSession.release();
await pg.end();
