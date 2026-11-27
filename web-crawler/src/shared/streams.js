// Reliable Redis Stream consumer (stand-in for Kafka / SQS).
//
// - Consumer groups: many parser processes share the work.
// - At-least-once: a message is XACKed only after the handler succeeds, so handlers must
//   be idempotent.
// - Messages stuck with a dead consumer are reclaimed with XAUTOCLAIM.
// - After maxAttempts failures a message goes to the dead-letter stream.
import { sleep } from './clients.js';

export async function publish(redis, stream, payload, maxLen) {
  return redis.xadd(stream, 'MAXLEN', '~', maxLen, '*', 'data', JSON.stringify(payload));
}

export async function runConsumer({
  redis, stream, group, consumer, handler, deadLetter, maxAttempts = 5,
  batch = 20, reclaimIdleMs = 60_000, isRunning = () => true,
}) {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err;
  }

  const handle = async (entries) => {
    for (const [id, fields] of entries) {
      if (!fields) continue; // trimmed entry
      const payload = JSON.parse(fields[fields.indexOf('data') + 1]);
      try {
        await handler(payload);
        await redis.xack(stream, group, id);
        await redis.del(`attempts:${stream}:${id}`);
      } catch (err) {
        const attempts = await redis.incr(`attempts:${stream}:${id}`);
        await redis.expire(`attempts:${stream}:${id}`, 86400);
        console.error(`[${stream}] ${id} failed (attempt ${attempts}):`, err.message);
        if (attempts >= maxAttempts) {
          await publish(redis, deadLetter, { stream, id, payload, error: err.message }, 100_000);
          await redis.xack(stream, group, id);
        }
      }
    }
  };

  let lastReclaim = 0;
  while (isRunning()) {
    try {
      if (Date.now() - lastReclaim > reclaimIdleMs / 2) {
        lastReclaim = Date.now();
        const [, stale] = await redis.xautoclaim(stream, group, consumer, reclaimIdleMs, '0-0', 'COUNT', batch);
        if (stale?.length) await handle(stale);
      }
      const res = await redis.xreadgroup('GROUP', group, consumer, 'COUNT', batch, 'BLOCK', 2000, 'STREAMS', stream, '>');
      if (res) await handle(res[0][1]);
    } catch (err) {
      console.error(`[${stream}] consumer loop error:`, err.message);
      await sleep(1000);
    }
  }
}
