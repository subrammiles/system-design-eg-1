// Reliable Redis Stream consumer: consumer groups, XACK only after success, retries,
// XAUTOCLAIM for messages stuck with a dead consumer, dead-letter queue after N attempts,
// and dedupe by event_id (delivery is at-least-once).
import { sleep } from './clients.js';

export async function runConsumer({ redis, stream, group, consumer, deadLetter, handler, maxAttempts = 5, dedupeTtl = 7 * 86400 }) {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err;
  }

  const handleEntries = async (entries) => {
    for (const [id, fields] of entries) {
      if (!fields) continue;
      const event = JSON.parse(fields[fields.indexOf('data') + 1]);
      const doneKey = `done:${group}:${event.event_id}`;
      try {
        if (!(await redis.exists(doneKey))) {
          await handler(event);
          await redis.set(doneKey, '1', 'EX', dedupeTtl);
        }
        await redis.xack(stream, group, id);
      } catch (err) {
        const attempts = await redis.incr(`attempts:${group}:${id}`);
        await redis.expire(`attempts:${group}:${id}`, 86400);
        console.error(`[${group}] ${event.type} ${id} failed (attempt ${attempts}):`, err.message);
        if (attempts >= maxAttempts) {
          await redis.xadd(deadLetter, '*', 'data', JSON.stringify({ group, id, event, error: err.message }));
          await redis.xack(stream, group, id);
        }
      }
    }
  };

  let lastReclaim = 0;
  for (;;) {
    try {
      if (Date.now() - lastReclaim > 30_000) {
        lastReclaim = Date.now();
        const [, stale] = await redis.xautoclaim(stream, group, consumer, 60_000, '0-0', 'COUNT', 50);
        if (stale?.length) await handleEntries(stale);
      }
      const res = await redis.xreadgroup('GROUP', group, consumer, 'COUNT', 50, 'BLOCK', 2000, 'STREAMS', stream, '>');
      if (res) await handleEntries(res[0][1]);
    } catch (err) {
      console.error(`[${group}] loop error:`, err.message);
      await sleep(1000);
    }
  }
}
