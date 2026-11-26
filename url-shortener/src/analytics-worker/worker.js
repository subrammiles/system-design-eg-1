// Analytics processor.
//
//   click-events stream -> consumer group -> dedupe -> aggregate -> click_stats (DynamoDB)
//                                                               -> unique visitors (Redis HyperLogLog)
//
// Delivery is at-least-once: a consumer can crash after processing but before XACK,
// so every event carries an eventId and is deduplicated before counting.
// Messages left pending by a dead consumer are reclaimed with XAUTOCLAIM.
import { createHash } from 'node:crypto';
import { config } from '../shared/config.js';
import { createDynamo, createRedis } from '../shared/clients.js';
import { metricsFor } from '../shared/clickEvents.js';
import { StatsRepository } from '../shared/statsRepository.js';

const { stream, group, consumer, dedupeTtlSeconds } = config.analytics;
const BATCH = 100;
const RECLAIM_IDLE_MS = 60_000;

const redis = createRedis();
const stats = new StatsRepository({ docClient: createDynamo(), tables: config.dynamo.tables });

let running = true;
process.on('SIGTERM', () => (running = false));
process.on('SIGINT', () => (running = false));

async function ensureGroup() {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err;
  }
}

async function processEvent(event) {
  const dedupeKey = `click:seen:${event.eventId}`;
  const fresh = await redis.set(dedupeKey, '1', 'EX', dedupeTtlSeconds, 'NX');
  if (!fresh) return; // duplicate delivery - already counted

  try {
    await Promise.all([
      ...metricsFor(event).map((m) => stats.increment(event.shortCode, m)),
      stats.incrementUrlClicks(event.shortCode),
    ]);
    // Unique visitors: HyperLogLog of hashed IPs (~12KB per link, ~0.8% error, no raw IPs stored).
    const visitor = createHash('sha256').update(`${event.ip}|${event.userAgent}`).digest('hex').slice(0, 16);
    await redis.pfadd(`uv:${event.shortCode}`, visitor);
  } catch (err) {
    await redis.del(dedupeKey); // allow the retry to count it
    throw err;
  }
}

async function handle(entries) {
  for (const [id, fields] of entries) {
    try {
      const data = fields[fields.indexOf('data') + 1];
      await processEvent(JSON.parse(data));
      await redis.xack(stream, group, id);
    } catch (err) {
      // Not acked -> stays pending -> reclaimed and retried later.
      console.error(`[analytics] failed to process ${id}:`, err.message);
    }
  }
}

async function reclaimStale() {
  const [, entries] = await redis.xautoclaim(stream, group, consumer, RECLAIM_IDLE_MS, '0-0', 'COUNT', BATCH);
  if (entries?.length) {
    console.log(`[analytics] reclaimed ${entries.length} stale events`);
    await handle(entries.filter(([, fields]) => fields)); // deleted entries come back as null
  }
}

async function main() {
  await ensureGroup();
  console.log(`[analytics] consumer ${consumer} reading ${stream} as group ${group}`);
  let lastReclaim = 0;

  while (running) {
    try {
      if (Date.now() - lastReclaim > RECLAIM_IDLE_MS) {
        lastReclaim = Date.now();
        await reclaimStale();
      }
      const res = await redis.xreadgroup(
        'GROUP', group, consumer, 'COUNT', BATCH, 'BLOCK', 5000, 'STREAMS', stream, '>',
      );
      if (res) await handle(res[0][1]);
    } catch (err) {
      console.error('[analytics] loop error:', err.message);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  await redis.quit();
  console.log('[analytics] stopped');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
