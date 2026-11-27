// Janitor / recrawl scheduler. Runs on a timer:
//
//   recrawl  ZSET -> frontier   (pages whose next_crawl_at has arrived)
//   delayed  ZSET -> frontier   (retries after backoff)
//   inflight ZSET -> frontier   (leases of crashed fetchers that expired)
//
// Recrawls and new URLs share the frontier, but recrawls enter with their own priority,
// so freshness work doesn't starve discovery (or vice versa).
import { config } from '../shared/config.js';
import { createRedis, sleep } from '../shared/clients.js';
import { Frontier } from '../shared/frontier.js';

const redis = createRedis();
const frontier = new Frontier(redis, { leaseTimeoutMs: config.politeness.leaseTimeoutMs });
const RECRAWL_PRIORITY = Number(process.env.RECRAWL_PRIORITY ?? 6);

let running = true;
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => (running = false));

console.log('[janitor] started');
while (running) {
  try {
    const [recrawls, retries, reclaimed] = await Promise.all([
      frontier.promoteDue('recrawl', { priority: RECRAWL_PRIORITY }),
      frontier.promoteDue('delayed'),
      frontier.reclaimExpired(),
    ]);
    if (recrawls || retries || reclaimed) {
      await redis.multi()
        .hincrby('stats:janitor', 'recrawls', recrawls)
        .hincrby('stats:janitor', 'retries', retries)
        .hincrby('stats:janitor', 'reclaimed_leases', reclaimed)
        .exec();
      console.log(`[janitor] recrawl=${recrawls} retry=${retries} reclaimed=${reclaimed}`);
    }
  } catch (err) {
    console.error('[janitor]', err.message);
  }
  await sleep(1000);
}
await redis.quit();
