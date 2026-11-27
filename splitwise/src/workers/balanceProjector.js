// Balance projector: keeps read-optimized views fresh after every financial event.
//
//   user_summary:{user}  home screen "you are owed $X / you owe $Y" in O(1)
//   group:{id} pub/sub   real-time push to open apps (API relays it over Server-Sent Events)
//
// It RECOMPUTES the affected users' totals from PostgreSQL instead of applying +/- deltas
// to Redis, so redelivered or reordered events can't double-count.
import { config } from '../shared/config.js';
import { createPg, createRedis, retry } from '../shared/clients.js';
import { runConsumer } from '../shared/streams.js';
import { computeUserSummary } from '../api/service.js';

const pool = createPg();
await retry('postgres', () => pool.query('SELECT 1'));
const redis = createRedis();

console.log('[projector] consuming', config.events.stream);
await runConsumer({
  redis,
  stream: config.events.stream,
  group: 'balance-projector',
  consumer: `projector-${process.pid}`,
  deadLetter: config.events.deadLetter,
  handler: async (event) => {
    for (const userId of event.affected_users ?? []) {
      const summary = await computeUserSummary(pool, userId);
      await redis.set(`user_summary:${userId}`, JSON.stringify(summary), 'EX', 7 * 86400);
    }
    await redis.publish(`group:${event.group_id}`, JSON.stringify({
      type: event.type, actor: event.actor, balance_version: event.balance_version, at: event.at, data: event.data,
    }));
  },
});
