// Notification worker: tells group members what happened. Secondary to the money: if this is
// down, expenses are still recorded; events wait in the stream, failures retry, then go to the DLQ.
// "Delivery" here = an in-app inbox (Redis list) + a log line standing in for email/push.
import { config } from '../shared/config.js';
import { createPg, createRedis, retry } from '../shared/clients.js';
import { runConsumer } from '../shared/streams.js';
import { format } from '../domain/money.js';

const pool = createPg();
await retry('postgres', () => pool.query('SELECT 1'));
const redis = createRedis();
const nameCache = new Map();

async function name(userId) {
  if (!nameCache.has(userId)) {
    const { rows } = await pool.query('SELECT name FROM users WHERE user_id = $1', [userId]);
    nameCache.set(userId, rows[0]?.name ?? userId);
  }
  return nameCache.get(userId);
}

async function messageFor(event, recipient) {
  const who = event.actor === 'system' ? 'Payment provider' : await name(event.actor);
  const d = event.data ?? {};
  const money = (v) => format(v, event.currency);
  switch (event.type) {
    case 'EXPENSE_CREATED':
    case 'EXPENSE_UPDATED': {
      const mine = d.splits?.find((s) => s.user_id === recipient)?.amount ?? 0;
      const verb = event.type === 'EXPENSE_CREATED' ? 'added' : 'updated';
      const yourPart = d.paid_by === recipient
        ? `you paid ${money(d.total_amount)}`
        : `your share is ${money(mine)} (paid by ${await name(d.paid_by)})`;
      return `${who} ${verb} "${d.description}" ${money(d.total_amount)} - ${yourPart}`;
    }
    case 'EXPENSE_DELETED': return `${who} deleted "${d.description}" (${money(d.total_amount)})`;
    case 'EXPENSE_RESTORED': return `${who} restored "${d.description}"`;
    case 'SETTLEMENT_PENDING':
      if (d.method === 'UPI' || d.method === 'CARD') {
        return `${await name(d.from_user)} started a ${d.method} payment of ${money(d.amount)} to ${await name(d.to_user)}`;
      }
      return recipient === d.to_user
        ? `${await name(d.from_user)} says they paid you ${money(d.amount)} - please confirm`
        : `Payment of ${money(d.amount)} to ${await name(d.to_user)} is pending`;
    case 'SETTLEMENT_CONFIRMED': return `${await name(d.from_user)} paid ${await name(d.to_user)} ${money(d.amount)} ✔`;
    case 'SETTLEMENT_REJECTED': return `${await name(d.to_user)} rejected a payment of ${money(d.amount)}`;
    case 'SETTLEMENT_FAILED': return `Payment of ${money(d.amount)} failed`;
    case 'MEMBER_ADDED': return `${who} added ${await name(d.user_id)} to the group`;
    default: return null;
  }
}

console.log('[notifications] consuming', config.events.stream);
await runConsumer({
  redis,
  stream: config.events.stream,
  group: 'notifications',
  consumer: `notifier-${process.pid}`,
  deadLetter: config.events.deadLetter,
  handler: async (event) => {
    for (const userId of event.affected_users ?? []) {
      if (userId === event.actor) continue; // don't notify people about their own actions
      const text = await messageFor(event, userId);
      if (!text) continue;
      const item = JSON.stringify({ event_id: event.event_id, group_id: event.group_id, type: event.type, text, at: event.at });
      await redis.multi().lpush(`notifications:${userId}`, item).ltrim(`notifications:${userId}`, 0, 99).exec();
      console.log(`[notify] -> ${await name(userId)}: ${text}`);
    }
  },
});
