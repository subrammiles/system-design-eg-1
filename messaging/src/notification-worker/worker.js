// Notification Worker: push notifications for users with no online device.
//
// Push (FCM/APNs) only WAKES the device; it is never the source of truth. The app then
// connects and syncs the real messages. So the payload carries no message content (it's
// end-to-end encrypted anyway) and bursts are collapsed: at most one push per user per
// conversation per COLLAPSE_SECONDS.
import { config } from '../shared/config.js';
import { createKafka, createPg, createRedis, retry } from '../shared/clients.js';

const COLLAPSE_SECONDS = Number(process.env.PUSH_COLLAPSE_SECONDS ?? 10);
const pg = createPg();
const redis = createRedis();
const consumer = createKafka().consumer({ groupId: 'notification-workers' });

async function sendPush(device, payload) {
  // Stub: a real implementation calls FCM HTTP v1 / APNs with device.push_token.
  console.log(`[push] ${device.platform} device=${device.id} token=${device.push_token ?? '-'} ` +
    `"New message" conv=${payload.conversation_id} seq=${payload.conv_seq}`);
  await redis.hincrby('stats:push', 'sent', 1);
}

await retry('kafka', () => consumer.connect());
await consumer.subscribe({ topics: [config.kafka.topics.pushNotifications] });
await consumer.run({
  eachMessage: async ({ message }) => {
    const p = JSON.parse(message.value.toString());
    const first = await redis.set(`push:collapse:${p.user_id}:${p.conversation_id}`, '1', 'EX', COLLAPSE_SECONDS, 'NX');
    if (!first) {
      await redis.hincrby('stats:push', 'collapsed', 1);
      return;
    }
    const { rows } = await pg.query('SELECT id, platform, push_token FROM devices WHERE user_id = $1', [p.user_id]);
    for (const device of rows) await sendPush(device, p);
  },
});
console.log('[notification] consuming', config.kafka.topics.pushNotifications);
