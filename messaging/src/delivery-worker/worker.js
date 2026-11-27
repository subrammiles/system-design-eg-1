// Delivery Worker: routes persisted events to recipients' devices.
//
//   Kafka message-events ──► members ──► online devices (Redis registry) ──► gw:{id} pub/sub ──► gateway ──► socket
//                                     └► no online device ──► push-notifications topic (FCM/APNs)
//
// - The message is already durable (PostgreSQL + Cassandra) before we ever see it. Delivery
//   is best-effort fast path; anything missed is recovered by the device's sync on reconnect.
//   That's why pub/sub to gateways (fire-and-forget) is acceptable here.
// - Multi-device: every device of every member gets it, including the sender's OTHER devices.
// - Large groups (> LARGE_GROUP_THRESHOLD): the recipient list is split into chunks published
//   to `fanout-chunks`, so many workers share one big fan-out (the "celebrity" problem).
//   Storage is already fan-out-on-read: one copy per conversation plus per-member cursors,
//   never one inbox copy per member.
// - Kafka consumer group: add workers to scale; partitions keyed by conversation keep order.
import { config } from '../shared/config.js';
import { createKafka, createPg, createRedis, retry } from '../shared/clients.js';
import { Membership } from '../shared/membership.js';
import { gatewayChannel, Presence } from '../shared/presence.js';

const { topics } = config.kafka;
const { largeGroupThreshold, chunkSize } = config.delivery;

const redis = createRedis();
const presence = new Presence(redis, { ttlSeconds: config.gateway.presenceTtlSeconds });
const membership = new Membership({ pg: createPg(), redis });
const kafka = createKafka();
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'delivery-workers' });

const stats = { delivered: 0, pushed: 0, chunks: 0, receipts: 0 };
setInterval(() => {
  if (Object.values(stats).some(Boolean)) console.log('[delivery]', JSON.stringify(stats));
  for (const k of Object.keys(stats)) stats[k] = 0;
}, 10_000).unref();

/** Push `frame` to every online device of `userIds` (except `skipDevice`). Returns offline users. */
async function pushToUsers(userIds, frame, skipDevice) {
  const offline = [];
  const pipeline = redis.pipeline();
  let n = 0;
  for (const userId of userIds) {
    const devices = (await presence.onlineDevices(userId)).filter((d) => d.deviceId !== skipDevice);
    if (!devices.length) offline.push(userId);
    for (const d of devices) {
      pipeline.publish(gatewayChannel(d.gatewayId), JSON.stringify({ device_id: d.deviceId, frame }));
      n++;
    }
  }
  if (n) await pipeline.exec();
  return { offline, sent: n };
}

async function deliverMessage(message, userIds, allowPush) {
  const frame = { type: 'message', ...message };
  const { offline, sent } = await pushToUsers(userIds, frame, message.sender_device);
  stats.delivered += sent;
  const toPush = offline.filter((u) => u !== message.sender_id);
  if (allowPush && toPush.length) {
    await producer.send({
      topic: topics.pushNotifications,
      messages: toPush.map((userId) => ({
        key: userId,
        value: JSON.stringify({ user_id: userId, conversation_id: message.conversation_id, conv_seq: message.conv_seq, sender_id: message.sender_id }),
      })),
    });
    stats.pushed += toPush.length;
  }
}

async function onMessageEvent(event) {
  if (event.type === 'message_created') {
    const { message } = event;
    const members = await membership.members(message.conversation_id);
    if (members.length <= largeGroupThreshold) return deliverMessage(message, members, true);

    // Big group / channel: split the fan-out so the whole worker group shares it.
    const chunks = [];
    for (let i = 0; i < members.length; i += chunkSize) chunks.push(members.slice(i, i + chunkSize));
    await producer.send({
      topic: topics.fanoutChunks,
      messages: chunks.map((userIds, i) => ({ key: `${message.conversation_id}:${i}`, value: JSON.stringify({ message, user_ids: userIds }) })),
    });
    stats.chunks += chunks.length;
    return undefined;
  }
  if (event.type === 'receipt') {
    // Ticks for senders, and "read on phone" -> clear unread on laptop.
    const members = await membership.members(event.conversation_id);
    await pushToUsers(members, event, null);
    stats.receipts++;
  }
  return undefined;
}

await retry('kafka', () => producer.connect());
await retry('kafka', () => consumer.connect());
await consumer.subscribe({ topics: [topics.messageEvents, topics.fanoutChunks] });
await consumer.run({
  partitionsConsumedConcurrently: 3,
  eachMessage: async ({ topic, message }) => {
    const payload = JSON.parse(message.value.toString());
    if (topic === topics.fanoutChunks) {
      await deliverMessage(payload.message, payload.user_ids, false); // no push storms for huge groups
    } else {
      await onMessageEvent(payload);
    }
  },
});
console.log('[delivery] consuming', topics.messageEvents, topics.fanoutChunks);

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    process.exit(0);
  });
}
