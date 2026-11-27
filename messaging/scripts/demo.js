// End-to-end scenario against the running stack (docker compose up). Every step asserts;
// the process exits non-zero on the first failure.
//
//   MESSAGING_URL=http://localhost:3001 GW1=ws://localhost:8081 GW2=ws://localhost:8082 node scripts/demo.js
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createApi } from '../client/api.js';
import { ChatClient } from '../client/chatClient.js';

const API = process.env.MESSAGING_URL ?? 'http://localhost:3001';
const GW1 = process.env.GW1 ?? 'ws://localhost:8081';
const GW2 = process.env.GW2 ?? 'ws://localhost:8082';
const api = createApi(API);
const clients = [];

const step = (title) => console.log(`\n\x1b[1m▶ ${title}\x1b[0m`);
const ok = (text) => console.log(`  \x1b[32m✔\x1b[0m ${text}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(what, fn, timeoutMs = 8000) {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${what}`);
    await sleep(50);
  }
}

async function device(user, platform, gatewayUrl, { connect = true } = {}) {
  const d = await api.registerDevice(user.user_id, platform, `${platform}-push-token-${user.name}`);
  const c = new ChatClient({ url: gatewayUrl, token: d.token, userId: user.user_id, deviceId: d.device_id, ackTimeoutMs: 1500 });
  c.label = `${user.name}/${platform}`;
  c.token = d.token;
  clients.push(c);
  if (connect) await c.connect();
  return c;
}

async function refreshConversations(...cs) {
  for (const c of cs) c.setConversations(await api.conversations(c.token));
}

const bodies = (c, conv) => c.store.timeline(conv).map((m) => m.body);
const sentBy = (c, conv) => c.store.timeline(conv).filter((m) => m.sender_id === c.userId);

async function main() {
  step('Setup: users and devices on two different gateways');
  const [alice, bob, carol, dave] = await Promise.all(['Alice', 'Bob', 'Carol', 'Dave'].map((n) => api.createUser(n)));
  const alicePhone = await device(alice, 'android', GW1);
  const aliceLaptop = await device(alice, 'web', GW2);
  const bobPhone = await device(bob, 'ios', GW2);
  ok(`Alice: phone on gw-1, laptop on gw-2; Bob: phone on gw-2. Carol and Dave: offline`);

  step('1:1 chat: store-then-deliver, conversation order, multi-device');
  const dm = await api.createDirect(alicePhone.token, bob.user_id);
  await refreshConversations(alicePhone, aliceLaptop, bobPhone);
  for (const t of ['Hello', 'How are you?', 'Good morning']) alicePhone.send(dm.conversation_id, t);
  await waitFor('Bob receives 3 messages', () => bobPhone.store.timeline(dm.conversation_id).length === 3);
  assert.deepEqual(bodies(bobPhone, dm.conversation_id), ['Hello', 'How are you?', 'Good morning']);
  assert.deepEqual(bobPhone.store.timeline(dm.conversation_id).map((m) => m.conv_seq), [1, 2, 3]);
  ok('Bob (other gateway) got all 3 in order, conv_seq 1,2,3');
  await waitFor("Alice's laptop gets her own messages", () => aliceLaptop.store.timeline(dm.conversation_id).length === 3);
  ok("Alice's laptop received the copies sent from her phone (sender's other devices)");

  step('Receipts: ✔ SENT -> ✔✔ DELIVERED -> blue ✔✔ READ');
  const last = () => sentBy(alicePhone, dm.conversation_id).at(-1);
  await waitFor('DELIVERED', () => alicePhone.statusOf(last()) === 'DELIVERED');
  ok(`after Bob's device acked: ${alicePhone.statusOf(last())}`);
  bobPhone.markRead(dm.conversation_id);
  await waitFor('READ', () => alicePhone.statusOf(last()) === 'READ');
  ok(`after Bob opened the chat: ${alicePhone.statusOf(last())} (one watermark covers all 3 messages)`);
  await waitFor('laptop sees READ too', () => aliceLaptop.statusOf(last()) === 'READ');
  ok("Alice's laptop shows READ as well");

  step('Idempotency: a retried send with the same message_id is not duplicated');
  const original = last();
  const dupAck = new Promise((res) => alicePhone.once('ack_raw', res));
  const onRaw = (f) => f.type === 'send_ack' && f.message_id === original.message_id && alicePhone.emit('ack_raw', f);
  alicePhone.ws.on('message', (d) => onRaw(JSON.parse(d)));
  alicePhone.frame({ type: 'send', message_id: original.message_id, conversation_id: dm.conversation_id, client_seq: original.client_seq, body: original.body });
  const ack = await dupAck;
  assert.equal(ack.duplicate, true);
  assert.equal(ack.conv_seq, original.conv_seq);
  await sleep(500);
  assert.equal(bobPhone.store.timeline(dm.conversation_id).length, 3);
  ok(`server answered duplicate=true with the original conv_seq ${ack.conv_seq}; Bob still has 3 messages`);

  step("Per-device order: the server rejects a client_seq gap ('Msg1 lost, Msg2 shown' cannot happen)");
  const gapErr = new Promise((res) => alicePhone.once('send_error_raw', res));
  alicePhone.ws.on('message', (d) => { const f = JSON.parse(d); if (f.type === 'send_error') alicePhone.emit('send_error_raw', f); });
  alicePhone.frame({ type: 'send', message_id: randomUUID(), conversation_id: dm.conversation_id, client_seq: alicePhone.store.clientSeq + 5, body: 'skipped ahead' });
  const ge = await gapErr;
  assert.equal(ge.code, 'CLIENT_SEQ_GAP');
  assert.equal(ge.expected, alicePhone.store.clientSeq + 1);
  ok(`rejected with CLIENT_SEQ_GAP, expected client_seq ${ge.expected}`);

  step('Typing indicator (ephemeral, not stored)');
  const typing = new Promise((res) => bobPhone.once('typing', res));
  alicePhone.typing(dm.conversation_id);
  assert.equal((await typing).user_id, alice.user_id);
  ok('Bob saw "Alice is typing…"');

  step('Offline receiver: store, push notification, deliver on reconnect via sync');
  const group = await api.createGroup(alicePhone.token, 'Weekend trip', [bob.user_id, carol.user_id]);
  await refreshConversations(alicePhone, aliceLaptop, bobPhone);
  alicePhone.send(group.conversation_id, 'Trip on Saturday?');
  await waitFor('Bob gets group msg', () => bodies(bobPhone, group.conversation_id).length === 1);
  bobPhone.send(group.conversation_id, "I'm in!");
  await waitFor('Alice gets Bob reply', () => bodies(alicePhone, group.conversation_id).length === 2);
  const carolPresence = await api.presence(alicePhone.token, carol.user_id);
  assert.equal(carolPresence.online, false);
  ok('Carol is offline (presence) - messages are stored, push notification queued (see notification-worker logs)');
  const firstGroupMsg = sentBy(alicePhone, group.conversation_id)[0];
  assert.equal(alicePhone.statusOf(firstGroupMsg), 'SENT', 'not DELIVERED while Carol is offline');
  ok("Alice's group message stays ✔ SENT: Carol hasn't received it");

  const carolPhone = await device(carol, 'android', GW1);
  await refreshConversations(carolPhone);
  await waitFor('Carol synced', () => bodies(carolPhone, group.conversation_id).length === 2);
  assert.deepEqual(bodies(carolPhone, group.conversation_id), ['Trip on Saturday?', "I'm in!"]);
  ok('Carol came online and synced both messages, in order');
  await waitFor('group DELIVERED', () => alicePhone.statusOf(firstGroupMsg) === 'DELIVERED');
  ok("Alice's group message is now ✔✔ DELIVERED (all members have it)");

  step('Network drop: Bob loses connection, misses messages, auto-reconnects and syncs');
  bobPhone.dropConnection();
  await sleep(200);
  alicePhone.send(group.conversation_id, 'Bring snacks');
  carolPhone.send(group.conversation_id, 'And water');
  await waitFor('Bob reconnected', () => bobPhone.ws.readyState === 1, 10_000);
  await waitFor('Bob caught up', () => bodies(bobPhone, group.conversation_id).length === 4, 10_000);
  ok(`Bob's timeline: ${JSON.stringify(bodies(bobPhone, group.conversation_id))}`);

  step('Offline sender: messages queued locally while disconnected are sent on reconnect');
  carolPhone.autoReconnect = false;
  carolPhone.dropConnection();
  await sleep(100);
  carolPhone.send(group.conversation_id, 'Typed on the subway 1');
  carolPhone.send(group.conversation_id, 'Typed on the subway 2');
  assert.equal(carolPhone.store.pending.size, 2);
  ok('2 messages PENDING in Carol\'s local store');
  carolPhone.autoReconnect = true;
  await carolPhone.connect();
  await waitFor('pending flushed', () => carolPhone.store.pending.size === 0);
  await waitFor('Alice has them', () => bodies(alicePhone, group.conversation_id).length === 6);
  assert.deepEqual(bodies(alicePhone, group.conversation_id).slice(-2), ['Typed on the subway 1', 'Typed on the subway 2']);
  ok('flushed in client_seq order after reconnect');

  step('Ordering under concurrency: 3 senders x 30 messages into the same group');
  const N = 30;
  for (let i = 1; i <= N; i++) {
    alicePhone.send(group.conversation_id, `A${i}`);
    bobPhone.send(group.conversation_id, `B${i}`);
    carolPhone.send(group.conversation_id, `C${i}`);
  }
  const total = 6 + 3 * N;
  const everyone = [alicePhone, aliceLaptop, bobPhone, carolPhone];
  await waitFor('all timelines complete', () => everyone.every((c) => c.store.timeline(group.conversation_id).length === total), 20_000);
  const reference = bodies(alicePhone, group.conversation_id);
  for (const c of everyone) assert.deepEqual(bodies(c, group.conversation_id), reference, `${c.label} order differs`);
  for (const prefix of ['A', 'B', 'C']) {
    const mine = reference.filter((b) => /^[ABC]\d+$/.test(b) && b[0] === prefix).map((b) => Number(b.slice(1)));
    assert.deepEqual(mine, Array.from({ length: N }, (_, i) => i + 1), `${prefix}'s messages out of send order`);
  }
  const seqs = alicePhone.store.timeline(group.conversation_id).map((m) => m.conv_seq);
  assert.deepEqual(seqs, Array.from({ length: total }, (_, i) => i + 1));
  ok(`all 4 devices show the identical order; conv_seq 1..${total} has no gaps; each sender's messages kept their send order`);

  step('History API (Cassandra, paged across sequence buckets)');
  // With BUCKET_SIZE=50 (compose demo setting) seq 45..54 spans buckets 0 and 1.
  const page = await api.history(bobPhone.token, group.conversation_id, 55, 10);
  assert.deepEqual(page.map((m) => m.conv_seq), Array.from({ length: 10 }, (_, i) => 54 - i));
  ok(`GET /messages?before_seq=55&limit=10 -> seq ${page[0].conv_seq}..${page.at(-1).conv_seq} (newest first, two partitions)`);

  step('Late joiner only sees messages after joining');
  await api.addMember(alicePhone.token, group.conversation_id, dave.user_id);
  const daveLaptop = await device(dave, 'web', GW2, { connect: false });
  await refreshConversations(daveLaptop);
  await daveLaptop.connect();
  alicePhone.send(group.conversation_id, 'Welcome Dave!');
  await waitFor('Dave gets welcome', () => bodies(daveLaptop, group.conversation_id).includes('Welcome Dave!'));
  await sleep(300);
  assert.deepEqual(bodies(daveLaptop, group.conversation_id), ['Welcome Dave!']);
  ok('Dave received only "Welcome Dave!" - none of the earlier history');

  step('Media: upload straight to S3 with a presigned URL; the message only carries the key');
  const bytes = Buffer.from('fake-jpeg-bytes-'.repeat(64));
  const up = await api.uploadUrl(alicePhone.token, 'image/jpeg', bytes.length);
  const put = await fetch(up.upload_url, { method: 'PUT', headers: up.headers, body: bytes });
  assert.ok(put.ok, `S3 upload failed: ${put.status}`);
  alicePhone.send(dm.conversation_id, null, { media: { key: up.key, content_type: 'image/jpeg', size: bytes.length } });
  const mediaMsg = await waitFor('Bob gets media msg', () => bobPhone.store.timeline(dm.conversation_id).find((m) => m.media));
  const { download_url: dl } = await api.downloadUrl(bobPhone.token, mediaMsg.media.key);
  const got = Buffer.from(await (await fetch(dl)).arrayBuffer());
  assert.ok(got.equals(bytes));
  ok(`Bob downloaded the ${bytes.length}-byte image from S3 via presigned GET; chat servers never touched the bytes`);

  console.log('\n\x1b[32m\x1b[1mAll scenarios passed.\x1b[0m');
}

main()
  .then(() => {
    for (const c of clients) c.close();
    process.exit(0);
  })
  .catch((err) => {
    console.error('\n\x1b[31mFAILED:\x1b[0m', err.message);
    for (const c of clients) c.close();
    process.exit(1);
  });
