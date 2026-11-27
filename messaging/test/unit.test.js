import { test } from 'node:test';
import assert from 'node:assert/strict';
import { issueToken, verifyToken } from '../src/shared/auth.js';
import { bucketOf } from '../src/shared/messageStore.js';
import { backoffDelay, ChatClient, LocalStore } from '../client/chatClient.js';

test('device tokens: valid, tampered, wrong secret, expired', () => {
  const t = issueToken({ userId: 'u_1', deviceId: 'd_1' }, 's3cret');
  assert.deepEqual(verifyToken(t, 's3cret'), { userId: 'u_1', deviceId: 'd_1' });
  assert.equal(verifyToken(t, 'other'), null);
  const [data, sig] = t.split('.');
  const forged = Buffer.from(JSON.stringify({ u: 'u_2', d: 'd_1', e: 9e9 })).toString('base64url');
  assert.equal(verifyToken(`${forged}.${sig}`, 's3cret'), null);
  assert.equal(verifyToken(`${data}.x`, 's3cret'), null);
  assert.equal(verifyToken(issueToken({ userId: 'u', deviceId: 'd' }, 's', -1), 's'), null);
  assert.equal(verifyToken(undefined, 's'), null);
});

test('sequence-range buckets', () => {
  assert.equal(bucketOf(0, 1000), 0);
  assert.equal(bucketOf(999, 1000), 0);
  assert.equal(bucketOf(1000, 1000), 1);
  assert.equal(bucketOf(2500, 1000), 2);
});

test('reconnect backoff grows exponentially with jitter and is capped', () => {
  const lo = (a) => backoffDelay(a, { random: () => 0 });
  const hi = (a) => backoffDelay(a, { random: () => 1 });
  assert.deepEqual([0, 1, 2, 3].map(hi), [500, 1000, 2000, 4000]);
  assert.deepEqual([0, 1, 2, 3].map(lo), [250, 500, 1000, 2000]); // jitter spreads the herd
  assert.equal(hi(20), 30_000);
});

const msg = (seq, id = `m${seq}`, conv = 'c1') => ({ message_id: id, conversation_id: conv, conv_seq: seq, body: `#${seq}` });

test('local store: dedupe by message_id, order by conv_seq, contiguous cursor, gap detection', () => {
  const s = new LocalStore();
  assert.deepEqual(s.insert(msg(1)), { result: 'new', gap: false });
  assert.deepEqual(s.insert(msg(3)), { result: 'new', gap: true }); // 2 missing -> must sync
  assert.equal(s.cursors.c1, 1, 'cursor only advances over contiguous messages');
  assert.deepEqual(s.insert(msg(1)), { result: 'duplicate', gap: true });
  assert.deepEqual(s.insert(msg(2)), { result: 'new', gap: false });
  assert.equal(s.cursors.c1, 3);
  assert.deepEqual(s.timeline('c1').map((m) => m.conv_seq), [1, 2, 3]);
});

test('local store: joined-late floor (group member added at seq 40)', () => {
  const s = new LocalStore();
  s.setCursorFloor('g', 40);
  assert.deepEqual(s.insert(msg(41, 'x', 'g')), { result: 'new', gap: false });
  assert.equal(s.cursors.g, 41);
});

test('message status: SENT -> DELIVERED -> READ from receipt watermarks (group needs everyone)', () => {
  const c = new ChatClient({ url: 'ws://x', token: 't', userId: 'a', deviceId: 'd' });
  c.setConversations([{ conversation_id: 'g', members: ['a', 'b', 'c'] }]);
  const m = { conversation_id: 'g', conv_seq: 5 };
  assert.equal(c.statusOf({ conversation_id: 'g', status: 'PENDING' }), 'PENDING');
  assert.equal(c.statusOf(m), 'SENT');
  c.applyReceipt({ conversation_id: 'g', user_id: 'b', delivered_seq: 5, read_seq: 5 });
  assert.equal(c.statusOf(m), 'SENT', 'c has not received it yet');
  c.applyReceipt({ conversation_id: 'g', user_id: 'c', delivered_seq: 7, read_seq: 0 });
  assert.equal(c.statusOf(m), 'DELIVERED');
  c.applyReceipt({ conversation_id: 'g', user_id: 'c', delivered_seq: 3, read_seq: 6 }); // stale delivered ignored
  assert.equal(c.statusOf(m), 'READ');
  assert.equal(c.store.receipts.g.c.delivered_seq, 7, 'watermarks never go backwards');
});

test('client retransmits unacked sends with the same message_id', async () => {
  const c = new ChatClient({ url: 'ws://x', token: 't', userId: 'a', deviceId: 'd', ackTimeoutMs: 20 });
  const sent = [];
  c.frame = (f) => sent.push(f);
  const m = c.send('c1', 'hello');
  assert.equal(m.client_seq, 1);
  await new Promise((r) => setTimeout(r, 70));
  assert.ok(sent.length >= 2, `retransmitted ${sent.length} times`);
  assert.ok(sent.every((f) => f.message_id === m.message_id && f.client_seq === 1));
  c.onFrame({ type: 'send_ack', message_id: m.message_id, conversation_id: 'c1', conv_seq: 9 });
  const n = sent.length;
  await new Promise((r) => setTimeout(r, 60));
  assert.equal(sent.length, n, 'no more retransmits after ACK');
  assert.equal(c.store.timeline('c1')[0].status, 'SENT');
  c.close();
});
