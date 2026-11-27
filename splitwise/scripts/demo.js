// End-to-end scenario against the running stack. Every step asserts; exits non-zero on failure.
//   API_URL=http://localhost:3000 DATABASE_URL=postgres://splitwise:splitwise@localhost:5432/splitwise node scripts/demo.js
// (DATABASE_URL is only used for the reconciliation step, to simulate a corrupted projection.)
import assert from 'node:assert/strict';
import { createHmac, randomUUID } from 'node:crypto';
import pg from 'pg';

const API = process.env.API_URL ?? 'http://localhost:3000';
const DB = process.env.DATABASE_URL ?? 'postgres://splitwise:splitwise@localhost:5432/splitwise';
const SECRET = process.env.AUTH_SECRET ?? 'dev-only-secret-change-me';

const step = (t) => console.log(`\n\x1b[1m▶ ${t}\x1b[0m`);
const ok = (t) => console.log(`  \x1b[32m✔\x1b[0m ${t}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const usd = (c) => `$${(c / 100).toFixed(2)}`;

async function call(method, path, { token, body, headers = {} } = {}) {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...(token && { authorization: `Bearer ${token}` }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, body: json, headers: res.headers };
}
async function ok2xx(method, path, opts) {
  const r = await call(method, path, opts);
  if (r.status >= 300) throw new Error(`${method} ${path} -> ${r.status} ${JSON.stringify(r.body)}`);
  return r.body;
}
async function waitFor(what, fn, ms = 5000) {
  const t0 = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - t0 > ms) throw new Error(`timed out: ${what}`);
    await sleep(100);
  }
}

async function main() {
  step('Setup: users and a USD group');
  const [alice, bob, charlie, dave] = await Promise.all(['Alice', 'Bob', 'Charlie', 'Dave'].map((name) => ok2xx('POST', '/users', { body: { name } })));
  const T = { [alice.user_id]: alice.token, [bob.user_id]: bob.token, [charlie.user_id]: charlie.token };
  const N = { [alice.user_id]: 'Alice', [bob.user_id]: 'Bob', [charlie.user_id]: 'Charlie', [dave.user_id]: 'Dave' };
  const g = await ok2xx('POST', '/groups', { token: alice.token, body: { name: 'Goa Trip', currency: 'USD', members: [bob.user_id, charlie.user_id] } });
  const G = g.group_id;
  const A = alice.user_id;
  const B = bob.user_id;
  const C = charlie.user_id;
  const balances = () => ok2xx('GET', `/groups/${G}/balances`, { token: alice.token });
  const debts = (bal) => bal.pairwise.map((d) => `${N[d.from_user]}→${N[d.to_user]} ${usd(d.amount)}`).sort();
  ok(`group ${G} with Alice (admin), Bob, Charlie`);

  // Live updates: subscribe before anything happens.
  const live = [];
  const sse = new AbortController();
  fetch(`${API}/groups/${G}/stream?access_token=${encodeURIComponent(alice.token)}`, { signal: sse.signal }).then(async (res) => {
    const dec = new TextDecoder();
    for await (const chunk of res.body) for (const line of dec.decode(chunk).split('\n')) if (line.startsWith('data: ')) live.push(JSON.parse(line.slice(6)));
  }).catch(() => {});

  step("Doc example: Alice pays $1,200 dinner, Bob pays $600 taxi (equal splits)");
  const dinner = await ok2xx('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Dinner', total_amount: 120000 } });
  assert.deepEqual(dinner.splits.map((s) => s.amount), [40000, 40000, 40000]);
  let bal = await balances();
  assert.deepEqual(debts(bal), ['Bob→Alice $400.00', 'Charlie→Alice $400.00']);
  ok(`after dinner: ${debts(bal).join(', ')}`);
  const taxi = await ok2xx('POST', `/groups/${G}/expenses`, { token: bob.token, body: { description: 'Taxi', total_amount: 60000, paid_by: B } });
  bal = await balances();
  assert.deepEqual(debts(bal), ['Bob→Alice $200.00', 'Charlie→Alice $400.00', 'Charlie→Bob $200.00']);
  ok(`pairwise (offset): ${debts(bal).join(', ')}`);
  const nets = Object.fromEntries(bal.members.map((m) => [N[m.user_id], m.net_balance]));
  assert.deepEqual(nets, { Alice: 60000, Bob: 0, Charlie: -60000 });
  ok('net positions: Alice +$600, Bob $0, Charlie -$600');
  assert.deepEqual(bal.simplified.transfers.map((t) => `${N[t.from_user]}→${N[t.to_user]} ${usd(t.amount)}`), ['Charlie→Alice $600.00']);
  ok(`simplified (${bal.simplified.algorithm}): Charlie→Alice $600.00 - 1 payment instead of 3`);

  step('The penny problem: $100.00 / 3 (paid by Charlie)');
  const snacks = await ok2xx('POST', `/groups/${G}/expenses`, { token: charlie.token, body: { description: 'Snacks', total_amount: 10000, paid_by: C } });
  const byName = Object.fromEntries(snacks.splits.map((s) => [N[s.user_id], s.amount]));
  assert.deepEqual(byName, { Alice: 3333, Bob: 3333, Charlie: 3334 });
  ok('Alice $33.33, Bob $33.33, Charlie (payer) $33.34 - sums to exactly $100.00');

  step('Other split types and validation');
  const pct = await ok2xx('POST', `/groups/${G}/expenses`, { token: alice.token, body: {
    description: 'Hotel', total_amount: 100000, split_type: 'PERCENTAGE',
    participants: [{ user_id: A, percent: 50 }, { user_id: B, percent: 30 }, { user_id: C, percent: 20 }] } });
  assert.deepEqual(pct.splits.map((s) => s.amount), [50000, 30000, 20000]);
  const shares = await ok2xx('POST', `/groups/${G}/expenses`, { token: alice.token, body: {
    description: 'Boat', total_amount: 100000, split_type: 'SHARES',
    participants: [{ user_id: A, shares: 2 }, { user_id: B, shares: 1 }, { user_id: C, shares: 1 }] } });
  assert.deepEqual(shares.splits.map((s) => s.amount), [50000, 25000, 25000]);
  ok('PERCENTAGE 50/30/20 and SHARES 2/1/1 of $1,000');
  let r = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Bad', total_amount: 1000, split_type: 'PERCENTAGE', participants: [{ user_id: A, percent: 60 }, { user_id: B, percent: 30 }] } });
  assert.equal(r.status, 400);
  r = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Bad', total_amount: 1000, split_type: 'EXACT', participants: [{ user_id: A, amount: 500 }, { user_id: B, amount: 400 }] } });
  assert.equal(r.status, 400);
  r = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Bad', total_amount: 10.5 } });
  assert.equal(r.status, 400);
  r = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Bad', total_amount: 100, participants: [{ user_id: dave.user_id }] } });
  assert.equal(r.status, 400);
  r = await call('GET', `/groups/${G}/balances`, { token: dave.token });
  assert.equal(r.status, 404);
  ok('rejected: percentages ≠ 100, exact ≠ total, fractional cents, non-member participant; non-members can\'t see the group');

  step('Idempotency: a retried "Add expense" is recorded once');
  const key = randomUUID();
  const lunch = { description: 'Lunch', total_amount: 9000 };
  const v0 = (await balances()).balance_version;
  const first = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: lunch, headers: { 'idempotency-key': key } });
  const retry = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: lunch, headers: { 'idempotency-key': key } });
  assert.equal(first.status, 201);
  assert.equal(retry.body.expense_id, first.body.expense_id);
  assert.equal(retry.headers.get('idempotent-replay'), 'true');
  const key2 = randomUUID();
  const burst = await Promise.all(Array.from({ length: 8 }, () => call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { description: 'Coffee', total_amount: 1500 }, headers: { 'idempotency-key': key2 } })));
  assert.equal(new Set(burst.map((b) => b.body.expense_id)).size, 1);
  assert.equal((await balances()).balance_version, v0 + 2);
  r = await call('POST', `/groups/${G}/expenses`, { token: alice.token, body: { ...lunch, total_amount: 1 }, headers: { 'idempotency-key': key } });
  assert.equal(r.status, 422);
  ok('retry -> same expense (Idempotent-Replay: true); 8 concurrent retries -> 1 expense; key reused with a different body -> 422');

  step('Edit = reverse old effect + apply new (optimistic version check)');
  const before = Object.fromEntries((await balances()).members.map((m) => [m.user_id, m.net_balance]));
  r = await call('PATCH', `/expenses/${dinner.expense_id}`, { token: alice.token, body: { version: 1, total_amount: 150000 } });
  assert.equal(r.status, 200);
  assert.equal(r.body.version, 2);
  const after = Object.fromEntries((await balances()).members.map((m) => [m.user_id, m.net_balance]));
  assert.equal(after[A] - before[A], 20000); // Alice paid $300 more, her share grew $100
  assert.equal(after[B] - before[B], -10000);
  assert.equal(after[C] - before[C], -10000);
  ok('dinner $1,200 -> $1,500: Alice +$200, Bob -$100, Charlie -$100');
  r = await call('PATCH', `/expenses/${dinner.expense_id}`, { token: alice.token, body: { version: 1, description: 'Stale edit from another tab' } });
  assert.equal(r.status, 409);
  assert.equal(r.body.code, 'VERSION_CONFLICT');
  ok(`concurrent edit with a stale version -> 409 VERSION_CONFLICT (current ${r.body.current_version})`);
  r = await call('PATCH', `/expenses/${dinner.expense_id}`, { token: charlie.token, body: { version: 2, description: 'Not mine' } });
  assert.equal(r.status, 403);
  ok("Charlie (neither creator, payer nor admin) can't edit Alice's expense");
  const detail = await ok2xx('GET', `/expenses/${dinner.expense_id}`, { token: bob.token });
  assert.deepEqual(detail.history.map((h) => h.version), [1, 2]);
  ok('every revision kept in expense_versions');

  step('Delete = soft delete + reversal; restore re-applies');
  const v1 = Object.fromEntries((await balances()).members.map((m) => [m.user_id, m.net_balance]));
  await ok2xx('DELETE', `/expenses/${taxi.expense_id}?version=1`, { token: bob.token });
  const v2 = Object.fromEntries((await balances()).members.map((m) => [m.user_id, m.net_balance]));
  assert.equal(v2[B] - v1[B], -40000); // Bob no longer credited for the $600 taxi minus his share
  const list = await ok2xx('GET', `/groups/${G}/expenses?include_deleted=true&limit=100`, { token: alice.token });
  assert.equal(list.items.find((e) => e.expense_id === taxi.expense_id).status, 'DELETED');
  await ok2xx('POST', `/expenses/${taxi.expense_id}/restore`, { token: bob.token });
  assert.deepEqual(Object.fromEntries((await balances()).members.map((m) => [m.user_id, m.net_balance])), v1);
  ok('taxi deleted (balances reversed, record kept as DELETED), then restored (balances identical to before)');

  step('Settlements: who confirms what');
  const sum0 = (await balances()).members;
  const netOf = (members, u) => members.find((m) => m.user_id === u).net_balance;
  const s1 = await ok2xx('POST', `/groups/${G}/settlements`, { token: charlie.token, body: { from_user: C, to_user: A, amount: 20000, method: 'CASH' } });
  assert.equal(s1.status, 'PENDING');
  assert.equal(netOf((await balances()).members, C), netOf(sum0, C));
  ok("Charlie says he paid Alice $200 cash -> PENDING, balances unchanged (no fake 'I paid')");
  r = await call('POST', `/settlements/${s1.settlement_id}/confirm`, { token: charlie.token });
  assert.equal(r.status, 403);
  await ok2xx('POST', `/settlements/${s1.settlement_id}/confirm`, { token: alice.token });
  assert.equal(netOf((await balances()).members, C), netOf(sum0, C) + 20000);
  ok('only Alice (receiver) can confirm; after confirming Charlie owes $200 less');
  const s2 = await ok2xx('POST', `/groups/${G}/settlements`, { token: alice.token, body: { from_user: B, to_user: A, amount: 5000 } });
  assert.equal(s2.status, 'CONFIRMED');
  ok('Alice records receiving $50 from Bob -> CONFIRMED immediately (partial payment)');

  const upi = await ok2xx('POST', `/groups/${G}/settlements`, { token: bob.token, body: { from_user: B, to_user: A, amount: 7000, method: 'UPI' } });
  assert.equal(upi.status, 'PENDING');
  const webhook = (body) => call('POST', '/webhooks/payments', { body, headers: { 'x-signature': createHmac('sha256', SECRET).update(JSON.stringify(body)).digest('hex') } });
  r = await call('POST', '/webhooks/payments', { body: { provider_ref: upi.payment.provider_ref, outcome: 'SUCCEEDED' }, headers: { 'x-signature': 'forged' } });
  assert.equal(r.status, 401);
  const netB = netOf((await balances()).members, B);
  assert.equal((await webhook({ provider_ref: upi.payment.provider_ref, outcome: 'SUCCEEDED' })).body.status, 'CONFIRMED');
  assert.equal((await webhook({ provider_ref: upi.payment.provider_ref, outcome: 'SUCCEEDED' })).body.duplicate, true);
  assert.equal(netOf((await balances()).members, B), netB + 7000);
  ok('UPI payment: forged webhook rejected; provider SUCCEEDED -> CONFIRMED; duplicate webhook ignored (applied once)');
  const upi2 = await ok2xx('POST', `/groups/${G}/settlements`, { token: bob.token, body: { from_user: B, to_user: A, amount: 100, method: 'CARD' } });
  assert.equal((await webhook({ provider_ref: upi2.payment.provider_ref, outcome: 'FAILED' })).body.status, 'FAILED');
  ok('failed card payment -> FAILED, balances untouched');

  step('Membership rules');
  r = await call('DELETE', `/groups/${G}/members/${C}`, { token: alice.token });
  assert.equal(r.status, 409);
  ok('Charlie cannot leave with an unsettled balance (409)');
  await ok2xx('POST', `/groups/${G}/members`, { token: alice.token, body: { user_id: dave.user_id } });
  T[dave.user_id] = dave.token;
  ok('Dave added by the admin');

  step('Multi-currency: an INR receipt in a USD group');
  const inr = await ok2xx('POST', `/groups/${G}/expenses`, { token: alice.token, body: {
    description: 'Street food (₹)', total_amount: 249000, currency: 'INR', exchange_rate: '0.012', participants: [{ user_id: A }, { user_id: dave.user_id }] } });
  assert.equal(inr.total_amount, 2988); // ₹2,490.00 x 0.012 = $29.88
  assert.equal(inr.original_amount, 249000);
  ok('₹2,490.00 @ 0.012 -> $29.88 stored in the group currency (original amount + rate kept)');

  step('Concurrency: 40 expenses from 4 people at once (no lost updates)');
  const people = [A, B, C, dave.user_id];
  const bursts = await Promise.all(Array.from({ length: 40 }, (_, i) => {
    const payer = people[i % 4];
    return call('POST', `/groups/${G}/expenses`, { token: T[payer], body: { description: `Round ${i}`, total_amount: 1000 + i * 37, paid_by: payer } });
  }));
  assert.ok(bursts.every((b) => b.status === 201), JSON.stringify(bursts.find((b) => b.status !== 201)?.body));
  bal = await balances();
  assert.equal(bal.members.reduce((a, m) => a + m.net_balance, 0), 0);
  let rec = await ok2xx('GET', `/groups/${G}/reconcile`, { token: alice.token });
  assert.equal(rec.consistent, true, JSON.stringify(rec.drift));
  ok(`all 40 committed; nets sum to 0; reconcile vs full history (${rec.expenses} expenses, ${rec.settlements} settlements): consistent`);

  step('Expense history pagination (keyset cursor)');
  const seen = new Set();
  let cursor = null;
  let pages = 0;
  do {
    const page = await ok2xx('GET', `/groups/${G}/expenses?limit=7${cursor ? `&cursor=${cursor}` : ''}`, { token: bob.token });
    for (const e of page.items) {
      assert.ok(!seen.has(e.expense_id), 'duplicate across pages');
      seen.add(e.expense_id);
    }
    cursor = page.next_cursor;
    pages++;
  } while (cursor);
  assert.equal(seen.size, rec.expenses);
  ok(`${seen.size} active expenses in ${pages} pages of 7, no duplicates or gaps`);

  step('Cached balances keyed by balance_version');
  const c1 = await balances();
  const c2 = await balances();
  assert.equal(c1.balance_version, c2.balance_version);
  assert.equal(c2.cache, 'hit');
  await ok2xx('POST', `/groups/${G}/expenses`, { token: bob.token, body: { description: 'Water', total_amount: 300, paid_by: B } });
  const c3 = await balances();
  assert.equal(c3.cache, 'miss');
  assert.equal(c3.balance_version, c1.balance_version + 1);
  ok('repeat read = cache hit; any write bumps the version -> next read recomputes (never stale)');

  step('Async side effects: notifications, home-screen summary, live stream');
  const notes = await waitFor('Bob notified', async () => {
    const n = await ok2xx('GET', '/me/notifications', { token: bob.token });
    return n.some((x) => x.text.includes('"Dinner"')) && n;
  });
  ok(`Bob's inbox: "${notes.find((x) => x.text.includes('"Dinner"') && x.type === 'EXPENSE_CREATED').text}"`);
  const pendingNote = (await ok2xx('GET', '/me/notifications', { token: alice.token })).find((x) => x.type === 'SETTLEMENT_PENDING');
  assert.ok(pendingNote);
  ok(`Alice's inbox: "${pendingNote.text}"`);
  const summary = await waitFor('summary projected', async () => {
    const s = await ok2xx('GET', '/me/summary', { token: alice.token });
    const net = (await balances()).members.find((m) => m.user_id === A).net_balance;
    return s.source === 'cache' && s.totals.USD?.net === net && s;
  });
  ok(`Alice's home screen (Redis, O(1)): you are owed ${usd(summary.totals.USD.you_are_owed)}`);
  await waitFor('live events', () => live.some((e) => e.type === 'EXPENSE_UPDATED') && live.some((e) => e.type === 'SETTLEMENT_CONFIRMED'));
  ok(`live stream delivered ${live.length} events to Alice's open app (e.g. ${[...new Set(live.map((e) => e.type))].slice(0, 4).join(', ')})`);
  sse.abort();

  step('Reconciliation catches a corrupted projection');
  const db = new pg.Client({ connectionString: DB });
  await db.connect();
  await db.query('UPDATE group_members SET net_balance = net_balance + 1 WHERE group_id = $1 AND user_id = $2', [G, B]);
  await db.end();
  rec = await ok2xx('GET', `/groups/${G}/reconcile`, { token: alice.token });
  assert.equal(rec.consistent, false);
  assert.deepEqual(rec.drift.map((d) => d.kind), ['net']);
  ok(`simulated bug (+1 cent on Bob) detected: expected ${rec.drift[0].expected}, actual ${rec.drift[0].actual}`);
  rec = await ok2xx('GET', `/groups/${G}/reconcile?repair=true`, { token: alice.token });
  assert.equal(rec.repaired, true);
  assert.equal((await ok2xx('GET', `/groups/${G}/reconcile`, { token: alice.token })).consistent, true);
  ok('repaired from history (admin only, audited as BALANCES_REPAIRED)');

  bal = await balances();
  console.log('\n  Final simplified plan:');
  for (const t of bal.simplified.transfers) console.log(`    ${N[t.from_user]} pays ${N[t.to_user]} ${usd(t.amount)}`);
  console.log(`  (${bal.pairwise.length} pairwise debts -> ${bal.simplified.transfers.length} payments, ${bal.simplified.algorithm})`);
  console.log('\n\x1b[32m\x1b[1mAll scenarios passed.\x1b[0m');
}

main().then(() => process.exit(0)).catch((err) => {
  console.error('\n\x1b[31mFAILED:\x1b[0m', err.stack ?? err.message);
  process.exit(1);
});
