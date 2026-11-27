import { test } from 'node:test';
import assert from 'node:assert/strict';
import { calculateSplits, SplitError } from '../src/domain/splitCalculator.js';
import { debtDelta, expenseDeltas, pairsToDebts, replay, settlementDeltas } from '../src/domain/ledger.js';
import { greedy, optimal, simplify } from '../src/domain/simplify.js';
import { convert, format } from '../src/domain/money.js';
import { issueToken, verifyToken } from '../src/shared/auth.js';

const P = (...ids) => ids.map((user_id) => ({ user_id }));
const amounts = (splits) => Object.fromEntries(splits.map((s) => [s.user_id, s.amount]));

// ---------- split calculator ----------

test('EQUAL: $1,200 / 3 and the penny problem ($100 / 3 -> payer absorbs the extra cent)', () => {
  assert.deepEqual(amounts(calculateSplits({ total: 120000, splitType: 'EQUAL', paidBy: 'A', participants: P('A', 'B', 'C') })),
    { A: 40000, B: 40000, C: 40000 });
  assert.deepEqual(amounts(calculateSplits({ total: 10000, splitType: 'EQUAL', paidBy: 'B', participants: P('A', 'B', 'C') })),
    { A: 3333, B: 3334, C: 3333 });
  // Payer not participating: first participants absorb the cents.
  assert.deepEqual(amounts(calculateSplits({ total: 200, splitType: 'EQUAL', paidBy: 'Z', participants: P('A', 'B', 'C') })),
    { A: 67, B: 67, C: 66 });
});

test('EXACT: must add up to the total, to the cent', () => {
  const s = calculateSplits({ total: 100000, splitType: 'EXACT', paidBy: 'A',
    participants: [{ user_id: 'A', amount: 50000 }, { user_id: 'B', amount: 30000 }, { user_id: 'C', amount: 20000 }] });
  assert.deepEqual(amounts(s), { A: 50000, B: 30000, C: 20000 });
  assert.throws(() => calculateSplits({ total: 100000, splitType: 'EXACT', paidBy: 'A',
    participants: [{ user_id: 'A', amount: 50000 }, { user_id: 'B', amount: 30000 }] }), /difference 20000/);
  assert.throws(() => calculateSplits({ total: 100, splitType: 'EXACT', paidBy: 'A', participants: [{ user_id: 'A', amount: 99.5 }, { user_id: 'B', amount: 0.5 }] }), SplitError);
});

test('PERCENTAGE: 50/30/20, decimals allowed, must total 100', () => {
  const s = calculateSplits({ total: 100000, splitType: 'PERCENTAGE', paidBy: 'A',
    participants: [{ user_id: 'A', percent: 50 }, { user_id: 'B', percent: 30 }, { user_id: 'C', percent: 20 }] });
  assert.deepEqual(amounts(s), { A: 50000, B: 30000, C: 20000 });
  const odd = calculateSplits({ total: 1001, splitType: 'PERCENTAGE', paidBy: 'C',
    participants: [{ user_id: 'A', percent: '33.33' }, { user_id: 'B', percent: '33.33' }, { user_id: 'C', percent: '33.34' }] });
  assert.equal(odd.reduce((a, x) => a + x.amount, 0), 1001);
  assert.throws(() => calculateSplits({ total: 100, splitType: 'PERCENTAGE', paidBy: 'A',
    participants: [{ user_id: 'A', percent: 60 }, { user_id: 'B', percent: 30 }] }), /add up to 100 \(got 90\)/);
  assert.throws(() => calculateSplits({ total: 100, splitType: 'PERCENTAGE', paidBy: 'A',
    participants: [{ user_id: 'A', percent: '50.001' }, { user_id: 'B', percent: 50 }] }), /at most 2 decimals/);
});

test('SHARES: 2/1/1 of $1,000 and fractional shares', () => {
  const s = calculateSplits({ total: 100000, splitType: 'SHARES', paidBy: 'A',
    participants: [{ user_id: 'A', shares: 2 }, { user_id: 'B', shares: 1 }, { user_id: 'C', shares: 1 }] });
  assert.deepEqual(amounts(s), { A: 50000, B: 25000, C: 25000 });
  const f = calculateSplits({ total: 1000, splitType: 'SHARES', paidBy: 'A',
    participants: [{ user_id: 'A', shares: 1.5 }, { user_id: 'B', shares: 1 }, { user_id: 'C', shares: 0 }] });
  assert.deepEqual(amounts(f), { A: 600, B: 400, C: 0 });
});

test('split invariant holds for 2,000 random splits of every type', () => {
  let seed = 7;
  const rnd = (n) => (seed = (seed * 1103515245 + 12345) % 2 ** 31) % n;
  for (let t = 0; t < 2000; t++) {
    const n = 1 + rnd(12);
    const total = 1 + rnd(10_000_000);
    const ids = Array.from({ length: n }, (_, i) => `u${i}`);
    const type = ['EQUAL', 'SHARES', 'PERCENTAGE'][t % 3];
    let participants = P(...ids);
    if (type === 'SHARES') participants = ids.map((user_id) => ({ user_id, shares: 1 + rnd(5) }));
    if (type === 'PERCENTAGE') {
      const bp = ids.map(() => 1 + rnd(1000));
      const sum = bp.reduce((a, b) => a + b, 0);
      const scaledBp = bp.map((b) => Math.floor((b * 10000) / sum));
      scaledBp[0] += 10000 - scaledBp.reduce((a, b) => a + b, 0);
      participants = ids.map((user_id, i) => ({ user_id, percent: (scaledBp[i] / 100).toFixed(2) }));
    }
    const s = calculateSplits({ total, splitType: type, paidBy: ids[rnd(n)], participants });
    assert.equal(s.reduce((a, x) => a + x.amount, 0), total, `${type} n=${n} total=${total}`);
    assert.ok(s.every((x) => Number.isSafeInteger(x.amount) && x.amount >= 0));
  }
});

test('validation: duplicates, empty, bad totals', () => {
  assert.throws(() => calculateSplits({ total: 100, splitType: 'EQUAL', paidBy: 'A', participants: P('A', 'A') }), /duplicate/);
  assert.throws(() => calculateSplits({ total: 100, splitType: 'EQUAL', paidBy: 'A', participants: [] }), /participants/);
  assert.throws(() => calculateSplits({ total: 10.5, splitType: 'EQUAL', paidBy: 'A', participants: P('A') }), /integer/);
  assert.throws(() => calculateSplits({ total: 100, splitType: 'WEIRD', paidBy: 'A', participants: P('A') }), /split_type/);
});

// ---------- ledger ----------

test('pairwise: canonical single row per pair, offsetting debts (doc example)', () => {
  // Alice pays 1200 for A,B,C; Bob pays 600 for A,B,C.
  const e1 = { paid_by: 'alice', splits: [{ user_id: 'alice', amount: 400 }, { user_id: 'bob', amount: 400 }, { user_id: 'charlie', amount: 400 }] };
  const e2 = { paid_by: 'bob', splits: [{ user_id: 'alice', amount: 200 }, { user_id: 'bob', amount: 200 }, { user_id: 'charlie', amount: 200 }] };
  const { pairs, nets } = replay([e1, e2], []);
  const rows = [...pairs].map(([k, v]) => { const [user_low, user_high] = k.split('|'); return { user_low, user_high, low_to_high: v }; });
  const debts = pairsToDebts(rows).sort((a, b) => a.from_user.localeCompare(b.from_user) || a.to_user.localeCompare(b.to_user));
  assert.deepEqual(debts, [
    { from_user: 'bob', to_user: 'alice', amount: 200 },
    { from_user: 'charlie', to_user: 'alice', amount: 400 },
    { from_user: 'charlie', to_user: 'bob', amount: 200 },
  ]);
  assert.deepEqual(Object.fromEntries(nets), { alice: 600, bob: 0, charlie: -600 });
});

test('reversal: applying negated deltas restores the previous state exactly', () => {
  const splits = [{ user_id: 'a', amount: 3334 }, { user_id: 'b', amount: 3333 }, { user_id: 'c', amount: 3333 }];
  const fwd = expenseDeltas('a', splits);
  const rev = expenseDeltas('a', splits, -1);
  for (let i = 0; i < fwd.pairs.length; i++) assert.equal(fwd.pairs[i].delta + rev.pairs[i].delta, 0);
  for (const [u, v] of fwd.nets) assert.equal(v + rev.nets.get(u), 0);
  assert.equal([...fwd.nets.values()].reduce((x, y) => x + y, 0), 0, 'nets always sum to zero');
});

test('settlement reduces the debt; overpaying flips direction', () => {
  assert.deepEqual(debtDelta('b', 'a', 400), { low: 'a', high: 'b', delta: -400 }); // b owes a
  const s = settlementDeltas('b', 'a', 400);
  assert.deepEqual(s.pairs, [{ low: 'a', high: 'b', delta: 400 }]);
  assert.deepEqual(Object.fromEntries(s.nets), { b: 400, a: -400 });
});

// ---------- simplification ----------

test('simplify: chain A->B->C collapses to A->C (doc example)', () => {
  assert.deepEqual(greedy({ A: -100, B: 0, C: 100 }), [{ from_user: 'A', to_user: 'C', amount: 100 }]);
  assert.deepEqual(greedy({ alice: 600, bob: 0, charlie: -600 }), [{ from_user: 'charlie', to_user: 'alice', amount: 600 }]);
});

test('simplify: greedy is valid but not always minimal; optimal finds the minimum', () => {
  const nets = { u0: -20, u1: -20, u2: -30, u3: 40, u4: -30, u5: 60 }; // {u0,u1,u3} and {u2,u4,u5} are zero-sum
  const settle = (transfers) => {
    const left = { ...nets };
    for (const t of transfers) {
      left[t.from_user] += t.amount;
      left[t.to_user] -= t.amount;
      assert.ok(t.amount > 0);
    }
    return Object.values(left).every((v) => v === 0);
  };
  const g = greedy(nets);
  const o = optimal(nets);
  assert.ok(settle(g) && settle(o), 'both plans settle everyone');
  assert.equal(g.length, 5);
  assert.equal(o.length, 4);
  assert.equal(simplify(nets).algorithm, 'optimal');
  assert.equal(simplify(nets, { optimalMax: 3 }).algorithm, 'greedy');
});

test('simplify: random groups always settle, greedy <= n-1, optimal <= greedy', () => {
  let seed = 3;
  const rnd = (n) => (seed = (seed * 1103515245 + 12345) % 2 ** 31) % n;
  for (let t = 0; t < 300; t++) {
    const n = 2 + rnd(9);
    const v = Array.from({ length: n - 1 }, () => (rnd(200) - 100) * 25);
    v.push(-v.reduce((a, b) => a + b, 0));
    const nets = Object.fromEntries(v.map((x, i) => [`u${i}`, x]));
    const nonZero = v.filter(Boolean).length;
    const g = greedy(nets);
    const o = optimal(nets);
    assert.ok(g.length <= Math.max(0, nonZero - 1));
    assert.ok(o.length <= g.length);
    for (const plan of [g, o]) {
      const left = { ...nets };
      for (const x of plan) { left[x.from_user] += x.amount; left[x.to_user] -= x.amount; }
      assert.ok(Object.values(left).every((x) => x === 0));
    }
  }
  assert.throws(() => greedy({ a: 1, b: 0 }), /sum to zero/);
});

// ---------- money / auth ----------

test('money: formatting and exact currency conversion', () => {
  assert.equal(format(120000, 'USD'), '1200.00 USD');
  assert.equal(format(-5, 'USD'), '-0.05 USD');
  assert.equal(format(500, 'JPY'), '500 JPY');
  assert.equal(convert(1000, 'USD', 'INR', '83.125'), 83125);  // $10.00 -> ₹831.25
  assert.equal(convert(1000, 'USD', 'JPY', '151.5'), 1515);    // $10.00 -> ¥1515
  assert.equal(convert(1515, 'JPY', 'USD', '0.0066'), 1000);   // ¥1515 -> $10.00 (999.9 rounded)
  assert.throws(() => convert(100, 'USD', 'INR', '83,1'), /exchange_rate/);
});

test('auth tokens', () => {
  const t = issueToken('u_1', 'k');
  assert.equal(verifyToken(t, 'k'), 'u_1');
  assert.equal(verifyToken(t, 'other'), null);
  assert.equal(verifyToken(issueToken('u', 'k', -1), 'k'), null);
});
