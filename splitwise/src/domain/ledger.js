// Ledger math: how expenses and settlements move balances.
//
// Pairwise balances are stored ONCE per pair in canonical order (never two mirrored rows):
//   (user_low, user_high, low_to_high)   low_to_high > 0: low owes high;  < 0: high owes low.
//
// Net position per member:  net = paid - owed  (> 0 should receive, < 0 should pay).
// Every operation produces deltas for both views; applying the same deltas negated is an
// exact reversal (used for edits and deletions).

export function pairKey(a, b) {
  if (a === b) throw new Error('pair of identical users');
  return a < b ? { low: a, high: b } : { low: b, high: a };
}

/** "debtor owes creditor `amount`" as a signed delta on the canonical pair row. */
export function debtDelta(debtor, creditor, amount) {
  const { low, high } = pairKey(debtor, creditor);
  return { low, high, delta: debtor === low ? amount : -amount };
}

/**
 * Deltas of an expense: each participant other than the payer now owes the payer their share.
 * @returns {{pairs: Array<{low, high, delta}>, nets: Map<string, number>}}
 */
export function expenseDeltas(paidBy, splits, sign = 1) {
  const pairs = [];
  const nets = new Map();
  const add = (u, v) => nets.set(u, (nets.get(u) ?? 0) + v);
  const total = splits.reduce((a, s) => a + s.amount, 0);
  add(paidBy, sign * total);
  for (const s of splits) {
    add(s.user_id, -sign * s.amount);
    if (s.user_id !== paidBy && s.amount !== 0) pairs.push(debtDelta(s.user_id, paidBy, sign * s.amount));
  }
  return { pairs: mergePairs(pairs), nets };
}

/** A confirmed settlement: `from` paid `to`, which reduces what `from` owes `to`. */
export function settlementDeltas(from, to, amount, sign = 1) {
  return {
    pairs: [debtDelta(from, to, -sign * amount)],
    nets: new Map([[from, sign * amount], [to, -sign * amount]]),
  };
}

export function mergePairs(pairs) {
  const m = new Map();
  for (const p of pairs) {
    const k = `${p.low}|${p.high}`;
    const cur = m.get(k);
    if (cur) cur.delta += p.delta;
    else m.set(k, { ...p });
  }
  // Sorted, so concurrent transactions lock rows in the same order (no deadlocks).
  return [...m.values()].filter((p) => p.delta !== 0)
    .sort((a, b) => (a.low === b.low ? (a.high < b.high ? -1 : 1) : a.low < b.low ? -1 : 1));
}

/** Rebuild balances from history (source of truth) - used by reconciliation and tests. */
export function replay(expenses, settlements) {
  const pairs = new Map();
  const nets = new Map();
  const apply = ({ pairs: ps, nets: ns }) => {
    for (const p of ps) pairs.set(`${p.low}|${p.high}`, (pairs.get(`${p.low}|${p.high}`) ?? 0) + p.delta);
    for (const [u, v] of ns) nets.set(u, (nets.get(u) ?? 0) + v);
  };
  for (const e of expenses) apply(expenseDeltas(e.paid_by, e.splits));
  for (const s of settlements) apply(settlementDeltas(s.from_user, s.to_user, s.amount));
  return { pairs, nets };
}

/** Human view of pairwise rows: [{from_user, to_user, amount}] with amount > 0. */
export function pairsToDebts(rows) {
  return rows
    .filter((r) => r.low_to_high !== 0)
    .map((r) => (r.low_to_high > 0
      ? { from_user: r.user_low, to_user: r.user_high, amount: r.low_to_high }
      : { from_user: r.user_high, to_user: r.user_low, amount: -r.low_to_high }));
}
