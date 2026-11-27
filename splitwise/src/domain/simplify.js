// Debt simplification ("minimize cash flow").
//
// Input: net positions (net = paid - owed; they always sum to 0 inside a group).
// Output: SUGGESTED transfers. This never touches the ledger and never marks anything as paid.
//
// greedy  - max-heap of creditors + max-heap of debtors; repeatedly settle the largest debtor
//           with the largest creditor, push back the remainder. O(n log n), at most n-1 transfers.
//           Not always the true minimum.
// optimal - the true minimum is  (#non-zero members) - (max number of disjoint zero-sum
//           subgroups), because each zero-sum subgroup of k people needs exactly k-1 transfers.
//           Found with a bitmask DP, O(2^n * n), so only used for small n (default <= 15).

class MaxHeap {
  constructor(key) {
    this.a = [];
    this.key = key;
  }

  get size() {
    return this.a.length;
  }

  push(x) {
    const a = this.a;
    a.push(x);
    let i = a.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.key(a[p]) >= this.key(a[i])) break;
      [a[p], a[i]] = [a[i], a[p]];
      i = p;
    }
  }

  pop() {
    const a = this.a;
    const top = a[0];
    const last = a.pop();
    if (a.length) {
      a[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < a.length && this.key(a[l]) > this.key(a[m])) m = l;
        if (r < a.length && this.key(a[r]) > this.key(a[m])) m = r;
        if (m === i) break;
        [a[m], a[i]] = [a[i], a[m]];
        i = m;
      }
    }
    return top;
  }
}

function toEntries(nets) {
  const entries = (nets instanceof Map ? [...nets] : Object.entries(nets)).filter(([, v]) => v !== 0);
  const sum = entries.reduce((a, [, v]) => a + v, 0);
  if (sum !== 0) throw new Error(`net positions must sum to zero (got ${sum})`);
  // Deterministic order: by user id.
  return entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

export function greedy(nets) {
  const entries = toEntries(nets);
  // Ties broken by user id so the plan is stable across calls.
  const cmp = (x) => x.amount;
  const creditors = new MaxHeap(cmp);
  const debtors = new MaxHeap(cmp);
  for (const [user, v] of entries) (v > 0 ? creditors : debtors).push({ user, amount: Math.abs(v) });

  const transfers = [];
  while (debtors.size && creditors.size) {
    const d = debtors.pop();
    const c = creditors.pop();
    const amount = Math.min(d.amount, c.amount);
    transfers.push({ from_user: d.user, to_user: c.user, amount });
    if (d.amount > amount) debtors.push({ user: d.user, amount: d.amount - amount });
    if (c.amount > amount) creditors.push({ user: c.user, amount: c.amount - amount });
  }
  return transfers;
}

export function optimal(nets) {
  const entries = toEntries(nets);
  const n = entries.length;
  if (n === 0) return [];
  const vals = entries.map(([, v]) => v);
  const full = (1 << n) - 1;

  const sum = new Array(1 << n).fill(0);
  for (let mask = 1; mask <= full; mask++) {
    const low = mask & -mask;
    sum[mask] = sum[mask ^ low] + vals[31 - Math.clz32(low)];
  }
  // dp[mask] = max number of disjoint zero-sum groups covering exactly `mask` (only for zero-sum masks)
  const dp = new Int16Array(1 << n).fill(-1);
  const choice = new Int32Array(1 << n);
  dp[0] = 0;
  for (let mask = 1; mask <= full; mask++) {
    if (sum[mask] !== 0) continue;
    // Peel off one zero-sum submask that contains the lowest set bit (avoids recounting orderings).
    const low = mask & -mask;
    for (let sub = mask; sub; sub = (sub - 1) & mask) {
      if (!(sub & low) || sum[sub] !== 0 || dp[mask ^ sub] < 0) continue;
      if (dp[mask ^ sub] + 1 > dp[mask]) {
        dp[mask] = dp[mask ^ sub] + 1;
        choice[mask] = sub;
      }
    }
  }

  // Reconstruct groups; within each zero-sum group greedy needs exactly k-1 transfers.
  const transfers = [];
  for (let mask = full; mask; mask ^= choice[mask]) {
    const sub = choice[mask];
    const group = new Map();
    for (let i = 0; i < n; i++) if (sub & (1 << i)) group.set(entries[i][0], entries[i][1]);
    transfers.push(...greedy(group));
  }
  return transfers;
}

/** Best plan within budget: exact minimum for small groups, greedy otherwise. */
export function simplify(nets, { optimalMax = 15 } = {}) {
  const nonZero = toEntries(nets).length;
  if (nonZero <= optimalMax) return { algorithm: 'optimal', transfers: optimal(nets) };
  return { algorithm: 'greedy', transfers: greedy(nets) };
}
