// Split Calculator: turns "who shares this expense, and how" into exact integer amounts.
//
//   EQUAL       weights all 1
//   SHARES      weights = shares (up to 2 decimals, e.g. 1.5 nights)
//   PERCENTAGE  weights = basis points; percentages (up to 2 decimals) must total exactly 100
//   EXACT       amounts given; must total exactly the expense
//
// Invariant enforced on every path:  sum(splits) === total
//
// Weighted splits use the largest-remainder method on exact integer math:
//   base_i = floor(total * w_i / W), then the leftover cents go to the largest fractional
//   remainders. Ties (e.g. equal splits) go to the PAYER first, then participant order -
//   so $100 / 3 = 33.34 (payer), 33.33, 33.33. No cent is lost or invented.

export class SplitError extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
    this.code = 'INVALID_SPLIT';
  }
}

export const SPLIT_TYPES = ['EQUAL', 'EXACT', 'PERCENTAGE', 'SHARES'];

/** Parse a non-negative decimal with up to `places` decimals into an integer scaled by 10^places. */
function scaled(value, places, field) {
  const s = typeof value === 'number' ? String(value) : value;
  const re = new RegExp(`^\\d+(\\.\\d{1,${places}})?$`);
  if (typeof s !== 'string' || !re.test(s)) throw new SplitError(`${field} must be a non-negative number with at most ${places} decimals`);
  const [int, frac = ''] = s.split('.');
  return Number(int) * 10 ** places + Number(frac.padEnd(places, '0'));
}

export function allocateByWeights(total, weights, tieOrder) {
  const W = weights.reduce((a, b) => a + BigInt(b), 0n);
  if (W <= 0n) throw new SplitError('weights must not all be zero');
  const T = BigInt(total);
  const parts = weights.map((w, i) => {
    const prod = T * BigInt(w);
    return { i, base: prod / W, rem: prod % W };
  });
  let leftover = Number(T - parts.reduce((a, p) => a + p.base, 0n));
  const order = [...parts].sort((a, b) => (b.rem > a.rem ? 1 : b.rem < a.rem ? -1 : tieOrder[a.i] - tieOrder[b.i]));
  for (const p of order) {
    if (leftover === 0) break;
    if (weights[p.i] === 0) continue; // a zero-weight participant never absorbs a cent
    p.base += 1n;
    leftover--;
  }
  return parts.map((p) => Number(p.base));
}

/**
 * @param {object} input
 * @param {number} input.total            minor units
 * @param {string} input.splitType        EQUAL | EXACT | PERCENTAGE | SHARES
 * @param {string} input.paidBy           payer user id (gets rounding cents first on ties)
 * @param {Array<{user_id: string, amount?: number, percent?: number|string, shares?: number|string}>} input.participants
 * @returns {Array<{user_id: string, amount: number}>}
 */
export function calculateSplits({ total, splitType, paidBy, participants }) {
  if (!Number.isSafeInteger(total) || total <= 0) throw new SplitError('total must be a positive integer (minor units)');
  if (!SPLIT_TYPES.includes(splitType)) throw new SplitError(`split_type must be one of ${SPLIT_TYPES.join(', ')}`);
  if (!Array.isArray(participants) || participants.length === 0) throw new SplitError('participants required');
  const ids = participants.map((p) => p?.user_id);
  if (ids.some((id) => typeof id !== 'string' || !id)) throw new SplitError('every participant needs a user_id');
  if (new Set(ids).size !== ids.length) throw new SplitError('duplicate participant');

  // Payer first on ties, then the order the client sent.
  const tieOrder = ids.map((id, i) => (id === paidBy ? -1 : i));

  let amounts;
  switch (splitType) {
    case 'EQUAL':
      amounts = allocateByWeights(total, ids.map(() => 1), tieOrder);
      break;
    case 'SHARES': {
      const w = participants.map((p) => scaled(p.shares, 2, 'shares'));
      amounts = allocateByWeights(total, w, tieOrder);
      break;
    }
    case 'PERCENTAGE': {
      const bp = participants.map((p) => scaled(p.percent, 2, 'percent'));
      const sum = bp.reduce((a, b) => a + b, 0);
      if (sum !== 10_000) throw new SplitError(`percentages must add up to 100 (got ${sum / 100})`);
      amounts = allocateByWeights(total, bp, tieOrder);
      break;
    }
    case 'EXACT': {
      amounts = participants.map((p) => {
        if (!Number.isSafeInteger(p.amount) || p.amount < 0) throw new SplitError('exact amounts must be non-negative integers (minor units)');
        return p.amount;
      });
      const sum = amounts.reduce((a, b) => a + b, 0);
      if (sum !== total) throw new SplitError(`exact amounts add up to ${sum}, expected ${total} (difference ${total - sum})`);
      break;
    }
    default:
      throw new SplitError('unreachable');
  }

  const splits = ids.map((user_id, i) => ({ user_id, amount: amounts[i] }));
  if (splits.reduce((a, s) => a + s.amount, 0) !== total) throw new Error('split invariant violated'); // defensive
  return splits;
}
