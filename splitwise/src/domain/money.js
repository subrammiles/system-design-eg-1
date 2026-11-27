// Money is always an integer number of MINOR units (cents, paise). Never floats:
// 0.1 + 0.2 !== 0.3, and a ledger that loses or invents a cent is wrong.

export const CURRENCY_EXPONENT = { USD: 2, EUR: 2, GBP: 2, INR: 2, AUD: 2, CAD: 2, SGD: 2, JPY: 0, KRW: 0 };

export class MoneyError extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
    this.code = 'INVALID_AMOUNT';
  }
}

export function assertCurrency(c) {
  if (!(c in CURRENCY_EXPONENT)) throw new MoneyError(`unsupported currency ${c}`);
  return c;
}

export function assertMinor(amount, { max = 1e13, allowZero = false, field = 'amount' } = {}) {
  if (!Number.isSafeInteger(amount)) throw new MoneyError(`${field} must be an integer number of minor units (e.g. cents)`);
  if (amount < 0 || (!allowZero && amount === 0)) throw new MoneyError(`${field} must be positive`);
  if (amount > max) throw new MoneyError(`${field} too large`);
  return amount;
}

export function format(amount, currency) {
  const exp = CURRENCY_EXPONENT[currency] ?? 2;
  const sign = amount < 0 ? '-' : '';
  const abs = Math.abs(amount);
  if (exp === 0) return `${sign}${abs} ${currency}`;
  const s = String(abs).padStart(exp + 1, '0');
  return `${sign}${s.slice(0, -exp)}.${s.slice(-exp)} ${currency}`;
}

/**
 * Convert an amount between currencies with a decimal exchange rate given as a string
 * ("83.125" = 1 unit of `from` is 83.125 units of `to`). Exact BigInt math, half-up rounding.
 */
export function convert(amountMinor, from, to, rate) {
  assertCurrency(from);
  assertCurrency(to);
  if (from === to) return amountMinor;
  if (typeof rate !== 'string' || !/^\d+(\.\d{1,8})?$/.test(rate) || Number(rate) <= 0) {
    throw new MoneyError('exchange_rate must be a positive decimal string with up to 8 decimals');
  }
  const [int, frac = ''] = rate.split('.');
  const rateScaled = BigInt(int + frac.padEnd(8, '0')); // rate * 1e8
  const expDiff = CURRENCY_EXPONENT[to] - CURRENCY_EXPONENT[from];
  let num = BigInt(amountMinor) * rateScaled;
  let den = 10n ** 8n;
  if (expDiff > 0) num *= 10n ** BigInt(expDiff);
  else den *= 10n ** BigInt(-expDiff);
  const result = (num * 2n + den) / (2n * den); // round half up
  return Number(result);
}
