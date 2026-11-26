// Base62 = [0-9a-zA-Z] -> 10 + 26 + 26 = 62 characters.
// Base62 is a 1-to-1 mapping: unique numeric IDs -> unique codes (no collisions).
export const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const BASE = 62n;
const INDEX = new Map([...ALPHABET].map((c, i) => [c, BigInt(i)]));

export function encode(num, minLength = 1) {
  let n = BigInt(num);
  if (n < 0n) throw new RangeError('base62 encode: negative number');
  let out = '';
  do {
    out = ALPHABET[Number(n % BASE)] + out;
    n /= BASE;
  } while (n > 0n);
  return out.padStart(minLength, ALPHABET[0]);
}

export function decode(str) {
  let n = 0n;
  for (const ch of str) {
    const v = INDEX.get(ch);
    if (v === undefined) throw new Error(`base62 decode: invalid char "${ch}"`);
    n = n * BASE + v;
  }
  return n;
}

export const isBase62 = (str) => /^[0-9a-zA-Z]+$/.test(str);
