// Content fingerprints for duplicate detection.
//
//   SHA-256  -> exact duplicates (after text normalization)
//   SimHash  -> near duplicates: similar documents get fingerprints with a small Hamming distance
//   Bands    -> LSH candidate lookup: split 64 bits into k+1 bands. Two fingerprints within
//               distance k must share at least one band exactly (pigeonhole), so we only compare
//               against docs sharing a band instead of against every document.
//               k=3 -> 4 x 16-bit bands. Larger k catches looser edits but means narrower bands
//               and much bigger candidate sets.
import { createHash } from 'node:crypto';

const MASK64 = (1n << 64n) - 1n;

// Simple, stable 64-bit hash of a feature (FNV-1a).
function fnv1a64(str) {
  let h = 0xcbf29ce484222325n;
  for (let i = 0; i < str.length; i++) {
    h ^= BigInt(str.charCodeAt(i));
    h = (h * 0x100000001b3n) & MASK64;
  }
  return h;
}

export function normalizeText(text) {
  return text.toLowerCase().replace(/\s+/g, ' ').trim();
}

export const sha256 = (s) => createHash('sha256').update(s).digest('hex');

/** Word 3-shingles as features, weighted by frequency. Returns a 64-bit BigInt. */
export function simhash(text) {
  const words = normalizeText(text).split(' ').filter(Boolean);
  const features = new Map();
  if (words.length < 3) {
    for (const w of words) features.set(w, (features.get(w) ?? 0) + 1);
  } else {
    for (let i = 0; i + 2 < words.length; i++) {
      const f = `${words[i]} ${words[i + 1]} ${words[i + 2]}`;
      features.set(f, (features.get(f) ?? 0) + 1);
    }
  }
  const v = new Array(64).fill(0);
  for (const [f, weight] of features) {
    const h = fnv1a64(f);
    for (let i = 0; i < 64; i++) v[i] += (h >> BigInt(i)) & 1n ? weight : -weight;
  }
  let out = 0n;
  for (let i = 0; i < 64; i++) if (v[i] > 0) out |= 1n << BigInt(i);
  return out;
}

export function hamming(a, b) {
  let x = (a ^ b) & MASK64;
  let n = 0;
  while (x) {
    x &= x - 1n;
    n++;
  }
  return n;
}

/** Split into `count` bands (widths differ by at most 1 bit): ["0:ab12", "1:00ff", ...] */
export function bands(fp, count = 4) {
  const out = [];
  let offset = 0;
  for (let i = 0; i < count; i++) {
    const width = Math.floor(64 / count) + (i < 64 % count ? 1 : 0);
    const mask = (1n << BigInt(width)) - 1n;
    out.push(`${i}:${((fp >> BigInt(offset)) & mask).toString(16)}`);
    offset += width;
  }
  return out;
}

export const toHex = (fp) => fp.toString(16).padStart(16, '0');
export const fromHex = (hex) => BigInt(`0x${hex}`);
