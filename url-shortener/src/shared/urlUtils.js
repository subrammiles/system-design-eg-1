// URL validation, normalization (dedupe) and custom-alias rules.
import { createHash } from 'node:crypto';

const MAX_URL_LENGTH = 2048;

// Tracking params removed during normalization so the "same" link dedupes.
const TRACKING_PARAMS = new Set([
  'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
  'gclid', 'fbclid', 'mc_cid', 'mc_eid',
]);

// Paths the redirect service owns, or words we never hand out.
export const RESERVED_ALIASES = new Set([
  'admin', 'api', 'login', 'logout', 'signup', 'health', 'metrics', 'static',
  'assets', 'favicon.ico', 'robots.txt', 'shorten', 'qr', 'stats', 'www',
]);

// Minimal offensive-word list; replace with a real moderation list in production.
const BLOCKED_WORDS = ['fuck', 'shit', 'porn'];

export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
  }
}

/**
 * Validate and normalize a long URL so equivalent URLs map to ONE record:
 *   example.com, example.com/, HTTPS://Example.com  ->  https://example.com/
 */
export function normalizeUrl(input, { ownHost } = {}) {
  if (typeof input !== 'string' || input.trim() === '') {
    throw new ValidationError('url is required');
  }
  let raw = input.trim();
  if (raw.length > MAX_URL_LENGTH) throw new ValidationError('url too long');
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) raw = `https://${raw}`;

  let u;
  try {
    u = new URL(raw);
  } catch {
    throw new ValidationError('invalid url');
  }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') {
    throw new ValidationError('only http/https urls are allowed');
  }
  if (!u.hostname.includes('.') && u.hostname !== 'localhost') {
    throw new ValidationError('invalid host');
  }
  // Prevent redirect loops: short.ly/A -> short.ly/B -> short.ly/A
  if (ownHost && u.host.toLowerCase() === ownHost.toLowerCase()) {
    throw new ValidationError('cannot shorten a link that points to this shortener');
  }

  u.hostname = u.hostname.toLowerCase(); // URL() already lowercases, kept explicit
  u.hash = '';
  if ((u.protocol === 'https:' && u.port === '443') || (u.protocol === 'http:' && u.port === '80')) {
    u.port = '';
  }
  for (const key of [...u.searchParams.keys()]) {
    if (TRACKING_PARAMS.has(key.toLowerCase())) u.searchParams.delete(key);
  }
  u.searchParams.sort();
  // Remove trailing slash on non-root paths.
  if (u.pathname.length > 1 && u.pathname.endsWith('/')) {
    u.pathname = u.pathname.replace(/\/+$/, '');
  }
  return u.toString();
}

export const hashUrl = (normalized) => createHash('sha256').update(normalized).digest('hex');

export function validateAlias(alias) {
  if (typeof alias !== 'string') throw new ValidationError('customAlias must be a string');
  if (!/^[a-zA-Z0-9_-]{3,32}$/.test(alias)) {
    throw new ValidationError('customAlias must be 3-32 chars of [a-zA-Z0-9_-]');
  }
  const lower = alias.toLowerCase();
  if (RESERVED_ALIASES.has(lower)) throw new ValidationError('customAlias is reserved');
  if (BLOCKED_WORDS.some((w) => lower.includes(w))) {
    throw new ValidationError('customAlias is not allowed');
  }
  return alias;
}
