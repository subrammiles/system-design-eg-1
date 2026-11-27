// URL normalization, hashing and link filtering (incl. spider-trap heuristics).
//
// Normalization is deliberately conservative: removing query params or changing
// path case can merge pages that are actually different.
import { createHash } from 'node:crypto';

const TRACKING_PARAMS = /^(utm_[a-z]+|gclid|fbclid|msclkid|mc_cid|mc_eid|ref|_ga)$/i;

// Non-HTML resources we never fetch.
const SKIP_EXTENSIONS = new Set([
  'jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'ico', 'bmp', 'css', 'js', 'mjs', 'json', 'xml',
  'pdf', 'zip', 'gz', 'tar', 'rar', '7z', 'exe', 'dmg', 'iso', 'mp3', 'mp4', 'avi', 'mov', 'webm',
  'woff', 'woff2', 'ttf', 'eot', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
]);

// Links a crawler should never follow (side effects / session pages).
const SKIP_PATH = /\/(logout|log-out|signout|sign-out|cart|checkout|login|signin|register)(\/|$)/i;

export function normalizeUrl(input, base) {
  let u;
  try {
    u = base ? new URL(input, base) : new URL(input);
  } catch {
    return null;
  }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') return null; // mailto:, javascript:, tel:
  u.hash = ''; // fragments are never sent to the server
  u.username = '';
  u.password = '';
  if ((u.protocol === 'http:' && u.port === '80') || (u.protocol === 'https:' && u.port === '443')) u.port = '';
  u.hostname = u.hostname.toLowerCase().replace(/\.$/, '');
  u.pathname = u.pathname.replace(/\/{2,}/g, '/');
  if (u.pathname.length > 1 && u.pathname.endsWith('/')) u.pathname = u.pathname.slice(0, -1);
  for (const key of [...u.searchParams.keys()]) if (TRACKING_PARAMS.test(key)) u.searchParams.delete(key);
  u.searchParams.sort(); // ?b=1&a=2 == ?a=2&b=1
  const out = u.toString();
  return out.length > 2048 ? null : out;
}

export const urlHash = (normalized) => createHash('sha256').update(normalized).digest('hex').slice(0, 32);

export const hostOf = (normalized) => new URL(normalized).host; // includes port: "site-a:5000"

/**
 * URL "shape" used to detect generated URL spaces (calendars, faceted filters):
 *   /calendar/2026/11       -> /calendar/{n}/{n}
 *   /filter?color=red&p=3   -> /filter?color=*&p=*
 */
export function urlPattern(normalized) {
  const u = new URL(normalized);
  const path = u.pathname.replace(/\d+/g, '{n}');
  const keys = [...u.searchParams.keys()];
  return keys.length ? `${path}?${keys.map((k) => `${k}=*`).join('&')}` : path;
}

/**
 * Static checks that don't need shared state. Returns a rejection reason or null.
 */
export function rejectReason(normalized, { depth, maxDepth, allowedHosts }) {
  const u = new URL(normalized);
  if (allowedHosts?.length && !allowedHosts.includes(u.host)) return 'out_of_scope';
  if (depth > maxDepth) return 'max_depth';
  const ext = u.pathname.split('.').pop().toLowerCase();
  if (u.pathname.includes('.') && SKIP_EXTENSIONS.has(ext)) return 'non_html';
  if (SKIP_PATH.test(u.pathname)) return 'blocked_path';
  // Spider-trap heuristics.
  const segments = u.pathname.split('/').filter(Boolean);
  if (segments.length > 12) return 'trap_path_depth';
  const counts = new Map();
  for (const s of segments) counts.set(s, (counts.get(s) ?? 0) + 1);
  if ([...counts.values()].some((c) => c >= 3)) return 'trap_repeating_segment'; // /a/b/a/b/a
  if ([...u.searchParams.keys()].length > 6) return 'trap_query_params';
  return null;
}
