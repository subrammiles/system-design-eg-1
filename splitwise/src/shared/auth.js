// User tokens: base64url(JSON {u, e}) + "." + HMAC-SHA256. Stand-in for a real identity
// provider issuing JWTs; the API only needs to know who the caller is.
import { createHmac, timingSafeEqual } from 'node:crypto';

const sign = (data, secret) => createHmac('sha256', secret).update(data).digest('base64url');

export function issueToken(userId, secret, ttlSeconds = 30 * 24 * 3600) {
  const data = Buffer.from(JSON.stringify({ u: userId, e: Math.floor(Date.now() / 1000) + ttlSeconds })).toString('base64url');
  return `${data}.${sign(data, secret)}`;
}

export function verifyToken(token, secret) {
  if (typeof token !== 'string') return null;
  const [data, sig] = token.split('.');
  if (!data || !sig) return null;
  const a = Buffer.from(sign(data, secret));
  const b = Buffer.from(sig);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return null;
  try {
    const { u, e } = JSON.parse(Buffer.from(data, 'base64url').toString());
    return e >= Math.floor(Date.now() / 1000) ? u : null;
  } catch {
    return null;
  }
}
