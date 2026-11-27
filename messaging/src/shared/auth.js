// Device tokens: base64url("userId.deviceId.expiry") + "." + HMAC-SHA256 signature.
// Stand-in for a real identity provider (phone OTP / OAuth + JWT); the gateway only needs
// to know which user and device a socket belongs to.
import { createHmac, timingSafeEqual } from 'node:crypto';

const sign = (data, secret) => createHmac('sha256', secret).update(data).digest('base64url');

export function issueToken({ userId, deviceId }, secret, ttlSeconds = 30 * 24 * 3600) {
  const data = Buffer.from(JSON.stringify({ u: userId, d: deviceId, e: Math.floor(Date.now() / 1000) + ttlSeconds }))
    .toString('base64url');
  return `${data}.${sign(data, secret)}`;
}

export function verifyToken(token, secret) {
  if (typeof token !== 'string') return null;
  const [data, sig] = token.split('.');
  if (!data || !sig) return null;
  const expected = Buffer.from(sign(data, secret));
  const given = Buffer.from(sig);
  if (expected.length !== given.length || !timingSafeEqual(expected, given)) return null;
  try {
    const { u, d, e } = JSON.parse(Buffer.from(data, 'base64url').toString());
    if (e < Math.floor(Date.now() / 1000)) return null;
    return { userId: u, deviceId: d };
  } catch {
    return null;
  }
}
