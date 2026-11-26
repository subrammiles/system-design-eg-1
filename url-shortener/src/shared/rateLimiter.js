// Fixed-window rate limiter in Redis (e.g. 100 requests / minute / IP).
// INCR + EXPIRE run in one MULTI so the key always gets a TTL.
export function rateLimiter({ redis, windowSeconds, maxRequests, keyPrefix = 'rl' }) {
  return async (req, res, next) => {
    const window = Math.floor(Date.now() / 1000 / windowSeconds);
    const key = `${keyPrefix}:${req.ip}:${window}`;
    try {
      const [[, count]] = await redis.multi().incr(key).expire(key, windowSeconds).exec();
      res.setHeader('X-RateLimit-Limit', maxRequests);
      res.setHeader('X-RateLimit-Remaining', Math.max(0, maxRequests - count));
      if (count > maxRequests) {
        res.setHeader('Retry-After', windowSeconds);
        return res.status(429).json({ error: 'rate limit exceeded' });
      }
    } catch (err) {
      // Fail open: a Redis outage should not take the write API down.
      console.error('[rate-limit] redis error, allowing request:', err.message);
    }
    next();
  };
}
