// Two-level cache for the read path.
//
//   L1: in-process LRU (handles viral "hot keys" without even hitting Redis)
//   L2: Redis (shared by all redirect servers, TTL 24h)
//   miss -> DynamoDB -> populate Redis + L1
//
// Cached value = JSON { long_url, expires_at } so expiry can be checked without the DB.
// A "not found" result is cached briefly (negative caching) so random/scanned codes
// don't hammer the database.

export const NOT_FOUND = Symbol('NOT_FOUND');
const NEGATIVE_MARKER = '__nf__';

export class LocalLru {
  constructor({ maxEntries, ttlMs }) {
    this.max = maxEntries;
    this.ttlMs = ttlMs;
    this.map = new Map();
  }

  get(key) {
    const hit = this.map.get(key);
    if (!hit) return undefined;
    if (hit.expiresAt < Date.now()) {
      this.map.delete(key);
      return undefined;
    }
    this.map.delete(key); // move to most-recent
    this.map.set(key, hit);
    return hit.value;
  }

  set(key, value, ttlMs = this.ttlMs) {
    this.map.delete(key);
    this.map.set(key, { value, expiresAt: Date.now() + ttlMs });
    if (this.map.size > this.max) this.map.delete(this.map.keys().next().value);
  }

  delete(key) {
    this.map.delete(key);
  }
}

export class UrlCache {
  constructor({ redis, local, redisTtlSeconds, negativeTtlSeconds }) {
    this.redis = redis;
    this.local = local;
    this.redisTtlSeconds = redisTtlSeconds;
    this.negativeTtlSeconds = negativeTtlSeconds;
  }

  static key(code) {
    return `url:${code}`;
  }

  /** @returns {Promise<object|typeof NOT_FOUND|undefined>} undefined = cache miss */
  async get(code) {
    const l1 = this.local?.get(code);
    if (l1 !== undefined) return l1;

    let raw;
    try {
      raw = await this.redis.get(UrlCache.key(code));
    } catch (err) {
      console.error('[cache] redis get failed, falling back to DB:', err.message);
      return undefined; // cache is an optimization - never fail the request because of it
    }
    if (raw == null) return undefined;
    const value = raw === NEGATIVE_MARKER ? NOT_FOUND : JSON.parse(raw);
    this.local?.set(code, value, value === NOT_FOUND ? this.negativeTtlSeconds * 1000 : undefined);
    return value;
  }

  async set(code, record) {
    const value = { long_url: record.long_url, expires_at: record.expires_at ?? null };
    // Never cache past the link's own expiry.
    let ttl = this.redisTtlSeconds;
    if (value.expires_at) {
      ttl = Math.min(ttl, value.expires_at - Math.floor(Date.now() / 1000));
      if (ttl <= 0) return;
    }
    this.local?.set(code, value, Math.min(ttl * 1000, this.local.ttlMs));
    try {
      await this.redis.set(UrlCache.key(code), JSON.stringify(value), 'EX', ttl);
    } catch (err) {
      console.error('[cache] redis set failed:', err.message);
    }
  }

  async setNotFound(code) {
    this.local?.set(code, NOT_FOUND, this.negativeTtlSeconds * 1000);
    try {
      await this.redis.set(UrlCache.key(code), NEGATIVE_MARKER, 'EX', this.negativeTtlSeconds);
    } catch (err) {
      console.error('[cache] redis set failed:', err.message);
    }
  }

  async invalidate(code) {
    this.local?.delete(code);
    try {
      await this.redis.del(UrlCache.key(code));
    } catch (err) {
      console.error('[cache] redis del failed:', err.message);
    }
  }
}
