// Conversation membership with a Redis read-through cache. Membership changes rarely and
// every message needs it, so caching it keeps PostgreSQL off the hot path.
export class Membership {
  constructor({ pg, redis, ttlSeconds = 300 }) {
    this.pg = pg;
    this.redis = redis;
    this.ttl = ttlSeconds;
  }

  async members(conversationId) {
    const key = `members:${conversationId}`;
    const cached = await this.redis.smembers(key);
    if (cached.length) return cached;
    const { rows } = await this.pg.query('SELECT user_id FROM members WHERE conversation_id = $1', [conversationId]);
    const ids = rows.map((r) => r.user_id);
    if (ids.length) await this.redis.multi().sadd(key, ...ids).expire(key, this.ttl).exec();
    return ids;
  }

  invalidate(conversationId) {
    return this.redis.del(`members:${conversationId}`);
  }
}
