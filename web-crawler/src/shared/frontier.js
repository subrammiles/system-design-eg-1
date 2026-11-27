// Distributed URL Frontier + domain-aware scheduler (Mercator-style), in Redis.
//
// Instead of one physical queue per website (100M websites = unmanageable) or one
// global FIFO (no politeness), we keep:
//
//   fq:{host}    ZSET  per-host queue of URL items, score = priority then FIFO
//   hosts:due    ZSET  host -> time (ms) at which it may be fetched again   <- the "clock icons"
//   host:{host}  HASH  next (ms), leased (0/1), delay (ms), backoff (n)
//   inflight     ZSET  leases (host + item) -> lease expiry                  <- crash recovery
//   delayed      ZSET  retry items -> retry-at time
//   recrawl      ZSET  known URLs -> next crawl time
//
// A fetcher calls lease(): atomically pick a host whose time has come, pop its best URL
// and mark the host leased. While leased, NO other worker can fetch from that host. When
// the fetch completes, release() sets the host's next time = now + crawl delay. So politeness
// holds across all workers, and delay is measured from completion (slow sites get fewer
// concurrent requests, not more). If a worker dies, the lease expires and the janitor puts
// the URL back.
//
// Lua keeps every step atomic. Note: keys are built inside scripts, so this targets a single
// Redis primary; at larger scale, shard hosts across N Redis instances by hash(host).

const SCORE_PRIORITY_STEP = 1e13; // > any epoch-ms, so priority dominates, then FIFO

const LUA_PUSH_ITEM = `
local function push(member, host, priority, now)
  local added = redis.call('ZADD', 'fq:' .. host, 'NX', (10 - priority) * ${SCORE_PRIORITY_STEP} + now, member)
  if added == 1 then redis.call('HINCRBY', 'stats', 'queued', 1) end
  if redis.call('HGET', 'host:' .. host, 'leased') ~= '1' and not redis.call('ZSCORE', 'hosts:due', host) then
    local nxt = tonumber(redis.call('HGET', 'host:' .. host, 'next') or '0')
    redis.call('ZADD', 'hosts:due', math.max(nxt, now), host)
  end
  return added
end
`;

const SCRIPTS = {
  // ARGV: member, host, priority, now
  frontierAdd: {
    numberOfKeys: 0,
    lua: `${LUA_PUSH_ITEM}
      return push(ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]))`,
  },

  // ARGV: now, leaseTimeoutMs  -> {host, item, lease} | nil
  frontierLease: {
    numberOfKeys: 0,
    lua: `
      if redis.call('EXISTS', 'crawl:paused') == 1 then return nil end
      local now = tonumber(ARGV[1])
      for _ = 1, 20 do
        local due = redis.call('ZRANGEBYSCORE', 'hosts:due', '-inf', now, 'LIMIT', 0, 1)
        if #due == 0 then return nil end
        local host = due[1]
        redis.call('ZREM', 'hosts:due', host)
        local popped = redis.call('ZPOPMIN', 'fq:' .. host)
        if #popped > 0 then
          redis.call('HINCRBY', 'stats', 'queued', -1)
          redis.call('HSET', 'host:' .. host, 'leased', '1')
          local lease = cjson.encode({host = host, item = popped[1], t = now})
          redis.call('ZADD', 'inflight', now + tonumber(ARGV[2]), lease)
          return {host, popped[1], lease}
        end
      end
      return nil`,
  },

  // ARGV: host, lease, now, delayMs
  frontierRelease: {
    numberOfKeys: 0,
    lua: `
      local host, now, delay = ARGV[1], tonumber(ARGV[3]), tonumber(ARGV[4])
      redis.call('ZREM', 'inflight', ARGV[2])
      redis.call('HSET', 'host:' .. host, 'leased', '0', 'next', now + delay)
      if redis.call('ZCARD', 'fq:' .. host) > 0 then
        redis.call('ZADD', 'hosts:due', now + delay, host)
      end
      return 1`,
  },

  // Move due members of a ZSET (delayed retries / recrawls) into the frontier.
  // KEYS[1] = source zset; ARGV: now, limit, priorityOverride(-1 = keep item priority)
  frontierPromote: {
    numberOfKeys: 1,
    lua: `${LUA_PUSH_ITEM}
      local now = tonumber(ARGV[1])
      local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
      for _, member in ipairs(due) do
        redis.call('ZREM', KEYS[1], member)
        local item = cjson.decode(member)
        local p = tonumber(ARGV[3])
        if p < 0 then p = item.p end
        push(member, item.host, p, now)
      end
      return #due`,
  },

  // Re-queue URLs whose fetcher died mid-fetch. ARGV: now, limit
  frontierReclaim: {
    numberOfKeys: 0,
    lua: `${LUA_PUSH_ITEM}
      local now = tonumber(ARGV[1])
      local expired = redis.call('ZRANGEBYSCORE', 'inflight', '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
      for _, lease in ipairs(expired) do
        redis.call('ZREM', 'inflight', lease)
        local l = cjson.decode(lease)
        redis.call('HSET', 'host:' .. l.host, 'leased', '0')
        push(l.item, l.host, 10, now)
      end
      return #expired`,
  },
};

export function registerFrontierScripts(redis) {
  for (const [name, { numberOfKeys, lua }] of Object.entries(SCRIPTS)) {
    if (!redis[name]) redis.defineCommand(name, { numberOfKeys, lua });
  }
}

/** Frontier item: h=url hash, u=url, d=depth, p=priority(0-10), r=retries, host */
export const makeItem = ({ h, u, d, p, r = 0, host }) => JSON.stringify({ h, u, d, p, r, host });

export class Frontier {
  constructor(redis, { leaseTimeoutMs }) {
    registerFrontierScripts(redis);
    this.redis = redis;
    this.leaseTimeoutMs = leaseTimeoutMs;
  }

  add(item) {
    const obj = JSON.parse(item);
    return this.redis.frontierAdd(item, obj.host, obj.p, Date.now());
  }

  async lease() {
    const res = await this.redis.frontierLease(Date.now(), this.leaseTimeoutMs);
    if (!res) return null;
    const [host, item, lease] = res;
    return { host, item: JSON.parse(item), lease };
  }

  release(host, lease, delayMs) {
    return this.redis.frontierRelease(host, lease, Date.now(), Math.round(delayMs));
  }

  retryLater(itemObj, atMs) {
    return this.redis.zadd('delayed', atMs, makeItem({ ...itemObj, r: (itemObj.r ?? 0) + 1 }));
  }

  scheduleRecrawl(itemObj, atMs) {
    // Stable member per URL (r=0) so rescheduling only moves its score.
    return this.redis.zadd('recrawl', atMs, makeItem({ ...itemObj, r: 0 }));
  }

  promoteDue(key, { limit = 500, priority = -1 } = {}) {
    return this.redis.frontierPromote(key, Date.now(), limit, priority);
  }

  reclaimExpired(limit = 500) {
    return this.redis.frontierReclaim(Date.now(), limit);
  }

  pause() {
    return this.redis.set('crawl:paused', '1');
  }

  resume() {
    return this.redis.del('crawl:paused');
  }

  async hostState(host) {
    const [state, queued, due] = await Promise.all([
      this.redis.hgetall(`host:${host}`),
      this.redis.zcard(`fq:${host}`),
      this.redis.zscore('hosts:due', host),
    ]);
    return { ...state, queued, dueAt: due ? Number(due) : null };
  }
}
