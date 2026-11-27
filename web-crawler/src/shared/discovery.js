// URL discovery: every URL (seed, extracted link, redirect target, canonical) goes through
//
//   normalize -> scope/trap filters -> seen? (local LRU -> Bloom filter -> DynamoDB)
//             -> per-host budget + URL-pattern cap -> create PENDING record -> frontier
//
// Bloom filter role: an optimization, NOT the source of truth (false positives would
// silently drop pages). "Definitely new" skips the DB read and goes straight to a
// conditional insert; "maybe seen" is confirmed with a cheap GetItem. Most extracted links
// (nav bars, footers) are already seen, so this turns most checks into a Redis call
// instead of a failed conditional write.
import { createHash } from 'node:crypto';
import { makeItem } from './frontier.js';
import { Status } from './urlRepository.js';
import { hostOf, normalizeUrl, rejectReason, urlHash, urlPattern } from './urls.js';

const BLOOM_LUA = `
  local all = 1
  for i = 1, #ARGV do
    if redis.call('GETBIT', KEYS[1], ARGV[i]) == 0 then all = 0 end
  end
  if all == 0 then
    for i = 1, #ARGV do redis.call('SETBIT', KEYS[1], ARGV[i], 1) end
  end
  return all`;

export function bloomPositions(key, bits, k) {
  const d = createHash('sha256').update(key).digest();
  const h1 = d.readUInt32BE(0);
  const h2 = (d.readUInt32BE(4) | 1) >>> 0; // keep unsigned: `|` yields a signed 32-bit int
  return Array.from({ length: k }, (_, i) => (h1 + i * h2) % bits);
}

class RecentSet {
  constructor(max) {
    this.max = max;
    this.set = new Set();
  }

  has(k) {
    return this.set.has(k);
  }

  add(k) {
    this.set.add(k);
    if (this.set.size > this.max) this.set.delete(this.set.values().next().value);
  }
}

export class Discovery {
  constructor({ redis, repo, frontier, scope, dedupe }) {
    this.redis = redis;
    this.repo = repo;
    this.frontier = frontier;
    this.scope = scope;
    this.dedupe = dedupe;
    this.recent = new RecentSet(50_000);
    if (!redis.bloomCheckAdd) redis.defineCommand('bloomCheckAdd', { numberOfKeys: 1, lua: BLOOM_LUA });
  }

  /** @returns {Promise<{result: string, url?: string, hash?: string}>} */
  async discover({ url, base, depth = 0, priority = 5, source }) {
    const normalized = normalizeUrl(url, base);
    if (!normalized) return this.count('invalid');

    const reason = rejectReason(normalized, { depth, ...this.scope });
    if (reason) return this.count(reason, normalized);

    const hash = urlHash(normalized);
    if (this.recent.has(hash)) return this.count('seen', normalized);
    this.recent.add(hash);

    const positions = bloomPositions(hash, this.dedupe.bloomBits, this.dedupe.bloomHashes);
    const maybeSeen = await this.redis.bloomCheckAdd('bloom:urls', ...positions);
    if (maybeSeen && (await this.repo.exists(hash))) return this.count('seen', normalized);

    const host = hostOf(normalized);
    const [[, patternCount], [, hostCount]] = await this.redis.multi()
      .hincrby(`patterns:${host}`, urlPattern(normalized), 1)
      .incr(`budget:${host}`)
      .exec();
    if (patternCount > this.scope.maxUrlsPerPattern) return this.count('trap_pattern', normalized);
    if (hostCount > this.scope.hostBudget) return this.count('host_budget', normalized);

    const created = await this.repo.insertIfNew({
      url_hash: hash,
      url: normalized,
      host,
      depth,
      priority,
      status: Status.PENDING,
      discovered_at: Date.now(),
      source,
    });
    if (!created) return this.count('seen', normalized);

    await this.frontier.add(makeItem({ h: hash, u: normalized, d: depth, p: priority, host }));
    return { ...(await this.count('queued')), url: normalized, hash };
  }

  async count(result, url) {
    await this.redis.hincrby('stats:discovery', result, 1);
    return { result, url };
  }
}
