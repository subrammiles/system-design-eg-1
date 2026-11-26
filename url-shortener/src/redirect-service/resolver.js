// URL Redirection flow - business logic.
//
//   L1 local cache -> L2 Redis -> DynamoDB (on miss) -> populate caches
//   expired / disabled / missing -> null
import { NOT_FOUND } from '../shared/cache.js';
import { isExpired } from '../shared/urlRepository.js';

export class Resolver {
  constructor({ cache, repo }) {
    this.cache = cache;
    this.repo = repo;
    this.inflight = new Map(); // request coalescing: one DB read per code under a stampede
  }

  /** @returns {Promise<{long_url: string, expires_at: number|null} | null>} */
  async resolve(code) {
    const cached = await this.cache.get(code);
    if (cached === NOT_FOUND) return null;
    if (cached) return isExpired(cached) ? null : cached;

    let p = this.inflight.get(code);
    if (!p) {
      p = this.loadFromDb(code).finally(() => this.inflight.delete(code));
      this.inflight.set(code, p);
    }
    return p;
  }

  async loadFromDb(code) {
    const item = await this.repo.getByCode(code);
    if (!item || item.status !== 'active' || isExpired(item)) {
      await this.cache.setNotFound(code);
      return null;
    }
    await this.cache.set(code, item);
    return { long_url: item.long_url, expires_at: item.expires_at ?? null };
  }
}
