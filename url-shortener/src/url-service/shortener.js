// URL Shortening flow (Create Short URL) - business logic, independent of HTTP.
//
//   validate + normalize URL
//     -> dedupe: hash(long_url) already mapped?  -> return existing short URL
//     -> custom alias?  conditional insert (409 if taken)
//     -> else range-allocated ID -> scramble -> Base62 -> conditional insert
//     -> write-through to Redis cache
//     -> async safety check (phishing/malware) -> disable link if bad
import { AlreadyExistsError, isExpired, nowSeconds } from '../shared/urlRepository.js';
import { hashUrl, normalizeUrl, validateAlias, ValidationError } from '../shared/urlUtils.js';

export class ConflictError extends Error {
  constructor(message) {
    super(message);
    this.status = 409;
  }
}

const MAX_EXPIRY_SECONDS = 5 * 365 * 24 * 60 * 60;
const MAX_INSERT_ATTEMPTS = 5;

export class Shortener {
  constructor({ repo, idGenerator, cache, safetyChecker, baseUrl }) {
    this.repo = repo;
    this.idGenerator = idGenerator;
    this.cache = cache;
    this.safetyChecker = safetyChecker;
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.ownHost = new URL(this.baseUrl).host;
  }

  shortUrl(code) {
    return `${this.baseUrl}/${code}`;
  }

  async shorten({ url, customAlias, expiresInSeconds, userId }) {
    const longUrl = normalizeUrl(url, { ownHost: this.ownHost });
    const expiresAt = parseExpiry(expiresInSeconds);

    // Dedupe only plain links: aliases and expiring links have their own semantics.
    const dedupe = !customAlias && !expiresAt;
    const urlHash = hashUrl(longUrl);
    if (dedupe) {
      const existing = await this.findExisting(urlHash);
      if (existing) return { ...this.toResponse(existing), created: false };
    }

    const base = {
      long_url: longUrl,
      created_at: nowSeconds(),
      expires_at: expiresAt ?? undefined, // DynamoDB TTL attribute
      user_id: userId,
      clicks: 0,
      status: 'active',
    };

    let item;
    if (customAlias) {
      item = { ...base, short_code: validateAlias(customAlias), is_custom: true };
      try {
        await this.repo.insert(item);
      } catch (err) {
        if (err instanceof AlreadyExistsError) throw new ConflictError('customAlias already taken');
        throw err;
      }
    } else {
      item = await this.insertGenerated(base);
      if (dedupe) {
        // If two requests race on the same URL, both get a valid link; only one owns the hash.
        await this.repo.claimHash(urlHash, item.short_code);
      }
    }

    await this.cache.set(item.short_code, item);
    this.scheduleSafetyCheck(item);
    return { ...this.toResponse(item), created: true };
  }

  async findExisting(urlHash) {
    const mapping = await this.repo.getByHash(urlHash);
    if (!mapping) return null;
    const record = await this.repo.getByCode(mapping.short_code);
    if (!record || isExpired(record) || record.status !== 'active') return null;
    return record;
  }

  async insertGenerated(base) {
    // A generated code can only collide with a custom alias of the same shape;
    // the conditional write detects it and we simply take the next ID.
    for (let attempt = 1; attempt <= MAX_INSERT_ATTEMPTS; attempt++) {
      const item = { ...base, short_code: await this.idGenerator.nextCode(), is_custom: false };
      try {
        await this.repo.insert(item);
        return item;
      } catch (err) {
        if (!(err instanceof AlreadyExistsError)) throw err;
      }
    }
    throw new Error('could not allocate a unique short code');
  }

  scheduleSafetyCheck(item) {
    if (!this.safetyChecker) return;
    // Async: never slows down the create request.
    setImmediate(async () => {
      try {
        const verdict = await this.safetyChecker.check(item.long_url);
        if (!verdict.safe) {
          await this.repo.disable(item.short_code, verdict.reason);
          await this.cache.invalidate(item.short_code);
          console.warn(`[safety] disabled ${item.short_code}: ${verdict.reason}`);
        }
      } catch (err) {
        console.error('[safety] check failed:', err.message);
      }
    });
  }

  toResponse(item) {
    return {
      short_code: item.short_code,
      short_url: this.shortUrl(item.short_code),
      long_url: item.long_url,
      expires_at: item.expires_at ?? null,
      is_custom: Boolean(item.is_custom),
    };
  }
}

function parseExpiry(expiresInSeconds) {
  if (expiresInSeconds == null) return null;
  const n = Number(expiresInSeconds);
  if (!Number.isInteger(n) || n <= 0 || n > MAX_EXPIRY_SECONDS) {
    throw new ValidationError(`expiresInSeconds must be an integer in 1..${MAX_EXPIRY_SECONDS}`);
  }
  return nowSeconds() + n;
}

// Placeholder for Google Safe Browsing / internal threat-intel lookups.
export class DomainBlocklistChecker {
  constructor(blockedDomains = []) {
    this.blocked = new Set(blockedDomains.map((d) => d.toLowerCase()));
  }

  async check(longUrl) {
    const host = new URL(longUrl).hostname;
    const parts = host.split('.');
    for (let i = 0; i < parts.length - 1; i++) {
      if (this.blocked.has(parts.slice(i).join('.'))) return { safe: false, reason: 'blocked domain' };
    }
    return { safe: true };
  }
}
