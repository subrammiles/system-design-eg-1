// Content deduplication, layered (never compare a page against every stored page):
//
//   1. SHA-256 of normalized text -> exact duplicate?   (DynamoDB conditional put, O(1))
//   2. SimHash (64-bit)           -> near duplicate?
//      LSH bands in Redis         -> candidate set (docs sharing >= 1 of k+1 bands)
//      Hamming distance <= k      -> verify candidates
//
//   lsh:{n}:{band}  SET   url_hashes having this band value (n = number of bands)
//   simhash:fp      HASH  url_hash -> fingerprint hex
//   simhash:seq     HASH  url_hash -> insertion sequence number
//
// Race safety: two near-identical pages parsed at the same moment must not both come out
// "unique". Each page first indexes itself with an increasing sequence number, THEN looks
// for candidates, and only treats candidates with a LOWER sequence as the original. Of two
// concurrent pages, exactly one (the earlier) stays unique. A page that turns out to be a
// duplicate removes itself from the index, so later pages match the original instead.
import { bands, fromHex, hamming, normalizeText, sha256, simhash, toHex } from '../shared/simhash.js';

const MIN_WORDS_FOR_SIMHASH = 30; // tiny pages produce noisy fingerprints

export class ContentDedupe {
  constructor({ redis, repo, maxDistance }) {
    this.redis = redis;
    this.repo = repo;
    this.maxDistance = maxDistance;
    this.bandCount = maxDistance + 1;
  }

  bandKeys(fp) {
    return bands(fp, this.bandCount).map((b) => `lsh:${this.bandCount}:${b}`);
  }

  /**
   * @returns {{contentHash, simhash, duplicateOf: string|null, kind: 'exact'|'near'|null, distance?}}
   */
  async check(urlHash, text, previousSimhash) {
    const normalized = normalizeText(text);
    const contentHash = sha256(normalized);

    const owner = await this.repo.claimContent(contentHash, urlHash);
    if (owner !== urlHash) return { contentHash, simhash: null, duplicateOf: owner, kind: 'exact' };

    if (normalized.split(' ').length < MIN_WORDS_FOR_SIMHASH) {
      return { contentHash, simhash: null, duplicateOf: null, kind: null };
    }

    const fp = simhash(normalized);
    const fpHex = toHex(fp);
    const myKeys = this.bandKeys(fp);

    // 1. Index ourselves first (replacing our old fingerprint on recrawl).
    const seq = await this.redis.incr('simhash:seq:counter');
    const tx = this.redis.multi();
    if (previousSimhash && previousSimhash !== fpHex) {
      for (const k of this.bandKeys(fromHex(previousSimhash))) tx.srem(k, urlHash);
    }
    for (const k of myKeys) tx.sadd(k, urlHash);
    tx.hset('simhash:fp', urlHash, fpHex);
    tx.hset('simhash:seq', urlHash, seq);
    await tx.exec();

    // 2. Candidates = docs sharing a band; only earlier-indexed docs can be "the original".
    const sets = await Promise.all(myKeys.map((k) => this.redis.smembers(k)));
    const candidates = [...new Set(sets.flat())].filter((h) => h !== urlHash);
    let best = null;
    if (candidates.length) {
      const [fps, seqs] = await Promise.all([
        this.redis.hmget('simhash:fp', ...candidates),
        this.redis.hmget('simhash:seq', ...candidates),
      ]);
      candidates.forEach((h, i) => {
        if (!fps[i] || !seqs[i] || Number(seqs[i]) >= seq) return;
        const d = hamming(fp, fromHex(fps[i]));
        if (d <= this.maxDistance && (!best || d < best.distance)) best = { h, distance: d };
      });
    }
    if (!best) return { contentHash, simhash: fpHex, duplicateOf: null, kind: null };

    // 3. We're a near duplicate: leave the index so others match the original, not us.
    const undo = this.redis.multi();
    for (const k of myKeys) undo.srem(k, urlHash);
    undo.hdel('simhash:fp', urlHash).hdel('simhash:seq', urlHash);
    await undo.exec();
    return { contentHash, simhash: fpHex, duplicateOf: best.h, kind: 'near', distance: best.distance };
  }
}
