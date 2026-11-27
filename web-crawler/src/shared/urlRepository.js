// Crawl metadata (DynamoDB). Page bodies live in S3, never here.
//
// crawl_urls      PK url_hash | url, host, depth, status, priority, discovered_at,
//                              last_crawled, next_crawl_at, http_status, content_hash, simhash,
//                              s3_key, title, etag, last_modified, recrawl_interval,
//                              change_count, fetch_count, duplicate_of, canonical_url, redirect_to, error
// content_hashes  PK content_hash | url_hash (first URL seen with this exact content)
import { GetCommand, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

export const Status = {
  PENDING: 'PENDING',
  FETCHED: 'FETCHED',
  NOT_MODIFIED: 'NOT_MODIFIED',
  DUPLICATE: 'DUPLICATE',
  REDIRECTED: 'REDIRECTED',
  BLOCKED_ROBOTS: 'BLOCKED_ROBOTS',
  NOT_HTML: 'NOT_HTML',
  GONE: 'GONE',
  FAILED: 'FAILED',
  RETRYING: 'RETRYING',
};

export class UrlRepository {
  constructor(docClient, tables) {
    this.db = docClient;
    this.tables = tables;
  }

  /** Authoritative "seen" check: creates the record only if the URL is new. */
  async insertIfNew(record) {
    try {
      await this.db.send(new PutCommand({
        TableName: this.tables.urls,
        Item: record,
        ConditionExpression: 'attribute_not_exists(url_hash)',
      }));
      return true;
    } catch (err) {
      if (err.name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  async get(hash) {
    const res = await this.db.send(new GetCommand({ TableName: this.tables.urls, Key: { url_hash: hash } }));
    return res.Item ?? null;
  }

  async exists(hash) {
    const res = await this.db.send(new GetCommand({
      TableName: this.tables.urls,
      Key: { url_hash: hash },
      ProjectionExpression: 'url_hash',
    }));
    return Boolean(res.Item);
  }

  /** SET the given fields (undefined skipped); optional ADD counters. */
  async update(hash, fields, add = {}) {
    const names = {};
    const values = {};
    const sets = [];
    const adds = [];
    for (const [k, v] of Object.entries(fields)) {
      if (v === undefined) continue;
      names[`#${k}`] = k;
      values[`:${k}`] = v;
      sets.push(`#${k} = :${k}`);
    }
    for (const [k, v] of Object.entries(add)) {
      names[`#${k}`] = k;
      values[`:${k}`] = v;
      adds.push(`#${k} :${k}`);
    }
    if (!sets.length && !adds.length) return;
    await this.db.send(new UpdateCommand({
      TableName: this.tables.urls,
      Key: { url_hash: hash },
      UpdateExpression: [sets.length && `SET ${sets.join(', ')}`, adds.length && `ADD ${adds.join(', ')}`]
        .filter(Boolean).join(' '),
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    }));
  }

  /**
   * Exact-duplicate registry. Returns the url_hash that owns this content
   * (== ownerHash if we are first).
   */
  async claimContent(contentHash, ownerHash) {
    try {
      await this.db.send(new PutCommand({
        TableName: this.tables.contentHashes,
        Item: { content_hash: contentHash, url_hash: ownerHash },
        ConditionExpression: 'attribute_not_exists(content_hash)',
      }));
      return ownerHash;
    } catch (err) {
      if (err.name !== 'ConditionalCheckFailedException') throw err;
      const res = await this.db.send(new GetCommand({
        TableName: this.tables.contentHashes,
        Key: { content_hash: contentHash },
      }));
      return res.Item.url_hash;
    }
  }
}
