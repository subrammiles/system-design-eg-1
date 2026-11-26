// Data access for the `urls` and `url_hash` tables (DynamoDB).
//
// urls       PK short_code  | long_url, created_at, expires_at (TTL attr), is_custom, clicks, status
// url_hash   PK url_hash    | short_code, expires_at   (dedupe: same long URL -> same short URL)
import { GetCommand, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

export class AlreadyExistsError extends Error {}

export class UrlRepository {
  constructor({ docClient, tables }) {
    this.db = docClient;
    this.tables = tables;
  }

  // O(1) key lookup: short_code -> hash -> partition. No scan.
  async getByCode(code) {
    const res = await this.db.send(
      new GetCommand({ TableName: this.tables.urls, Key: { short_code: code } }),
    );
    return res.Item ?? null;
  }

  /**
   * Conditional insert: fails if the short_code already exists. This is what
   * guarantees a generated code never overwrites a custom alias (or vice versa).
   */
  async insert(item) {
    try {
      await this.db.send(
        new PutCommand({
          TableName: this.tables.urls,
          Item: item,
          ConditionExpression: 'attribute_not_exists(short_code)',
        }),
      );
    } catch (err) {
      if (err.name === 'ConditionalCheckFailedException') throw new AlreadyExistsError(item.short_code);
      throw err;
    }
  }

  async getByHash(urlHash) {
    const res = await this.db.send(
      new GetCommand({ TableName: this.tables.urlHash, Key: { url_hash: urlHash } }),
    );
    return res.Item ?? null;
  }

  /** Returns true if this call claimed the hash, false if someone else already had it. */
  async claimHash(urlHash, shortCode, expiresAt) {
    try {
      await this.db.send(
        new PutCommand({
          TableName: this.tables.urlHash,
          Item: { url_hash: urlHash, short_code: shortCode, expires_at: expiresAt },
          ConditionExpression: 'attribute_not_exists(url_hash)',
        }),
      );
      return true;
    } catch (err) {
      if (err.name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  async disable(code, reason) {
    await this.db.send(
      new UpdateCommand({
        TableName: this.tables.urls,
        Key: { short_code: code },
        UpdateExpression: 'SET #s = :s, disabled_reason = :r',
        ConditionExpression: 'attribute_exists(short_code)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':s': 'disabled', ':r': reason },
      }),
    );
  }
}

export const nowSeconds = () => Math.floor(Date.now() / 1000);

// DynamoDB TTL deletion is eventual (can lag), so always check at read time too.
export const isExpired = (record) => Boolean(record.expires_at && record.expires_at <= nowSeconds());
