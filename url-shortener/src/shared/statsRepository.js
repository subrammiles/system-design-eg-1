// Aggregated click analytics in DynamoDB.
//
// click_stats  PK short_code, SK metric
//   metric = "total" | "day#2026-11-26" | "device#mobile" | "browser#chrome" | "country#IN" | "referrer#t.co"
//   attr   count (atomic ADD)
import { QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

export class StatsRepository {
  constructor({ docClient, tables }) {
    this.db = docClient;
    this.tables = tables;
  }

  async increment(shortCode, metric, by = 1) {
    await this.db.send(
      new UpdateCommand({
        TableName: this.tables.clickStats,
        Key: { short_code: shortCode, metric },
        UpdateExpression: 'ADD #c :n',
        ExpressionAttributeNames: { '#c': 'count' },
        ExpressionAttributeValues: { ':n': by },
      }),
    );
  }

  // Keep a denormalized total on the url item too (atomic, no locks).
  async incrementUrlClicks(shortCode, by = 1) {
    await this.db.send(
      new UpdateCommand({
        TableName: this.tables.urls,
        Key: { short_code: shortCode },
        UpdateExpression: 'ADD clicks :n',
        ConditionExpression: 'attribute_exists(short_code)',
        ExpressionAttributeValues: { ':n': by },
      }),
    ).catch((err) => {
      if (err.name !== 'ConditionalCheckFailedException') throw err; // link deleted/expired
    });
  }

  async getStats(shortCode) {
    const out = { total: 0, byDay: {}, byDevice: {}, byBrowser: {}, byCountry: {}, byReferrer: {} };
    const buckets = {
      day: 'byDay', device: 'byDevice', browser: 'byBrowser', country: 'byCountry', referrer: 'byReferrer',
    };
    let ExclusiveStartKey;
    do {
      const res = await this.db.send(
        new QueryCommand({
          TableName: this.tables.clickStats,
          KeyConditionExpression: 'short_code = :c',
          ExpressionAttributeValues: { ':c': shortCode },
          ExclusiveStartKey,
        }),
      );
      for (const { metric, count } of res.Items ?? []) {
        if (metric === 'total') {
          out.total = count;
          continue;
        }
        const i = metric.indexOf('#');
        const bucket = buckets[metric.slice(0, i)];
        if (bucket) out[bucket][metric.slice(i + 1)] = count;
      }
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return out;
  }
}
