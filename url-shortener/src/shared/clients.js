// Shared infrastructure clients: DynamoDB (primary store) and Redis (cache, rate limit, stream).
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import Redis from 'ioredis';
import { config } from './config.js';

export function createDynamo() {
  const local = Boolean(config.dynamo.endpoint);
  const client = new DynamoDBClient({
    region: config.dynamo.region,
    ...(local && {
      endpoint: config.dynamo.endpoint,
      credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    }),
  });
  return DynamoDBDocumentClient.from(client, {
    marshallOptions: { removeUndefinedValues: true },
  });
}

export function createRedis() {
  const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 2, enableOfflineQueue: true });
  redis.on('error', (err) => console.error('[redis]', err.message));
  return redis;
}
