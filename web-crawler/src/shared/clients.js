// Infrastructure clients: Redis (frontier, scheduler state, streams, bloom filter),
// DynamoDB (URL metadata, content fingerprints), S3 (raw pages).
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { S3Client } from '@aws-sdk/client-s3';
import Redis from 'ioredis';
import { config } from './config.js';

export function createRedis() {
  const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 3 });
  redis.on('error', (err) => console.error('[redis]', err.message));
  return redis;
}

export function createDynamoRaw() {
  const local = Boolean(config.dynamo.endpoint);
  return new DynamoDBClient({
    region: config.dynamo.region,
    ...(local && {
      endpoint: config.dynamo.endpoint,
      credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    }),
  });
}

export const createDynamo = () =>
  DynamoDBDocumentClient.from(createDynamoRaw(), { marshallOptions: { removeUndefinedValues: true } });

export function createS3() {
  const local = Boolean(config.s3.endpoint);
  return new S3Client({
    region: config.dynamo.region,
    ...(local && {
      endpoint: config.s3.endpoint,
      forcePathStyle: true,
      credentials: { accessKeyId: config.s3.accessKeyId, secretAccessKey: config.s3.secretAccessKey },
    }),
  });
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
