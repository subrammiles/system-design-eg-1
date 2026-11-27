// Creates DynamoDB tables and the S3 bucket (idempotent).
import { CreateTableCommand, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { config } from '../src/shared/config.js';
import { createDynamoRaw, createS3, sleep } from '../src/shared/clients.js';

const dynamo = createDynamoRaw();
const s3 = createS3();

async function retry(name, fn, attempts = 30) {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= attempts) throw err;
      console.log(`waiting for ${name} (${err.name})...`);
      await sleep(1000);
    }
  }
}

async function ensureTable(name, key) {
  try {
    await dynamo.send(new DescribeTableCommand({ TableName: name }));
    console.log(`table ${name} exists`);
  } catch (err) {
    if (err.name !== 'ResourceNotFoundException') throw err;
    await dynamo.send(new CreateTableCommand({
      TableName: name,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: key, AttributeType: 'S' }],
      KeySchema: [{ AttributeName: key, KeyType: 'HASH' }],
    }));
    console.log(`created table ${name}`);
  }
}

await retry('DynamoDB', () => ensureTable(config.dynamo.tables.urls, 'url_hash'));
await ensureTable(config.dynamo.tables.contentHashes, 'content_hash');

await retry('S3', async () => {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: config.s3.bucket }));
    console.log(`bucket ${config.s3.bucket} exists`);
  } catch (err) {
    if (err.$metadata?.httpStatusCode !== 404 && err.name !== 'NotFound') throw err;
    await s3.send(new CreateBucketCommand({ Bucket: config.s3.bucket }));
    console.log(`created bucket ${config.s3.bucket}`);
  }
});
