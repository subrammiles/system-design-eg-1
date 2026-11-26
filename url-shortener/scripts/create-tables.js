// Creates the DynamoDB tables (idempotent) and enables TTL on expires_at.
import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import { config } from '../src/shared/config.js';

const local = Boolean(config.dynamo.endpoint);
const client = new DynamoDBClient({
  region: config.dynamo.region,
  ...(local && {
    endpoint: config.dynamo.endpoint,
    credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
  }),
});
const t = config.dynamo.tables;

const tables = [
  { name: t.urls, keys: [['short_code', 'HASH']], ttl: 'expires_at' },
  { name: t.urlHash, keys: [['url_hash', 'HASH']], ttl: 'expires_at' },
  { name: t.counters, keys: [['name', 'HASH']] },
  { name: t.clickStats, keys: [['short_code', 'HASH'], ['metric', 'RANGE']] },
];

async function exists(name) {
  try {
    await client.send(new DescribeTableCommand({ TableName: name }));
    return true;
  } catch (err) {
    if (err.name === 'ResourceNotFoundException') return false;
    throw err;
  }
}

async function waitForDynamo(retries = 30) {
  for (let i = 1; ; i++) {
    try {
      await exists('__ping__');
      return;
    } catch (err) {
      if (i >= retries) throw err;
      console.log(`waiting for DynamoDB (${err.name})...`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

await waitForDynamo();
for (const { name, keys, ttl } of tables) {
  if (await exists(name)) {
    console.log(`table ${name} exists`);
    continue;
  }
  await client.send(
    new CreateTableCommand({
      TableName: name,
      BillingMode: 'PAY_PER_REQUEST', // on-demand: scales partitions with traffic
      AttributeDefinitions: keys.map(([AttributeName]) => ({ AttributeName, AttributeType: 'S' })),
      KeySchema: keys.map(([AttributeName, KeyType]) => ({ AttributeName, KeyType })),
    }),
  );
  if (ttl) {
    await client.send(
      new UpdateTimeToLiveCommand({
        TableName: name,
        TimeToLiveSpecification: { AttributeName: ttl, Enabled: true },
      }),
    );
  }
  console.log(`created table ${name}${ttl ? ` (TTL on ${ttl})` : ''}`);
}
