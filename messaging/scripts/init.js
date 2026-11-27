// One-shot setup (idempotent): PostgreSQL schema, Cassandra keyspace/table, Kafka topics, S3 bucket.
import { readFile } from 'node:fs/promises';
import { CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { config } from '../src/shared/config.js';
import { createCassandra, createKafka, createPg, createS3, retry } from '../src/shared/clients.js';
import { CQL_SCHEMA } from '../src/shared/messageStore.js';

const pg = createPg();
await retry('postgres', () => pg.query('SELECT 1'));
await pg.query(await readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
console.log('postgres schema ready');
await pg.end();

const cass = createCassandra({ withKeyspace: false });
await retry('cassandra', () => cass.connect(), 90);
for (const cql of CQL_SCHEMA(config.cassandra.keyspace, config.cassandra.replicationFactor)) await cass.execute(cql);
console.log('cassandra schema ready');
await cass.shutdown();

const admin = createKafka().admin();
await retry('kafka', () => admin.connect());
const existing = await admin.listTopics();
const wanted = Object.values(config.kafka.topics).filter((t) => !existing.includes(t));
if (wanted.length) {
  await admin.createTopics({
    topics: wanted.map((topic) => ({ topic, numPartitions: config.kafka.partitions, replicationFactor: 1 })),
  });
}
console.log('kafka topics ready:', Object.values(config.kafka.topics).join(', '));
await admin.disconnect();

const s3 = createS3();
await retry('s3', async () => {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: config.s3.bucket }));
  } catch (err) {
    if (err.$metadata?.httpStatusCode !== 404 && err.name !== 'NotFound') throw err;
    await s3.send(new CreateBucketCommand({ Bucket: config.s3.bucket }));
  }
});
console.log('s3 bucket ready');
