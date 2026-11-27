// Raw page storage in S3 (gzip, ~70-80% smaller for HTML).
// Key layout spreads objects across prefixes: pages/ab/cd/<urlhash>/<fetchedAt>.html.gz
// Production note: batch many pages into WARC files to cut per-object overhead.
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { gunzipSync, gzipSync } from 'node:zlib';

export class PageStore {
  constructor(s3, bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  async put(urlHash, fetchedAt, body, meta = {}) {
    const key = `pages/${urlHash.slice(0, 2)}/${urlHash.slice(2, 4)}/${urlHash}/${fetchedAt}.html.gz`;
    await this.s3.send(new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      Body: gzipSync(body),
      ContentType: 'text/html',
      ContentEncoding: 'gzip',
      Metadata: Object.fromEntries(Object.entries(meta).map(([k, v]) => [k, String(v).slice(0, 512)])),
    }));
    return key;
  }

  async get(key) {
    const res = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
    return gunzipSync(Buffer.from(await res.Body.transformToByteArray())).toString('utf8');
  }
}
