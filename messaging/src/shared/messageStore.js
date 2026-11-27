// Message history in Cassandra.
//
//   PRIMARY KEY ((conversation_id, bucket_id), conv_seq)  CLUSTERING ORDER BY conv_seq DESC
//
// - Partition by conversation (all of a chat's recent messages live together: "last 50" is one
//   partition read) - NOT by message_id, which would scatter a chat across every node.
// - Sequence-range buckets (seq 0-999 -> bucket 0, 1000-1999 -> bucket 1, ...) cap partition
//   size, so a hyperactive group doesn't grow one unbounded hot partition.
// - Writes are idempotent upserts keyed by (conversation, seq): retries are harmless.
import { config } from './config.js';

export const bucketOf = (seq, size = config.cassandra.bucketSize) => Math.floor(Number(seq) / size);

export const CQL_SCHEMA = (keyspace, rf) => [
  `CREATE KEYSPACE IF NOT EXISTS ${keyspace}
     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': ${rf}}`,
  `CREATE TABLE IF NOT EXISTS ${keyspace}.messages_by_conversation (
     conversation_id text,
     bucket_id       int,
     conv_seq        bigint,
     message_id      uuid,
     sender_id       text,
     sender_device   text,
     client_seq      bigint,
     body            text,       -- E2EE in production: opaque ciphertext blob
     media           text,       -- JSON {key, content_type, size} or null
     created_at      timestamp,
     PRIMARY KEY ((conversation_id, bucket_id), conv_seq)
   ) WITH CLUSTERING ORDER BY (conv_seq DESC)`,
];

const rowToMessage = (r) => ({
  conversation_id: r.conversation_id,
  conv_seq: Number(r.conv_seq),
  message_id: r.message_id.toString(),
  sender_id: r.sender_id,
  sender_device: r.sender_device,
  client_seq: r.client_seq == null ? null : Number(r.client_seq),
  body: r.body,
  media: r.media ? JSON.parse(r.media) : null,
  created_at: r.created_at?.getTime() ?? null,
});

export class MessageStore {
  constructor(client, bucketSize = config.cassandra.bucketSize) {
    this.db = client;
    this.bucketSize = bucketSize;
  }

  async insert(m) {
    await this.db.execute(
      `INSERT INTO messages_by_conversation
         (conversation_id, bucket_id, conv_seq, message_id, sender_id, sender_device, client_seq, body, media, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [m.conversation_id, bucketOf(m.conv_seq, this.bucketSize), m.conv_seq, m.message_id, m.sender_id,
        m.sender_device, m.client_seq, m.body ?? null, m.media ? JSON.stringify(m.media) : null, new Date(m.created_at)],
    );
  }

  /** Messages with afterSeq < conv_seq <= toSeq, ascending (used by sync). */
  async range(conversationId, afterSeq, toSeq, limit = 500) {
    const out = [];
    const end = Math.min(toSeq, afterSeq + limit);
    for (let b = bucketOf(afterSeq + 1, this.bucketSize); b <= bucketOf(end, this.bucketSize); b++) {
      const rs = await this.db.execute(
        `SELECT * FROM messages_by_conversation
          WHERE conversation_id = ? AND bucket_id = ? AND conv_seq > ? AND conv_seq <= ?
          ORDER BY conv_seq ASC`,
        [conversationId, b, afterSeq, end],
      );
      out.push(...rs.rows.map(rowToMessage));
    }
    return out;
  }

  /** Latest `limit` messages before `beforeSeq` (history paging), newest first. */
  async history(conversationId, beforeSeq, limit = 50, minSeq = 0) {
    const out = [];
    for (let b = bucketOf(beforeSeq - 1, this.bucketSize); b >= 0 && out.length < limit; b--) {
      const rs = await this.db.execute(
        `SELECT * FROM messages_by_conversation
          WHERE conversation_id = ? AND bucket_id = ? AND conv_seq < ? AND conv_seq > ?
          LIMIT ?`,
        [conversationId, b, beforeSeq, minSeq, limit - out.length],
      );
      out.push(...rs.rows.map(rowToMessage));
      if ((b * this.bucketSize) <= minSeq) break;
    }
    return out;
  }
}
