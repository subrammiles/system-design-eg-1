// Messaging Service core logic (no HTTP here).
//
// Accepting a message ("store, then deliver" - the golden rule):
//
//   BEGIN
//     idempotency hit?           -> return the original conv_seq (retry of an already-accepted send)
//     per-device client_seq      -> must be exactly last+1, so one sender's messages can never be
//                                   accepted out of order or with a hole ("Msg1 lost, Msg2 shown")
//     UPDATE conversations SET last_seq = last_seq + 1   -> conv_seq (row lock = per-chat sequencer)
//     INSERT idempotency, outbox(message_created)
//     INSERT into Cassandra      -> history written while the transaction is still open
//   COMMIT                       -> only now ACK the sender ("SENT ✔")
//
// If Cassandra fails we ROLLBACK: the seq is not consumed (no gaps) and nothing is published.
// If COMMIT fails after the Cassandra write, the orphan row has a seq that the next message
// reuses, so the idempotent upsert overwrites it. ACK therefore means: in PostgreSQL, in
// Cassandra, and guaranteed to reach Kafka through the outbox.
//
// Trade-off: the conversation row stays locked for one Cassandra write (~ms). That caps a
// SINGLE chat at a few hundred msgs/s, which is plenty; different chats don't contend.
import { randomBytes, randomUUID } from 'node:crypto';
import { issueToken } from '../shared/auth.js';
import { Membership } from '../shared/membership.js';
import { gatewayChannel } from '../shared/presence.js';

export class ApiError extends Error {
  constructor(status, code, message, extra = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.extra = extra;
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const newId = (prefix) => `${prefix}_${randomBytes(6).toString('hex')}`;

export class ChatService {
  constructor({ pg, store, redis, presence, config }) {
    this.pg = pg;
    this.store = store;
    this.redis = redis;
    this.presence = presence;
    this.config = config;
    this.membership = new Membership({ pg, redis });
  }

  async tx(fn) {
    const client = await this.pg.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  // ---------- users, devices, conversations ----------

  async createUser(name) {
    if (!name || typeof name !== 'string' || name.length > 64) throw new ApiError(400, 'BAD_NAME', 'name required (max 64)');
    const id = newId('u');
    await this.pg.query('INSERT INTO users (id, name) VALUES ($1, $2)', [id, name]);
    return { user_id: id, name };
  }

  async registerDevice(userId, { platform = 'unknown', pushToken = null } = {}) {
    const id = newId('d');
    try {
      await this.pg.query('INSERT INTO devices (id, user_id, platform, push_token) VALUES ($1, $2, $3, $4)',
        [id, userId, platform, pushToken]);
    } catch (err) {
      if (err.code === '23503') throw new ApiError(404, 'NO_USER', 'unknown user');
      throw err;
    }
    return { device_id: id, token: issueToken({ userId, deviceId: id }, this.config.authSecret) };
  }

  async createDirect(userId, otherId) {
    if (userId === otherId) throw new ApiError(400, 'SELF_CHAT', 'cannot open a direct chat with yourself');
    const [a, b] = [userId, otherId].sort();
    const id = `dm:${a}:${b}`; // deterministic: opening the chat twice returns the same one
    await this.tx(async (c) => {
      const { rows } = await c.query('SELECT id FROM users WHERE id = ANY($1)', [[a, b]]);
      if (rows.length !== 2) throw new ApiError(404, 'NO_USER', 'unknown user');
      const ins = await c.query(
        `INSERT INTO conversations (id, type, created_by) VALUES ($1, 'direct', $2) ON CONFLICT DO NOTHING RETURNING id`,
        [id, userId]);
      if (ins.rowCount) {
        await c.query('INSERT INTO members (conversation_id, user_id) VALUES ($1, $2), ($1, $3)', [id, a, b]);
      }
    });
    return { conversation_id: id, type: 'direct', members: [a, b] };
  }

  async createGroup(userId, name, memberIds = []) {
    const members = [...new Set([userId, ...memberIds])];
    if (members.length > 100_000) throw new ApiError(400, 'TOO_MANY', 'group too large');
    const id = `grp:${randomUUID()}`;
    await this.tx(async (c) => {
      const { rows } = await c.query('SELECT count(*)::int AS n FROM users WHERE id = ANY($1)', [members]);
      if (rows[0].n !== members.length) throw new ApiError(404, 'NO_USER', 'unknown user in members');
      await c.query(`INSERT INTO conversations (id, type, name, created_by) VALUES ($1, 'group', $2, $3)`, [id, name ?? null, userId]);
      await c.query(
        `INSERT INTO members (conversation_id, user_id, role)
         SELECT $1, u, CASE WHEN u = $3 THEN 'admin' ELSE 'member' END FROM unnest($2::text[]) AS u`,
        [id, members, userId]);
    });
    return { conversation_id: id, type: 'group', name, members };
  }

  async addMember(actorId, conversationId, userId) {
    await this.tx(async (c) => {
      const { rows } = await c.query(
        `SELECT c.type, m.role FROM conversations c JOIN members m ON m.conversation_id = c.id AND m.user_id = $2
          WHERE c.id = $1 FOR UPDATE OF c`, [conversationId, actorId]);
      if (!rows.length) throw new ApiError(404, 'NOT_MEMBER', 'conversation not found');
      if (rows[0].type !== 'group' || rows[0].role !== 'admin') throw new ApiError(403, 'FORBIDDEN', 'only group admins can add members');
      // New members only see messages sent after they joined.
      await c.query(
        `INSERT INTO members (conversation_id, user_id, joined_seq, delivered_seq, read_seq)
         SELECT id, $2, last_seq, last_seq, last_seq FROM conversations WHERE id = $1
         ON CONFLICT DO NOTHING`, [conversationId, userId]);
    });
    await this.membership.invalidate(conversationId);
    return { conversation_id: conversationId, added: userId };
  }

  /** Member ids (Redis read-through cache). */
  members(conversationId) {
    return this.membership.members(conversationId);
  }

  async assertMember(conversationId, userId) {
    const members = await this.members(conversationId);
    if (!members.includes(userId)) throw new ApiError(403, 'NOT_MEMBER', 'not a member of this conversation');
    return members;
  }

  async conversationsOf(userId) {
    const { rows } = await this.pg.query(
      `SELECT c.id AS conversation_id, c.type, c.name, c.last_seq::int, m.joined_seq::int, m.delivered_seq::int, m.read_seq::int,
              (SELECT array_agg(user_id) FROM members x WHERE x.conversation_id = c.id) AS members
         FROM members m JOIN conversations c ON c.id = m.conversation_id
        WHERE m.user_id = $1 ORDER BY c.created_at`, [userId]);
    return rows;
  }

  // ---------- the write path ----------

  async acceptMessage({ userId, deviceId, message_id: messageId, conversation_id: conversationId, client_seq: clientSeq, body, media }) {
    if (!UUID_RE.test(messageId ?? '')) throw new ApiError(400, 'BAD_MESSAGE_ID', 'message_id must be a client-generated UUID');
    if (!Number.isInteger(clientSeq) || clientSeq < 1) throw new ApiError(400, 'BAD_CLIENT_SEQ', 'client_seq must be a positive integer');
    if (!body && !media) throw new ApiError(400, 'EMPTY', 'body or media required');
    if (body && (typeof body !== 'string' || body.length > this.config.messaging.maxBodyChars)) {
      throw new ApiError(400, 'BAD_BODY', `body must be a string up to ${this.config.messaging.maxBodyChars} chars`);
    }
    if (media && (typeof media.key !== 'string' || !media.key.startsWith('media/'))) {
      throw new ApiError(400, 'BAD_MEDIA', 'media.key must come from /media/upload-url');
    }
    await this.assertMember(conversationId, userId);

    return this.tx(async (c) => {
      // Lock the sending device first: concurrent retries of the same send (e.g. via two
      // gateways during a reconnect) serialize here, so the second one sees the first's
      // idempotency row instead of racing past it.
      const dev = await c.query('SELECT last_client_seq FROM devices WHERE id = $1 AND user_id = $2 FOR UPDATE', [deviceId, userId]);
      if (!dev.rowCount) throw new ApiError(401, 'NO_DEVICE', 'unknown device');

      const dup = await c.query('SELECT sender_device, conversation_id, conv_seq, created_at FROM idempotency WHERE message_id = $1', [messageId]);
      if (dup.rowCount) {
        const d = dup.rows[0];
        if (d.sender_device !== deviceId || d.conversation_id !== conversationId) {
          throw new ApiError(409, 'MESSAGE_ID_CONFLICT', 'message_id already used');
        }
        return { message_id: messageId, conversation_id: conversationId, conv_seq: Number(d.conv_seq), created_at: d.created_at.getTime(), duplicate: true };
      }

      // Per-device ordering: the sender's messages are accepted strictly in client_seq order.
      const expected = Number(dev.rows[0].last_client_seq) + 1;
      if (clientSeq > expected) {
        throw new ApiError(409, 'CLIENT_SEQ_GAP', `waiting for client_seq ${expected}`, { expected, retryable: true });
      }
      if (clientSeq < expected) {
        // Lower seq with a NEW message_id: e.g. app data restored on the same device id.
        throw new ApiError(409, 'CLIENT_SEQ_STALE', `client_seq ${clientSeq} already used; next is ${expected}`, { expected });
      }

      const seqRes = await c.query('UPDATE conversations SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq', [conversationId]);
      const convSeq = Number(seqRes.rows[0].last_seq);
      const createdAt = Date.now();
      const message = {
        message_id: messageId, conversation_id: conversationId, conv_seq: convSeq, sender_id: userId,
        sender_device: deviceId, client_seq: clientSeq, body: body ?? null, media: media ?? null, created_at: createdAt,
      };

      await c.query('UPDATE devices SET last_client_seq = $2 WHERE id = $1', [deviceId, clientSeq]);
      await c.query(
        'INSERT INTO idempotency (message_id, sender_device, conversation_id, conv_seq, created_at) VALUES ($1, $2, $3, $4, to_timestamp($5 / 1000.0))',
        [messageId, deviceId, conversationId, convSeq, createdAt]);
      // The sender has obviously "read" their own message.
      await c.query(
        `UPDATE members SET delivered_seq = GREATEST(delivered_seq, $3), read_seq = GREATEST(read_seq, $3)
          WHERE conversation_id = $1 AND user_id = $2`, [conversationId, userId, convSeq]);
      await c.query(
        `INSERT INTO device_cursors (device_id, conversation_id, synced_seq) VALUES ($1, $2, $3)
         ON CONFLICT (device_id, conversation_id) DO UPDATE SET synced_seq = GREATEST(device_cursors.synced_seq, $3)`,
        [deviceId, conversationId, convSeq]);
      await c.query('INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)',
        [this.config.kafka.topics.messageEvents, conversationId, { type: 'message_created', message }]);

      try {
        await this.store.insert(message); // history persisted before COMMIT -> before ACK
      } catch (err) {
        throw new ApiError(503, 'STORE_UNAVAILABLE', `message store write failed: ${err.message}`, { retryable: true });
      }
      return { message_id: messageId, conversation_id: conversationId, conv_seq: convSeq, created_at: createdAt, duplicate: false };
    });
  }

  // ---------- receipts (watermarks) ----------

  /**
   * kind = 'delivered' | 'read'. The client acks the highest CONTIGUOUS seq it holds, so one ack
   * covers many messages. Only an advance emits a receipt event (to the other members' devices).
   */
  async ack({ userId, deviceId, conversation_id: conversationId, conv_seq: seqIn, kind }) {
    const members = await this.assertMember(conversationId, userId);
    const seq = Number(seqIn);
    if (!Number.isInteger(seq) || seq < 0) throw new ApiError(400, 'BAD_SEQ', 'conv_seq must be a non-negative integer');

    return this.tx(async (c) => {
      const conv = await c.query('SELECT last_seq FROM conversations WHERE id = $1', [conversationId]);
      const capped = Math.min(seq, Number(conv.rows[0].last_seq)); // never trust a client past the end
      await c.query(
        `INSERT INTO device_cursors (device_id, conversation_id, synced_seq) VALUES ($1, $2, $3)
         ON CONFLICT (device_id, conversation_id) DO UPDATE SET synced_seq = GREATEST(device_cursors.synced_seq, $3)`,
        [deviceId, conversationId, capped]);

      const sql = kind === 'read'
        ? `UPDATE members SET read_seq = $3, delivered_seq = GREATEST(delivered_seq, $3)
            WHERE conversation_id = $1 AND user_id = $2 AND read_seq < $3 RETURNING delivered_seq, read_seq`
        : `UPDATE members SET delivered_seq = $3
            WHERE conversation_id = $1 AND user_id = $2 AND delivered_seq < $3 RETURNING delivered_seq, read_seq`;
      const upd = await c.query(sql, [conversationId, userId, capped]);
      if (!upd.rowCount) return { advanced: false, conv_seq: capped };

      const receipt = {
        type: 'receipt', conversation_id: conversationId, user_id: userId,
        delivered_seq: Number(upd.rows[0].delivered_seq), read_seq: Number(upd.rows[0].read_seq),
      };
      // Large groups: keep watermarks, but don't broadcast every member's ticks to everyone.
      if (members.length <= this.config.delivery.largeGroupThreshold) {
        await c.query('INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)',
          [this.config.kafka.topics.messageEvents, conversationId, receipt]);
      }
      return { advanced: true, ...receipt };
    });
  }

  // ---------- sync (offline catch-up) ----------

  /**
   * Everything this device is missing, per conversation, from its cursor up to last_seq.
   * `cursors` (client's own local-DB cursors) win over the server-side device cursor, so a
   * client that lost local data can ask for more.
   */
  async sync({ userId, deviceId, cursors = {} }) {
    const { rows } = await this.pg.query(
      `SELECT m.conversation_id, c.last_seq, m.joined_seq, COALESCE(dc.synced_seq, 0) AS synced_seq
         FROM members m
         JOIN conversations c ON c.id = m.conversation_id
         LEFT JOIN device_cursors dc ON dc.device_id = $2 AND dc.conversation_id = m.conversation_id
        WHERE m.user_id = $1`, [userId, deviceId]);

    const page = this.config.messaging.syncPageSize;
    const conversations = [];
    for (const r of rows) {
      const lastSeq = Number(r.last_seq);
      const clientCursor = cursors[r.conversation_id];
      const from = Math.max(Number(r.joined_seq), clientCursor != null ? Number(clientCursor) : Number(r.synced_seq));
      if (lastSeq <= from) continue;
      const messages = await this.store.range(r.conversation_id, from, lastSeq, page);
      conversations.push({ conversation_id: r.conversation_id, last_seq: lastSeq, messages, has_more: from + page < lastSeq });
    }

    // Current receipt watermarks of the other members, so ticks are right after reconnecting.
    const receipts = (await this.pg.query(
      `SELECT o.conversation_id, o.user_id, o.delivered_seq::int, o.read_seq::int
         FROM members m JOIN members o ON o.conversation_id = m.conversation_id AND o.user_id <> m.user_id
         JOIN conversations c ON c.id = m.conversation_id
        WHERE m.user_id = $1
          AND (SELECT count(*) FROM members z WHERE z.conversation_id = c.id) <= $2`,
      [userId, this.config.delivery.largeGroupThreshold])).rows;

    return { conversations, receipts };
  }

  async history(userId, conversationId, beforeSeq, limit = 50) {
    await this.assertMember(conversationId, userId);
    const { rows } = await this.pg.query(
      `SELECT c.last_seq, m.joined_seq FROM conversations c JOIN members m ON m.conversation_id = c.id
        WHERE c.id = $1 AND m.user_id = $2`, [conversationId, userId]);
    const before = beforeSeq ? Number(beforeSeq) : Number(rows[0].last_seq) + 1;
    return this.store.history(conversationId, before, Math.min(Number(limit) || 50, 200), Number(rows[0].joined_seq));
  }

  // ---------- typing (ephemeral: no persistence, no Kafka, best effort) ----------

  async typing({ userId, deviceId, conversation_id: conversationId }) {
    const members = await this.assertMember(conversationId, userId);
    if (members.length > this.config.delivery.largeGroupThreshold) return { sent: 0 };
    const frame = { type: 'typing', conversation_id: conversationId, user_id: userId };
    let sent = 0;
    for (const member of members) {
      for (const d of await this.presence.onlineDevices(member)) {
        if (member === userId && d.deviceId === deviceId) continue;
        await this.redis.publish(gatewayChannel(d.gatewayId), JSON.stringify({ device_id: d.deviceId, frame }));
        sent++;
      }
    }
    return { sent };
  }
}
