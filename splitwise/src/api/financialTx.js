// The one place financial state changes. Every expense create/edit/delete/restore and every
// settlement confirmation runs through financialTx():
//
//   BEGIN
//     idempotency claim       (retry with the same Idempotency-Key -> return the stored response)
//     SELECT group FOR UPDATE (serializes financial writes per group -> no lost updates;
//                              different groups never contend)
//     ...caller writes history rows (expense, splits, settlement)...
//     apply deltas            pairwise_balances += d (one canonical row per pair, sorted order)
//                             group_members.net_balance += d
//     balance_version += 1    (also the balance cache key -> invalidation for free)
//     INSERT audit_events, outbox
//   COMMIT
//
// History and its balance effect commit together or not at all: there is no state where the
// expense exists but the balances don't reflect it (the doc's "crash after step 2" problem).
import { createHash, randomUUID } from 'node:crypto';
import { withTx } from '../shared/clients.js';

export class ApiError extends Error {
  constructor(status, code, message, extra = {}) {
    super(message);
    Object.assign(this, { status, code, extra });
  }
}

export const requestHash = (body) => createHash('sha256').update(JSON.stringify(body ?? {})).digest('hex');

/**
 * Claim an idempotency key inside the current transaction. A concurrent request with the same
 * key blocks on the unique index until the first commits, then sees its stored response.
 * @returns {Promise<null | {status: number, body: object}>} stored response if this is a retry
 */
export async function claimIdempotency(client, userId, key, hash) {
  if (!key) return null;
  if (key.length > 128) throw new ApiError(400, 'BAD_IDEMPOTENCY_KEY', 'Idempotency-Key too long');
  const ins = await client.query(
    `INSERT INTO idempotency_keys (user_id, key, request_hash, status_code, response)
     VALUES ($1, $2, $3, 0, '{}') ON CONFLICT DO NOTHING RETURNING key`, [userId, key, hash]);
  if (ins.rowCount) return null;
  const { rows } = await client.query('SELECT request_hash, status_code, response FROM idempotency_keys WHERE user_id = $1 AND key = $2', [userId, key]);
  if (rows[0].request_hash !== hash) {
    throw new ApiError(422, 'IDEMPOTENCY_KEY_REUSED', 'Idempotency-Key was already used with a different request');
  }
  return { status: rows[0].status_code, body: rows[0].response, replayed: true };
}

async function storeIdempotency(client, userId, key, status, body) {
  if (!key) return;
  await client.query('UPDATE idempotency_keys SET status_code = $3, response = $4 WHERE user_id = $1 AND key = $2', [userId, key, status, body]);
}

/** Apply ledger deltas atomically (no read-modify-write in the application). */
export async function applyDeltas(client, groupId, { pairs, nets }) {
  for (const p of pairs) {
    await client.query(
      `INSERT INTO pairwise_balances (group_id, user_low, user_high, low_to_high) VALUES ($1, $2, $3, $4)
       ON CONFLICT (group_id, user_low, user_high)
       DO UPDATE SET low_to_high = pairwise_balances.low_to_high + EXCLUDED.low_to_high, updated_at = now()`,
      [groupId, p.low, p.high, p.delta]);
  }
  for (const [userId, delta] of [...nets].sort(([a], [b]) => (a < b ? -1 : 1))) {
    if (delta === 0) continue;
    const r = await client.query('UPDATE group_members SET net_balance = net_balance + $3 WHERE group_id = $1 AND user_id = $2',
      [groupId, userId, delta]);
    if (!r.rowCount) throw new Error(`member ${userId} missing from group ${groupId}`);
  }
}

export function mergeDeltas(...ds) {
  const pairs = new Map();
  const nets = new Map();
  for (const d of ds) {
    for (const p of d.pairs) {
      const k = `${p.low}|${p.high}`;
      pairs.set(k, { low: p.low, high: p.high, delta: (pairs.get(k)?.delta ?? 0) + p.delta });
    }
    for (const [u, v] of d.nets) nets.set(u, (nets.get(u) ?? 0) + v);
  }
  return {
    pairs: [...pairs.values()].filter((p) => p.delta !== 0)
      .sort((a, b) => (a.low === b.low ? (a.high < b.high ? -1 : 1) : a.low < b.low ? -1 : 1)),
    nets,
  };
}

/**
 * @param {object} o
 * @param {import('pg').Pool} o.pool
 * @param {string} o.groupId
 * @param {string} o.actor
 * @param {string} [o.idempotencyKey]
 * @param {object} [o.body]           request body (hashed for idempotency)
 * @param {(client, group) => Promise<{status?: number, response: object, deltas?: object,
 *          audit?: {action: string, entityId?: string, detail?: object},
 *          event?: {type: string, affectedUsers: string[], data: object}}>} fn
 */
export function financialTx({ pool, groupId, actor, idempotencyKey, body }, fn) {
  return withTx(pool, async (client) => {
    const replay = await claimIdempotency(client, actor, idempotencyKey, requestHash({ groupId, body }));
    if (replay) return replay;

    const g = await client.query('SELECT * FROM groups WHERE group_id = $1 FOR UPDATE', [groupId]);
    if (!g.rowCount) throw new ApiError(404, 'NOT_FOUND', 'group not found');
    let actorRole = 'SYSTEM'; // payment-provider webhooks act as 'system'
    if (actor !== 'system') {
      const m = await client.query('SELECT role FROM group_members WHERE group_id = $1 AND user_id = $2 AND removed_at IS NULL', [groupId, actor]);
      if (!m.rowCount) throw new ApiError(403, 'NOT_MEMBER', 'not a member of this group');
      actorRole = m.rows[0].role;
    }
    const group = { ...g.rows[0], actorRole };

    const out = await fn(client, group);
    let version = group.balance_version;
    if (out.deltas) {
      await applyDeltas(client, groupId, out.deltas);
      version = (await client.query('UPDATE groups SET balance_version = balance_version + 1 WHERE group_id = $1 RETURNING balance_version', [groupId])).rows[0].balance_version;
    }
    if (out.audit) {
      await client.query('INSERT INTO audit_events (group_id, actor, action, entity_id, detail) VALUES ($1, $2, $3, $4, $5)',
        [groupId, actor, out.audit.action, out.audit.entityId ?? null, out.audit.detail ?? {}]);
    }
    if (out.event) {
      await client.query('INSERT INTO outbox (payload) VALUES ($1)', [{
        event_id: randomUUID(), type: out.event.type, group_id: groupId, currency: group.currency, actor,
        balance_version: version, affected_users: out.event.affectedUsers, data: out.event.data, at: Date.now(),
      }]);
    }
    const status = out.status ?? 200;
    await storeIdempotency(client, actor, idempotencyKey, status, out.response);
    return { status, body: out.response };
  });
}
