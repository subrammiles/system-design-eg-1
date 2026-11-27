// Business logic for the Group, Expense, Settlement and Balance services.
//
// They live in one deployable on purpose: an expense and its balance effect must commit in ONE
// database transaction (strong consistency for money). Asynchronous work that may lag -
// notifications, home-screen totals, real-time pushes - goes through the outbox instead.
import { randomBytes } from 'node:crypto';
import { expenseDeltas, pairsToDebts, settlementDeltas } from '../domain/ledger.js';
import { assertCurrency, assertMinor, convert, format } from '../domain/money.js';
import { simplify } from '../domain/simplify.js';
import { calculateSplits } from '../domain/splitCalculator.js';
import { issueToken } from '../shared/auth.js';
import { withTx } from '../shared/clients.js';
import { reconcileGroup } from '../shared/reconcile.js';
import { ApiError, financialTx, mergeDeltas } from './financialTx.js';

const newId = (p) => `${p}_${randomBytes(6).toString('hex')}`;
const MANUAL_METHODS = new Set(['CASH', 'BANK_TRANSFER', 'OTHER']);
const ONLINE_METHODS = new Set(['UPI', 'CARD']);

export class SplitwiseService {
  constructor({ pool, redis, config }) {
    Object.assign(this, { pool, redis, config });
  }

  // ---------------------------------------------------------------- users & groups

  async createUser({ name, email }) {
    if (typeof name !== 'string' || !name.trim() || name.length > 80) throw new ApiError(400, 'BAD_NAME', 'name required');
    const userId = newId('u');
    try {
      await this.pool.query('INSERT INTO users (user_id, name, email) VALUES ($1, $2, $3)', [userId, name.trim(), email ?? null]);
    } catch (err) {
      if (err.code === '23505') throw new ApiError(409, 'EMAIL_TAKEN', 'email already registered');
      throw err;
    }
    return { user_id: userId, name, token: issueToken(userId, this.config.authSecret) };
  }

  async createGroup(actor, { name, currency = 'USD', members = [], simplify_debts: simplifyDebts = true }) {
    if (typeof name !== 'string' || !name.trim()) throw new ApiError(400, 'BAD_NAME', 'name required');
    assertCurrency(currency);
    const all = [...new Set([actor, ...members])];
    if (all.length > this.config.limits.maxGroupMembers) throw new ApiError(400, 'TOO_MANY_MEMBERS', 'group too large');
    const groupId = newId('g');
    await withTx(this.pool, async (c) => {
      const { rows } = await c.query('SELECT count(*)::int AS n FROM users WHERE user_id = ANY($1)', [all]);
      if (rows[0].n !== all.length) throw new ApiError(404, 'NO_USER', 'unknown user in members');
      await c.query('INSERT INTO groups (group_id, name, currency, simplify_debts, created_by) VALUES ($1, $2, $3, $4, $5)',
        [groupId, name.trim(), currency, Boolean(simplifyDebts), actor]);
      await c.query(
        `INSERT INTO group_members (group_id, user_id, role)
         SELECT $1, u, CASE WHEN u = $3 THEN 'ADMIN' ELSE 'MEMBER' END FROM unnest($2::text[]) u`, [groupId, all, actor]);
      await c.query(`INSERT INTO audit_events (group_id, actor, action, detail) VALUES ($1, $2, 'GROUP_CREATED', $3)`, [groupId, actor, { name, members: all }]);
    });
    return this.getGroup(actor, groupId);
  }

  async assertMember(groupId, userId) {
    const { rows } = await this.pool.query(
      `SELECT g.*, m.role FROM groups g JOIN group_members m ON m.group_id = g.group_id
        WHERE g.group_id = $1 AND m.user_id = $2 AND m.removed_at IS NULL`, [groupId, userId]);
    if (!rows.length) throw new ApiError(404, 'NOT_FOUND', 'group not found'); // don't leak existence to non-members
    return rows[0];
  }

  async getGroup(actor, groupId) {
    const g = await this.assertMember(groupId, actor);
    const { rows } = await this.pool.query(
      `SELECT m.user_id, u.name, m.role, m.net_balance FROM group_members m JOIN users u USING (user_id)
        WHERE m.group_id = $1 AND m.removed_at IS NULL ORDER BY m.joined_at, m.user_id`, [groupId]);
    return { group_id: g.group_id, name: g.name, currency: g.currency, simplify_debts: g.simplify_debts, created_by: g.created_by, members: rows };
  }

  async myGroups(actor) {
    const { rows } = await this.pool.query(
      `SELECT g.group_id, g.name, g.currency, m.net_balance FROM group_members m JOIN groups g USING (group_id)
        WHERE m.user_id = $1 AND m.removed_at IS NULL ORDER BY g.created_at`, [actor]);
    return rows;
  }

  async addMember(actor, groupId, userId) {
    return financialTx({ pool: this.pool, groupId, actor }, async (c, group) => {
      if (group.actorRole !== 'ADMIN') throw new ApiError(403, 'FORBIDDEN', 'only admins can add members');
      if (!(await c.query('SELECT 1 FROM users WHERE user_id = $1', [userId])).rowCount) throw new ApiError(404, 'NO_USER', 'unknown user');
      await c.query(
        `INSERT INTO group_members (group_id, user_id) VALUES ($1, $2)
         ON CONFLICT (group_id, user_id) DO UPDATE SET removed_at = NULL`, [groupId, userId]);
      return {
        status: 201,
        response: { group_id: groupId, user_id: userId },
        audit: { action: 'MEMBER_ADDED', entityId: userId },
        event: { type: 'MEMBER_ADDED', affectedUsers: [userId], data: { user_id: userId } },
      };
    });
  }

  async removeMember(actor, groupId, userId) {
    return financialTx({ pool: this.pool, groupId, actor }, async (c, group) => {
      if (group.actorRole !== 'ADMIN' && actor !== userId) throw new ApiError(403, 'FORBIDDEN', 'only admins can remove other members');
      const m = await c.query('SELECT net_balance FROM group_members WHERE group_id = $1 AND user_id = $2 AND removed_at IS NULL', [groupId, userId]);
      if (!m.rowCount) throw new ApiError(404, 'NOT_MEMBER', 'not a member');
      const open = await c.query(
        `SELECT 1 FROM pairwise_balances WHERE group_id = $1 AND (user_low = $2 OR user_high = $2) AND low_to_high <> 0 LIMIT 1`, [groupId, userId]);
      if (m.rows[0].net_balance !== 0 || open.rowCount) {
        throw new ApiError(409, 'UNSETTLED_BALANCE', 'member must settle up before leaving the group');
      }
      await c.query('UPDATE group_members SET removed_at = now() WHERE group_id = $1 AND user_id = $2', [groupId, userId]);
      return { response: { removed: userId }, audit: { action: 'MEMBER_REMOVED', entityId: userId } };
    });
  }

  // ---------------------------------------------------------------- expenses

  async activeMemberIds(c, groupId) {
    const { rows } = await c.query('SELECT user_id FROM group_members WHERE group_id = $1 AND removed_at IS NULL', [groupId]);
    return new Set(rows.map((r) => r.user_id));
  }

  /** Validate an expense request and compute its exact splits (in the group currency). */
  buildExpense(group, members, input, actor) {
    const description = typeof input.description === 'string' ? input.description.trim() : '';
    if (!description || description.length > 200) throw new ApiError(400, 'BAD_DESCRIPTION', 'description required (max 200 chars)');
    const paidBy = input.paid_by ?? actor;
    if (!members.has(paidBy)) throw new ApiError(400, 'BAD_PAYER', 'payer must be a group member');
    const splitType = input.split_type ?? 'EQUAL';
    const participants = input.participants ?? [...members].map((user_id) => ({ user_id }));
    if (!Array.isArray(participants) || participants.length > this.config.limits.maxParticipants) {
      throw new ApiError(400, 'BAD_PARTICIPANTS', 'participants must be an array');
    }
    for (const p of participants) {
      if (!members.has(p?.user_id)) throw new ApiError(400, 'BAD_PARTICIPANT', `participant ${p?.user_id} is not a group member`);
    }

    const currency = input.currency ?? group.currency;
    assertCurrency(currency);
    const enteredAmount = assertMinor(input.total_amount, { max: this.config.limits.maxAmountMinor, field: 'total_amount' });
    let total = enteredAmount;
    let fx = {};
    if (currency !== group.currency) {
      if (splitType === 'EXACT') throw new ApiError(400, 'FX_EXACT', 'EXACT splits must be entered in the group currency');
      total = convert(enteredAmount, currency, group.currency, input.exchange_rate);
      if (total <= 0) throw new ApiError(400, 'INVALID_AMOUNT', 'converted amount rounds to zero');
      fx = { original_amount: enteredAmount, original_currency: currency, exchange_rate: input.exchange_rate };
    }
    const splits = calculateSplits({ total, splitType, paidBy, participants });
    return {
      description, paidBy, splitType, total, fx, splits,
      splitInput: { participants, currency, total_amount: enteredAmount, exchange_rate: input.exchange_rate ?? null },
      expenseDate: input.expense_date ?? null,
    };
  }

  expenseView(row, splits, currency) {
    return {
      expense_id: row.expense_id, group_id: row.group_id, description: row.description, paid_by: row.paid_by,
      total_amount: row.total_amount, currency, display: format(row.total_amount, currency),
      original_amount: row.original_amount ?? undefined, original_currency: row.original_currency ?? undefined,
      exchange_rate: row.exchange_rate ?? undefined, split_type: row.split_type, status: row.status, version: row.version,
      expense_date: row.expense_date, created_by: row.created_by, created_at: row.created_at,
      splits: splits.map((s) => ({ user_id: s.user_id, amount: s.amount ?? s.owed_amount })),
    };
  }

  async snapshot(c, expenseId, version, actor) {
    const e = (await c.query('SELECT * FROM expenses WHERE expense_id = $1', [expenseId])).rows[0];
    const s = (await c.query('SELECT user_id, owed_amount FROM expense_splits WHERE expense_id = $1 ORDER BY user_id', [expenseId])).rows;
    await c.query('INSERT INTO expense_versions (expense_id, version, snapshot, changed_by) VALUES ($1, $2, $3, $4)',
      [expenseId, version, { ...e, splits: s }, actor]);
  }

  async createExpense(actor, groupId, input, idempotencyKey) {
    return financialTx({ pool: this.pool, groupId, actor, idempotencyKey, body: input }, async (c, group) => {
      const members = await this.activeMemberIds(c, groupId);
      const x = this.buildExpense(group, members, input ?? {}, actor);
      const expenseId = newId('e');
      const row = (await c.query(
        `INSERT INTO expenses (expense_id, group_id, paid_by, description, total_amount, currency, original_amount,
                               original_currency, exchange_rate, split_type, split_input, expense_date, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12::date, CURRENT_DATE), $13) RETURNING *`,
        [expenseId, groupId, x.paidBy, x.description, x.total, group.currency, x.fx.original_amount ?? null,
          x.fx.original_currency ?? null, x.fx.exchange_rate ?? null, x.splitType, x.splitInput, x.expenseDate, actor])).rows[0];
      for (const s of x.splits) {
        await c.query('INSERT INTO expense_splits (expense_id, user_id, owed_amount) VALUES ($1, $2, $3)', [expenseId, s.user_id, s.amount]);
      }
      await this.snapshot(c, expenseId, 1, actor);
      const view = this.expenseView(row, x.splits, group.currency);
      return {
        status: 201,
        response: view,
        deltas: expenseDeltas(x.paidBy, x.splits),
        audit: { action: 'EXPENSE_CREATED', entityId: expenseId, detail: { total: x.total, paid_by: x.paidBy } },
        event: { type: 'EXPENSE_CREATED', affectedUsers: [...new Set([x.paidBy, ...x.splits.map((s) => s.user_id)])], data: view },
      };
    });
  }

  async groupOfExpense(expenseId) {
    const { rows } = await this.pool.query('SELECT group_id FROM expenses WHERE expense_id = $1', [expenseId]);
    if (!rows.length) throw new ApiError(404, 'NOT_FOUND', 'expense not found');
    return rows[0].group_id;
  }

  /** Lock the expense row, check permission + optimistic version, return it with its splits. */
  async lockExpense(c, expenseId, actor, group, expectedVersion) {
    const e = (await c.query('SELECT * FROM expenses WHERE expense_id = $1 FOR UPDATE', [expenseId])).rows[0];
    if (!e) throw new ApiError(404, 'NOT_FOUND', 'expense not found');
    if (![e.created_by, e.paid_by].includes(actor) && group.actorRole !== 'ADMIN') {
      throw new ApiError(403, 'FORBIDDEN', 'only the creator, the payer or an admin can change this expense');
    }
    if (expectedVersion != null && Number(expectedVersion) !== e.version) {
      throw new ApiError(409, 'VERSION_CONFLICT', `expense was modified (current version ${e.version})`, { current_version: e.version });
    }
    const splits = (await c.query('SELECT user_id, owed_amount AS amount FROM expense_splits WHERE expense_id = $1', [expenseId])).rows;
    return { e, splits };
  }

  /**
   * Edit = reverse the old financial effect + apply the new one, in one transaction.
   * Requires `version` (optimistic concurrency): two people editing the same expense can't
   * silently overwrite each other.
   */
  async updateExpense(actor, expenseId, input, idempotencyKey) {
    if (input?.version == null) throw new ApiError(400, 'VERSION_REQUIRED', 'version is required to edit an expense');
    const groupId = await this.groupOfExpense(expenseId);
    return financialTx({ pool: this.pool, groupId, actor, idempotencyKey, body: { expenseId, input } }, async (c, group) => {
      const { e, splits: oldSplits } = await this.lockExpense(c, expenseId, actor, group, input.version);
      if (e.status !== 'ACTIVE') throw new ApiError(409, 'DELETED', 'expense is deleted; restore it first');
      const members = await this.activeMemberIds(c, groupId);
      // Old participants who left the group may stay on an edited expense.
      for (const s of oldSplits) members.add(s.user_id);
      members.add(e.paid_by);
      const merged = {
        description: input.description ?? e.description,
        paid_by: input.paid_by ?? e.paid_by,
        split_type: input.split_type ?? e.split_type,
        participants: input.participants ?? e.split_input.participants,
        currency: input.currency ?? e.split_input.currency,
        total_amount: input.total_amount ?? e.split_input.total_amount,
        exchange_rate: input.exchange_rate ?? e.split_input.exchange_rate ?? undefined,
        expense_date: input.expense_date ?? e.expense_date,
      };
      if (input.split_type && !input.participants && input.split_type !== e.split_type) {
        throw new ApiError(400, 'PARTICIPANTS_REQUIRED', 'changing split_type requires participants');
      }
      const x = this.buildExpense(group, members, merged, actor);
      const version = e.version + 1;
      const row = (await c.query(
        `UPDATE expenses SET paid_by = $2, description = $3, total_amount = $4, original_amount = $5, original_currency = $6,
                exchange_rate = $7, split_type = $8, split_input = $9, expense_date = $10::date, version = $11, updated_at = now()
          WHERE expense_id = $1 RETURNING *`,
        [expenseId, x.paidBy, x.description, x.total, x.fx.original_amount ?? null, x.fx.original_currency ?? null,
          x.fx.exchange_rate ?? null, x.splitType, x.splitInput, merged.expense_date, version])).rows[0];
      await c.query('DELETE FROM expense_splits WHERE expense_id = $1', [expenseId]);
      for (const s of x.splits) {
        await c.query('INSERT INTO expense_splits (expense_id, user_id, owed_amount) VALUES ($1, $2, $3)', [expenseId, s.user_id, s.amount]);
      }
      await this.snapshot(c, expenseId, version, actor);
      const view = this.expenseView(row, x.splits, group.currency);
      const affected = [...new Set([e.paid_by, x.paidBy, ...oldSplits.map((s) => s.user_id), ...x.splits.map((s) => s.user_id)])];
      return {
        response: view,
        deltas: mergeDeltas(expenseDeltas(e.paid_by, oldSplits, -1), expenseDeltas(x.paidBy, x.splits)),
        audit: { action: 'EXPENSE_UPDATED', entityId: expenseId, detail: { from_version: e.version, to_version: version, old_total: e.total_amount, new_total: x.total } },
        event: { type: 'EXPENSE_UPDATED', affectedUsers: affected, data: { ...view, old_total: e.total_amount } },
      };
    });
  }

  /** Soft delete: reverse the balance effect, keep the record (and its history) forever. */
  async deleteExpense(actor, expenseId, expectedVersion) {
    const groupId = await this.groupOfExpense(expenseId);
    return financialTx({ pool: this.pool, groupId, actor }, async (c, group) => {
      const { e, splits } = await this.lockExpense(c, expenseId, actor, group, expectedVersion);
      if (e.status === 'DELETED') return { response: { expense_id: expenseId, status: 'DELETED', version: e.version } }; // idempotent
      const version = e.version + 1;
      await c.query(`UPDATE expenses SET status = 'DELETED', deleted_at = now(), version = $2, updated_at = now() WHERE expense_id = $1`, [expenseId, version]);
      await this.snapshot(c, expenseId, version, actor);
      return {
        response: { expense_id: expenseId, status: 'DELETED', version },
        deltas: expenseDeltas(e.paid_by, splits, -1),
        audit: { action: 'EXPENSE_DELETED', entityId: expenseId, detail: { total: e.total_amount } },
        event: { type: 'EXPENSE_DELETED', affectedUsers: [...new Set([e.paid_by, ...splits.map((s) => s.user_id)])], data: { expense_id: expenseId, description: e.description, total_amount: e.total_amount } },
      };
    });
  }

  async restoreExpense(actor, expenseId) {
    const groupId = await this.groupOfExpense(expenseId);
    return financialTx({ pool: this.pool, groupId, actor }, async (c, group) => {
      const { e, splits } = await this.lockExpense(c, expenseId, actor, group, null);
      if (e.status === 'ACTIVE') return { response: { expense_id: expenseId, status: 'ACTIVE', version: e.version } };
      const version = e.version + 1;
      await c.query(`UPDATE expenses SET status = 'ACTIVE', deleted_at = NULL, version = $2, updated_at = now() WHERE expense_id = $1`, [expenseId, version]);
      await this.snapshot(c, expenseId, version, actor);
      return {
        response: { expense_id: expenseId, status: 'ACTIVE', version },
        deltas: expenseDeltas(e.paid_by, splits),
        audit: { action: 'EXPENSE_RESTORED', entityId: expenseId },
        event: { type: 'EXPENSE_RESTORED', affectedUsers: [...new Set([e.paid_by, ...splits.map((s) => s.user_id)])], data: { expense_id: expenseId, description: e.description } },
      };
    });
  }

  async getExpense(actor, expenseId) {
    const groupId = await this.groupOfExpense(expenseId);
    const g = await this.assertMember(groupId, actor);
    const e = (await this.pool.query('SELECT * FROM expenses WHERE expense_id = $1', [expenseId])).rows[0];
    const s = (await this.pool.query('SELECT user_id, owed_amount FROM expense_splits WHERE expense_id = $1 ORDER BY user_id', [expenseId])).rows;
    const versions = (await this.pool.query('SELECT version, changed_by, changed_at FROM expense_versions WHERE expense_id = $1 ORDER BY version', [expenseId])).rows;
    return { ...this.expenseView(e, s, g.currency), history: versions };
  }

  /** Keyset pagination (stable under inserts, no OFFSET scans): ?cursor=<opaque>&limit=. */
  async listExpenses(actor, groupId, { cursor, limit = 20, includeDeleted = false }) {
    const g = await this.assertMember(groupId, actor);
    const lim = Math.min(Math.max(Number(limit) || 20, 1), 100);
    let after = null;
    if (cursor) {
      try {
        after = JSON.parse(Buffer.from(cursor, 'base64url').toString());
      } catch {
        throw new ApiError(400, 'BAD_CURSOR', 'invalid cursor');
      }
    }
    const { rows } = await this.pool.query(
      // created_at::text keeps PostgreSQL's microsecond precision for the cursor (a JS Date
      // would truncate to milliseconds and silently skip rows created in the same millisecond).
      `SELECT *, created_at::text AS cursor_ts FROM expenses
        WHERE group_id = $1 AND ($2::boolean OR status = 'ACTIVE')
          AND ($3::timestamptz IS NULL OR (created_at, expense_id) < ($3::timestamptz, $4))
        ORDER BY created_at DESC, expense_id DESC LIMIT $5`,
      [groupId, includeDeleted, after?.c ?? null, after?.id ?? '', lim + 1]);
    const page = rows.slice(0, lim);
    const splits = page.length
      ? (await this.pool.query('SELECT expense_id, user_id, owed_amount FROM expense_splits WHERE expense_id = ANY($1)', [page.map((r) => r.expense_id)])).rows
      : [];
    const items = page.map((r) => this.expenseView(r, splits.filter((s) => s.expense_id === r.expense_id), g.currency));
    const last = page.at(-1);
    return {
      items,
      next_cursor: rows.length > lim ? Buffer.from(JSON.stringify({ c: last.cursor_ts, id: last.expense_id })).toString('base64url') : null,
    };
  }

  // ---------------------------------------------------------------- settlements

  /**
   * Who confirms a settlement:
   *   recorded by the RECEIVER (manual)  -> CONFIRMED immediately (they acknowledge receiving it)
   *   recorded by the PAYER (manual)     -> PENDING until the receiver confirms (no fake "I paid")
   *   online (UPI/CARD)                  -> PENDING until the payment provider's webhook says SUCCEEDED
   * Only CONFIRMED settlements change balances. Partial payments are just smaller amounts.
   */
  async createSettlement(actor, groupId, input, idempotencyKey) {
    return financialTx({ pool: this.pool, groupId, actor, idempotencyKey, body: input }, async (c, group) => {
      const { from_user: from, to_user: to, method = 'CASH', note } = input ?? {};
      const amount = assertMinor(input?.amount, { max: this.config.limits.maxAmountMinor });
      if (from === to) throw new ApiError(400, 'SAME_USER', 'cannot settle with yourself');
      const members = await this.activeMemberIds(c, groupId);
      if (!members.has(from) || !members.has(to)) throw new ApiError(400, 'BAD_USERS', 'both users must be group members');
      if (actor !== from && actor !== to) throw new ApiError(403, 'FORBIDDEN', 'only the payer or the receiver can record a settlement');
      if (!MANUAL_METHODS.has(method) && !ONLINE_METHODS.has(method)) throw new ApiError(400, 'BAD_METHOD', 'unsupported method');

      const settlementId = newId('s');
      const online = ONLINE_METHODS.has(method);
      const status = !online && actor === to ? 'CONFIRMED' : 'PENDING';
      const providerRef = online ? `pay_${randomBytes(8).toString('hex')}` : null;
      const row = (await c.query(
        `INSERT INTO settlements (settlement_id, group_id, from_user, to_user, amount, method, status, provider_ref, note, created_by, confirmed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CASE WHEN $7 = 'CONFIRMED' THEN now() END) RETURNING *`,
        [settlementId, groupId, from, to, amount, method, status, providerRef, note ?? null, actor])).rows[0];
      const view = { ...row, display: format(amount, group.currency), ...(online && { payment: { provider_ref: providerRef, checkout_url: `https://pay.example/checkout/${providerRef}` } }) };
      return {
        status: 201,
        response: view,
        deltas: status === 'CONFIRMED' ? settlementDeltas(from, to, amount) : undefined,
        audit: { action: `SETTLEMENT_${status}`, entityId: settlementId, detail: { from, to, amount, method } },
        event: { type: status === 'CONFIRMED' ? 'SETTLEMENT_CONFIRMED' : 'SETTLEMENT_PENDING', affectedUsers: [from, to], data: view },
      };
    });
  }

  async transitionSettlement(actor, settlementId, action) {
    const s0 = (await this.pool.query('SELECT group_id FROM settlements WHERE settlement_id = $1', [settlementId])).rows[0];
    if (!s0) throw new ApiError(404, 'NOT_FOUND', 'settlement not found');
    return financialTx({ pool: this.pool, groupId: s0.group_id, actor }, async (c, group) => {
      const s = (await c.query('SELECT * FROM settlements WHERE settlement_id = $1 FOR UPDATE', [settlementId])).rows[0];
      const target = { confirm: 'CONFIRMED', reject: 'REJECTED', cancel: 'CANCELLED' }[action];
      if (s.status === target) return { response: s }; // idempotent
      if (s.status !== 'PENDING') throw new ApiError(409, 'NOT_PENDING', `settlement is ${s.status}`);
      if (action === 'cancel' && actor !== s.from_user) throw new ApiError(403, 'FORBIDDEN', 'only the payer can cancel');
      if (action !== 'cancel') {
        if (actor !== s.to_user) throw new ApiError(403, 'FORBIDDEN', 'only the receiver can confirm or reject');
        if (ONLINE_METHODS.has(s.method)) throw new ApiError(409, 'PROVIDER_CONFIRMS', 'online payments are confirmed by the payment provider');
      }
      const row = (await c.query(
        `UPDATE settlements SET status = $2, confirmed_at = CASE WHEN $2 = 'CONFIRMED' THEN now() END WHERE settlement_id = $1 RETURNING *`,
        [settlementId, target])).rows[0];
      return {
        response: { ...row, display: format(row.amount, group.currency) },
        deltas: target === 'CONFIRMED' ? settlementDeltas(s.from_user, s.to_user, s.amount) : undefined,
        audit: { action: `SETTLEMENT_${target}`, entityId: settlementId },
        event: { type: `SETTLEMENT_${target}`, affectedUsers: [s.from_user, s.to_user], data: row },
      };
    });
  }

  /** Payment-provider webhook (signature verified by the caller). Idempotent by provider_ref. */
  async paymentWebhook({ provider_ref: providerRef, outcome }) {
    const s0 = (await this.pool.query('SELECT settlement_id, group_id FROM settlements WHERE provider_ref = $1', [providerRef])).rows[0];
    if (!s0) throw new ApiError(404, 'NOT_FOUND', 'unknown provider_ref');
    return financialTx({ pool: this.pool, groupId: s0.group_id, actor: 'system' }, async (c, group) => {
      const s = (await c.query('SELECT * FROM settlements WHERE settlement_id = $1 FOR UPDATE', [s0.settlement_id])).rows[0];
      const target = outcome === 'SUCCEEDED' ? 'CONFIRMED' : 'FAILED';
      if (s.status !== 'PENDING') return { response: { settlement_id: s.settlement_id, status: s.status, duplicate: true } };
      const row = (await c.query(
        `UPDATE settlements SET status = $2, confirmed_at = CASE WHEN $2 = 'CONFIRMED' THEN now() END WHERE settlement_id = $1 RETURNING *`,
        [s.settlement_id, target])).rows[0];
      return {
        response: { settlement_id: row.settlement_id, status: row.status },
        deltas: target === 'CONFIRMED' ? settlementDeltas(s.from_user, s.to_user, s.amount) : undefined,
        audit: { action: `SETTLEMENT_${target}`, entityId: s.settlement_id, detail: { provider_ref: providerRef } },
        event: { type: `SETTLEMENT_${target}`, affectedUsers: [s.from_user, s.to_user], data: { ...row, display: format(row.amount, group.currency) } },
      };
    });
  }

  async listSettlements(actor, groupId) {
    await this.assertMember(groupId, actor);
    return (await this.pool.query('SELECT * FROM settlements WHERE group_id = $1 ORDER BY created_at DESC LIMIT 200', [groupId])).rows;
  }

  // ---------------------------------------------------------------- balances

  /**
   * Both views, explicitly different:
   *   pairwise   - exactly who owes whom, from the expense relationships
   *   simplified - a SUGGESTED minimal set of transfers settling everyone's net position
   * Cached under the group's balance_version: any financial write bumps it, so the cache
   * can never serve stale balances and needs no explicit invalidation.
   */
  async getBalances(actor, groupId) {
    const g = await this.assertMember(groupId, actor);
    const key = `balances:${groupId}:v${g.balance_version}`;
    const cached = await this.redis.get(key).catch(() => null);
    if (cached) return { ...JSON.parse(cached), cache: 'hit' };

    const members = (await this.pool.query(
      `SELECT m.user_id, u.name, m.net_balance FROM group_members m JOIN users u USING (user_id)
        WHERE m.group_id = $1 AND (m.removed_at IS NULL OR m.net_balance <> 0) ORDER BY m.user_id`, [groupId])).rows;
    const pairs = (await this.pool.query('SELECT user_low, user_high, low_to_high FROM pairwise_balances WHERE group_id = $1 AND low_to_high <> 0', [groupId])).rows;
    const nets = new Map(members.map((m) => [m.user_id, m.net_balance]));
    const plan = simplify(nets, { optimalMax: this.config.limits.optimalSimplifyMax });
    const result = {
      group_id: groupId,
      currency: g.currency,
      balance_version: g.balance_version,
      members: members.map((m) => ({ ...m, display: format(m.net_balance, g.currency) })),
      pairwise: pairsToDebts(pairs),
      simplified: { algorithm: plan.algorithm, transfers: plan.transfers },
      default_view: g.simplify_debts ? 'simplified' : 'pairwise',
    };
    await this.redis.set(key, JSON.stringify(result), 'EX', this.config.cache.balancesTtlSeconds).catch(() => {});
    return { ...result, cache: 'miss' };
  }

  async activity(actor, groupId, limit = 50) {
    await this.assertMember(groupId, actor);
    return (await this.pool.query('SELECT * FROM audit_events WHERE group_id = $1 ORDER BY id DESC LIMIT $2', [groupId, Math.min(Number(limit) || 50, 200)])).rows;
  }

  async reconcile(actor, groupId, repair) {
    const g = await this.assertMember(groupId, actor);
    if (repair && g.role !== 'ADMIN') throw new ApiError(403, 'FORBIDDEN', 'only admins can repair balances');
    return reconcileGroup(this.pool, groupId, { repair });
  }
}

/** Home-screen totals across all groups, per currency. Used by the projector (writes Redis). */
export async function computeUserSummary(pool, userId) {
  const { rows } = await pool.query(
    `SELECT g.group_id, g.name, g.currency, m.net_balance FROM group_members m JOIN groups g USING (group_id)
      WHERE m.user_id = $1 AND (m.removed_at IS NULL OR m.net_balance <> 0)`, [userId]);
  const totals = {};
  for (const r of rows) {
    const t = (totals[r.currency] ??= { you_are_owed: 0, you_owe: 0, net: 0 });
    if (r.net_balance > 0) t.you_are_owed += r.net_balance;
    else t.you_owe += -r.net_balance;
    t.net += r.net_balance;
  }
  return { user_id: userId, totals, groups: rows, computed_at: Date.now() };
}
