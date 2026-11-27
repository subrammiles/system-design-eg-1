// Reconciliation: rebuild a group's balances from the source of truth (ACTIVE expenses +
// CONFIRMED settlements) and compare with the maintained projection (pairwise_balances,
// group_members.net_balance). Any drift means a bug - report it, optionally repair it.
import { replay } from '../domain/ledger.js';
import { withTx } from './clients.js';

export async function reconcileGroup(pool, groupId, { repair = false } = {}) {
  return withTx(pool, async (c) => {
    // Lock the group so no financial write lands between reading history and projection.
    const g = await c.query('SELECT balance_version FROM groups WHERE group_id = $1 FOR UPDATE', [groupId]);
    if (!g.rowCount) return null;

    const exp = await c.query(
      `SELECT e.expense_id, e.paid_by, s.user_id, s.owed_amount
         FROM expenses e JOIN expense_splits s ON s.expense_id = e.expense_id
        WHERE e.group_id = $1 AND e.status = 'ACTIVE'`, [groupId]);
    const byExpense = new Map();
    for (const r of exp.rows) {
      if (!byExpense.has(r.expense_id)) byExpense.set(r.expense_id, { paid_by: r.paid_by, splits: [] });
      byExpense.get(r.expense_id).splits.push({ user_id: r.user_id, amount: r.owed_amount });
    }
    const set = await c.query(`SELECT from_user, to_user, amount FROM settlements WHERE group_id = $1 AND status = 'CONFIRMED'`, [groupId]);
    const expected = replay([...byExpense.values()], set.rows);

    const pairRows = (await c.query('SELECT user_low, user_high, low_to_high FROM pairwise_balances WHERE group_id = $1', [groupId])).rows;
    const memberRows = (await c.query('SELECT user_id, net_balance FROM group_members WHERE group_id = $1', [groupId])).rows;

    const drift = [];
    const actualPairs = new Map(pairRows.map((r) => [`${r.user_low}|${r.user_high}`, r.low_to_high]));
    for (const k of new Set([...actualPairs.keys(), ...expected.pairs.keys()])) {
      const want = expected.pairs.get(k) ?? 0;
      const have = actualPairs.get(k) ?? 0;
      if (want !== have) drift.push({ kind: 'pair', key: k, expected: want, actual: have });
    }
    for (const r of memberRows) {
      const want = expected.nets.get(r.user_id) ?? 0;
      if (want !== r.net_balance) drift.push({ kind: 'net', user_id: r.user_id, expected: want, actual: r.net_balance });
    }

    if (repair && drift.length) {
      for (const d of drift) {
        if (d.kind === 'pair') {
          const [low, high] = d.key.split('|');
          await c.query(
            `INSERT INTO pairwise_balances (group_id, user_low, user_high, low_to_high) VALUES ($1, $2, $3, $4)
             ON CONFLICT (group_id, user_low, user_high) DO UPDATE SET low_to_high = EXCLUDED.low_to_high, updated_at = now()`,
            [groupId, low, high, d.expected]);
        } else {
          await c.query('UPDATE group_members SET net_balance = $3 WHERE group_id = $1 AND user_id = $2', [groupId, d.user_id, d.expected]);
        }
      }
      await c.query('UPDATE groups SET balance_version = balance_version + 1 WHERE group_id = $1', [groupId]);
      await c.query(`INSERT INTO audit_events (group_id, actor, action, detail) VALUES ($1, 'system', 'BALANCES_REPAIRED', $2)`,
        [groupId, { drift }]);
    }
    return {
      group_id: groupId,
      consistent: drift.length === 0,
      expenses: byExpense.size,
      settlements: set.rowCount,
      drift,
      repaired: repair && drift.length > 0,
    };
  });
}
