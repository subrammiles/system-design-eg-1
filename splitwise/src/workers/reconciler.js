// Periodic reconciliation: for every group changed since the last pass, rebuild balances from
// history and compare with the maintained projection. Drift = a bug -> logged loudly.
// Auto-repair is opt-in (RECONCILE_REPAIR=true); by default a human looks first.
import { config } from '../shared/config.js';
import { createPg, retry, sleep } from '../shared/clients.js';
import { reconcileGroup } from '../shared/reconcile.js';

const pool = createPg();
await retry('postgres', () => pool.query('SELECT 1'));
const repair = process.env.RECONCILE_REPAIR === 'true';
const lastSeen = new Map(); // group_id -> balance_version checked

console.log(`[reconciler] every ${config.reconcileIntervalMs} ms (repair=${repair})`);
for (;;) {
  try {
    const { rows } = await pool.query('SELECT group_id, balance_version FROM groups');
    let checked = 0;
    let drifted = 0;
    for (const g of rows) {
      if (lastSeen.get(g.group_id) === g.balance_version) continue;
      const r = await reconcileGroup(pool, g.group_id, { repair });
      checked++;
      if (!r.consistent) {
        drifted++;
        console.error(`[reconciler] DRIFT in ${g.group_id}:`, JSON.stringify(r.drift));
      }
      lastSeen.set(g.group_id, g.balance_version);
    }
    if (checked) console.log(`[reconciler] checked ${checked} groups, ${drifted} with drift`);
  } catch (err) {
    console.error('[reconciler]', err.message);
  }
  await sleep(config.reconcileIntervalMs);
}
