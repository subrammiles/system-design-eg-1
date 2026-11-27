# Splitwise-style Expense Sharing

Runnable implementation of the Splitwise system design. The core rule:

> **Expenses and settlements are the source of truth. Balances are derived from them, kept as a
> fast projection, and can always be rebuilt and checked against history.**

**Stack:**
- Node.js + Express API (Group, Expense, Settlement and Balance services);
- PostgreSQL for the ledger, projections, idempotency, audit and outbox;
- Redis for the balance cache, home-screen totals, notification inbox, real-time pub/sub, and an event stream (standing in for Kafka);
- background workers for the outbox, balance projection, notifications and reconciliation.

**Target scale from the design:** 10M users and 100M expenses.

## Architecture

```
 Mobile / Web ──► API (auth, Idempotency-Key, validation)
                   │
                   │  ONE PostgreSQL transaction per financial change:
                   │    idempotency claim → SELECT group FOR UPDATE
                   │    → INSERT expense + splits / settlement            (history = source of truth)
                   │    → pairwise_balances += Δ, net_balance += Δ        (projection, atomic increments)
                   │    → balance_version += 1 → audit_events → outbox
                   ▼
              PostgreSQL ──► Outbox Publisher (1 leader) ──► Redis Stream group-events
                                                               ├─► Balance Projector → user_summary:{id} (home screen, O(1))
                                                               │                     → pub/sub group:{id} → API → SSE to open apps
                                                               └─► Notification Worker → inbox (retries + DLQ)
              Reconciler (periodic): replay history → compare with projection → report / repair drift
              GET /balances: Redis cache keyed by balance_version (never stale)
```

| Design component | Code |
|---|---|
| Split calculator (EQUAL / EXACT / PERCENTAGE / SHARES, penny-exact) | [src/domain/splitCalculator.js](src/domain/splitCalculator.js) |
| Ledger math (canonical pairwise rows, net positions, reversals, replay) | [src/domain/ledger.js](src/domain/ledger.js) |
| Debt simplification engine (heap greedy + exact optimum) | [src/domain/simplify.js](src/domain/simplify.js) |
| Money (integer minor units, currency conversion) | [src/domain/money.js](src/domain/money.js) |
| Transaction / consistency / idempotency core | [src/api/financialTx.js](src/api/financialTx.js) |
| Group, Expense, Settlement, Balance services | [src/api/service.js](src/api/service.js), [server.js](src/api/server.js) |
| Reconciliation | [src/shared/reconcile.js](src/shared/reconcile.js), [src/workers/reconciler.js](src/workers/reconciler.js) |
| Notifications / projections / outbox | [src/workers/](src/workers/) |
| Schema | [scripts/schema.sql](scripts/schema.sql) |

## Key design decisions

### Money is integers, splits are exact
- All amounts are BIGINT minor units (cents or paise). The API takes `total_amount: 120000` for $1,200.00 and never uses floats.
- Weighted splits use the **largest-remainder method** on exact BigInt math, which guarantees `sum(splits) === total` on every path. Leftover cents go to the largest fractional remainders. On ties they go to the **payer** first, so `$100 / 3` is $33.34 (payer), $33.33, $33.33.
- Percentages may have 2 decimals and must total exactly 100. Shares may be fractional, e.g. 1.5 nights.
- EXACT amounts must match the total to the cent. The error message tells you the difference.
- 2,000 randomized splits in the tests never lose or invent a cent.

### Two balance views, both kept current
- **Pairwise:** one row per pair, in canonical order `(user_low, user_high, low_to_high)`. A positive value means low owes high. There are never two mirrored rows that could disagree. Opposing debts offset automatically: Bob owes $400, Alice owes $200 back, so the row shows $200.
- **Net position:** `group_members.net_balance = paid − owed`. Nets always sum to zero inside a group.

`GET /balances` returns both, plus the simplified plan, with `default_view` following the
group's `simplify_debts` setting.

### Debt simplification: suggestions, never settlements
- **Greedy** (from the doc): max-heap of creditors and max-heap of debtors. It settles largest against largest and pushes back the remainder. O(n log n), at most n−1 transfers.
- **Greedy isn't always minimal.** For nets `{−20, −20, +40, −30, −30, +60}` it makes 5 transfers when 4 is optimal.
- **The true minimum** is `#non-zero − max #disjoint zero-sum subgroups`, found with a bitmask DP. It's used automatically for groups with up to 15 non-zero members (`OPTIMAL_SIMPLIFY_MAX`), and greedy above that.
- Simplification is computed when balances are read, not on every write, and cached by version.

### Consistency: one transaction, a per-group lock, atomic deltas
The doc warns about crashing after the expense rows are written but before the balances
change. Here the history rows, balance deltas, version bump, audit row and outbox event
commit **together or not at all**.
- `SELECT … FROM groups WHERE group_id = $1 FOR UPDATE` serializes financial writes **per group**, so there are no lost updates. Different groups never contend.
- Balances change by atomic SQL increments (`low_to_high = low_to_high + Δ`), never by reading, modifying and writing in the application.
- Rows are always updated in sorted order.
- The demo fires 40 concurrent expenses from 4 people. All commit, nets sum to 0, and a replay of the full history matches the projection exactly.

### Idempotency
`Idempotency-Key` on POST/PATCH is claimed **inside the same transaction**:
- A retry returns the stored response, with an `Idempotent-Replay: true` header.
- Concurrent duplicates block on the unique index, so 8 parallel retries in the demo create 1 expense.
- Reusing a key with a different body returns 422.

Payment webhooks are idempotent by `provider_ref`.

### Edits and deletes are reversals
- **Edit:** lock the expense, check the `version` (a stale version gets 409 `VERSION_CONFLICT`), **reverse the old splits' deltas, apply the new ones**, bump the version, and snapshot the revision into `expense_versions`. The audit log records old and new totals.
- **Delete:** a soft delete (`status = DELETED`, `deleted_at`) plus a reversal. **Restore** re-applies the splits. History is never lost.
- **Permissions:** an expense can be changed by its creator, its payer or a group admin.
- **Already-settled expenses:** balances aren't tied to individual expenses, so deleting a repaid expense makes the original payer owe the money back. That's an explicit, audited balance, not an inconsistency.

### Settlements: who confirms
| Recorded by | Method | Status | Balance changes when |
|---|---|---|---|
| Receiver | cash / bank / other | `CONFIRMED` | immediately (they acknowledge receiving it) |
| Payer | cash / bank / other | `PENDING` | the receiver confirms (or rejects) |
| Payer | UPI / card | `PENDING` | the payment provider's signed webhook says `SUCCEEDED` |

Only confirmed settlements touch balances, which prevents fake "I paid" claims. Partial
payments are just smaller amounts. A member can't leave a group with an open balance.

### Reads, caching and asynchronous work
| Read | How |
|---|---|
| Group balances | Redis key `balances:{group}:v{balance_version}`. Every write bumps the version, so there's no explicit invalidation and never a stale read |
| Home-screen totals ("you are owed $X") | `user_summary:{user}` in Redis, maintained by the projector. It **recomputes** from PostgreSQL instead of applying ±deltas, so redelivered events can't double-count |
| Live updates | projector → Redis pub/sub → API → Server-Sent Events (`GET /groups/:id/stream`) |
| Notifications | in-app inbox. Failures retry, then go to the dead-letter stream. The money is never blocked by them |
| History | keyset pagination on `(created_at, expense_id)` |

### Reconciliation
The reconciler replays ACTIVE expenses and CONFIRMED settlements and compares the result with
`pairwise_balances` and `net_balance`. It runs periodically for every group whose version
changed, and on demand via `GET /groups/:id/reconcile`. Repair (`?repair=true`) is admin-only
and audited. The demo corrupts one balance by a cent directly in the database, and
reconciliation detects and repairs it.

### Multi-currency
A group has one settlement currency. An expense entered in another currency carries an
`exchange_rate`, gets converted with exact BigInt math (half-up rounding), and stores the
original amount and rate. For example, ₹2,490.00 at 0.012 becomes $29.88.

## Data model
| Kind | Tables |
|---|---|
| Master data | `users`, `groups` (currency, `simplify_debts`, `balance_version`), `group_members` (role, `net_balance`, soft removal) |
| **Financial history (source of truth)** | `expenses` (split type + original input, status, version), `expense_splits` (final integer amount per user), `settlements` (status, method, `provider_ref`) |
| Derived projection | `pairwise_balances` (one canonical row per pair), `group_members.net_balance` |
| Supporting | `idempotency_keys`, `audit_events`, `expense_versions`, `outbox` |

## Run it

```bash
docker compose up -d --build
# Port clash? PG_HOST_PORT=5434 REDIS_HOST_PORT=6393 docker compose up -d --build

npm install
npm test                      # 14 unit tests: splits, ledger, simplification, money (no infra)
DATABASE_URL=postgres://splitwise:splitwise@localhost:5432/splitwise npm run demo   # end-to-end
docker compose logs -f notification-worker reconciler
```

The full endpoint list is in the header of [src/api/server.js](src/api/server.js). Example:

```bash
TOKEN=$(curl -s -XPOST localhost:3000/users -H 'content-type: application/json' -d '{"name":"Alice"}' | jq -r .token)
curl -XPOST localhost:3000/groups/$G/expenses -H "authorization: Bearer $TOKEN" -H 'Idempotency-Key: 7a2b-unique' \
  -H 'content-type: application/json' \
  -d '{"description":"Dinner","total_amount":120000,"split_type":"EQUAL","participants":[{"user_id":"..."}, ...]}'
curl localhost:3000/groups/$G/balances -H "authorization: Bearer $TOKEN"
```

### What the demo verifies (all passing)
| Scenario | Result |
|---|---|
| Doc example: Alice $1,200 dinner, Bob $600 taxi | Pairwise Bob→Alice $200, Charlie→Alice $400, Charlie→Bob $200. Nets +600 / 0 / −600. Simplified: **Charlie→Alice $600** |
| $100 / 3 | $33.33 / $33.33 / $33.34 (payer) |
| Percentage, shares, validation | 50/30/20 and 2/1/1 correct. Bad percentages, bad exact totals, fractional cents and non-members rejected. Non-members get 404 |
| Idempotency | Retry returns the same expense. 8 concurrent retries create 1 expense. A reused key with a different body gets 422 |
| Edit $1,200 → $1,500 | Alice +$200, Bob −$100, Charlie −$100. A stale version gets 409. A non-owner gets 403. Both revisions are stored |
| Delete, then restore | Reversed (record kept as DELETED), then balances are identical to before |
| Settlements | Payer's cash claim stays PENDING until the receiver confirms. Receiver's record is CONFIRMED at once. UPI: forged webhook gets 401, SUCCEEDED confirms, the duplicate webhook is ignored. A failed card payment leaves balances alone |
| Membership | Can't leave with a balance (409). Admin adds a member |
| INR expense in a USD group | ₹2,490.00 at 0.012 → $29.88 |
| 40 concurrent expenses | All committed, nets sum to 0, the projection matches a full replay |
| Pagination | 48 expenses across 7 pages, no duplicates or gaps |
| Cache | Hit on repeat. Any write gives a version bump and a miss |
| Async | Notifications in inboxes, home-screen total from Redis, 60 live SSE events |
| Reconciliation | An injected 1-cent drift was detected and repaired from history |

Bug found while building, and fixed: the pagination cursor first carried a JS Date
(millisecond precision) while PostgreSQL stores microseconds. Rows created in the same
millisecond, common under concurrency, were silently skipped. The cursor now carries
PostgreSQL's exact timestamp text.

## Scaling notes
- **Partition by group.** Every financial query is scoped to one group, so `group_id` is the natural shard key: ledger tables, projections and the outbox shard together. Cross-group views (the home screen) come from the projector's Redis summary, not cross-shard queries.
- **Per-group lock throughput.** One group's writes are serialized, which is fine: real groups see a few writes a minute. A hot "company-wide" group would need finer row locks on just the touched pairs.
- **Large groups:** the member cap is configurable. Simplification falls back from exact DP to O(n log n) greedy.
- **Scaling out:** the API is stateless. Workers scale as consumer groups, and outbox publishers run one leader plus standbys. Redis Streams map one-to-one onto Kafka topics.

## Not implemented / simplifications
- **Auth** is an HMAC token instead of OAuth/JWT. There's no API gateway or WAF.
- **Payments:** online payment is a simulated provider (checkout URL plus signed webhook).
- **Notifications** go to an in-app inbox; email and push are stubbed as log lines.
- **Offline-first clients** aren't covered beyond what the doc asks of the server, which is idempotent retries.
- **Multiple payers** per expense aren't supported (the doc's model has a single `paid_by`).
