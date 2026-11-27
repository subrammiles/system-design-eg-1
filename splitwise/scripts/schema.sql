-- Splitwise schema (PostgreSQL). Four kinds of data, kept separate:
--   master data      users, groups, group_members
--   financial history expenses, expense_splits, settlements      <- SOURCE OF TRUTH (never hard-deleted)
--   derived data     pairwise_balances, group_members.net_balance  <- fast projection, reconcilable
--   supporting data  idempotency_keys, audit_events, expense_versions, outbox
-- All money: BIGINT minor units (cents/paise). Never floats.

CREATE TABLE IF NOT EXISTS users (
  user_id     TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS groups (
  group_id         TEXT PRIMARY KEY,
  name             TEXT NOT NULL,
  currency         CHAR(3) NOT NULL,           -- one settlement currency per group
  simplify_debts   BOOLEAN NOT NULL DEFAULT true,
  balance_version  BIGINT NOT NULL DEFAULT 0,  -- bumped by every financial change: lock + cache key
  created_by       TEXT NOT NULL REFERENCES users(user_id),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_members (
  group_id     TEXT NOT NULL REFERENCES groups(group_id),
  user_id      TEXT NOT NULL REFERENCES users(user_id),
  role         TEXT NOT NULL DEFAULT 'MEMBER' CHECK (role IN ('ADMIN', 'MEMBER')),
  net_balance  BIGINT NOT NULL DEFAULT 0,      -- paid - owed (derived). > 0: should receive
  joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at   TIMESTAMPTZ,
  PRIMARY KEY (group_id, user_id)              -- a user can't be added twice
);
CREATE INDEX IF NOT EXISTS group_members_user ON group_members(user_id);

CREATE TABLE IF NOT EXISTS expenses (
  expense_id        TEXT PRIMARY KEY,
  group_id          TEXT NOT NULL REFERENCES groups(group_id),
  paid_by           TEXT NOT NULL REFERENCES users(user_id),
  description       TEXT NOT NULL,
  total_amount      BIGINT NOT NULL CHECK (total_amount > 0),  -- group currency, minor units
  currency          CHAR(3) NOT NULL,                         -- = group currency
  original_amount   BIGINT,                                   -- if entered in another currency
  original_currency CHAR(3),
  exchange_rate     NUMERIC(20, 8),
  split_type        TEXT NOT NULL CHECK (split_type IN ('EQUAL', 'EXACT', 'PERCENTAGE', 'SHARES')),
  split_input       JSONB NOT NULL,                           -- what the user asked for (percent/shares)
  status            TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DELETED')),
  version           INT NOT NULL DEFAULT 1,                   -- optimistic concurrency for edits
  expense_date      DATE NOT NULL DEFAULT CURRENT_DATE,
  created_by        TEXT NOT NULL REFERENCES users(user_id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ                               -- soft delete: history is never lost
);
CREATE INDEX IF NOT EXISTS expenses_group_page ON expenses(group_id, created_at DESC, expense_id DESC);

-- The exact amount each participant is responsible for (final integer amounts, any split type).
CREATE TABLE IF NOT EXISTS expense_splits (
  expense_id   TEXT NOT NULL REFERENCES expenses(expense_id),
  user_id      TEXT NOT NULL REFERENCES users(user_id),
  owed_amount  BIGINT NOT NULL CHECK (owed_amount >= 0),
  PRIMARY KEY (expense_id, user_id)
);
CREATE INDEX IF NOT EXISTS expense_splits_user ON expense_splits(user_id);

-- Every revision of an expense, for audit / dispute resolution.
CREATE TABLE IF NOT EXISTS expense_versions (
  expense_id  TEXT NOT NULL REFERENCES expenses(expense_id),
  version     INT NOT NULL,
  snapshot    JSONB NOT NULL,
  changed_by  TEXT NOT NULL,
  changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (expense_id, version)
);

CREATE TABLE IF NOT EXISTS settlements (
  settlement_id  TEXT PRIMARY KEY,
  group_id       TEXT NOT NULL REFERENCES groups(group_id),
  from_user      TEXT NOT NULL REFERENCES users(user_id),
  to_user        TEXT NOT NULL REFERENCES users(user_id),
  amount         BIGINT NOT NULL CHECK (amount > 0),
  method         TEXT NOT NULL DEFAULT 'CASH' CHECK (method IN ('CASH', 'BANK_TRANSFER', 'UPI', 'CARD', 'OTHER')),
  status         TEXT NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED', 'FAILED', 'CANCELLED')),
  provider_ref   TEXT UNIQUE,              -- payment provider transaction id (webhook idempotency)
  note           TEXT,
  created_by     TEXT NOT NULL REFERENCES users(user_id),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  confirmed_at   TIMESTAMPTZ,
  CHECK (from_user <> to_user)
);
CREATE INDEX IF NOT EXISTS settlements_group ON settlements(group_id, created_at DESC);

-- Derived: ONE row per pair (canonical order), never two mirrored rows that can disagree.
--   low_to_high > 0: user_low owes user_high;  < 0: user_high owes user_low.
CREATE TABLE IF NOT EXISTS pairwise_balances (
  group_id     TEXT NOT NULL REFERENCES groups(group_id),
  user_low     TEXT NOT NULL REFERENCES users(user_id),
  user_high    TEXT NOT NULL REFERENCES users(user_id),
  low_to_high  BIGINT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, user_low, user_high),
  CHECK (user_low < user_high)
);

-- A retried request (same Idempotency-Key) returns the original response instead of
-- creating a second expense / settlement.
CREATE TABLE IF NOT EXISTS idempotency_keys (
  user_id       TEXT NOT NULL,
  key           TEXT NOT NULL,
  request_hash  TEXT NOT NULL,
  status_code   INT NOT NULL,
  response      JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, key)
);

CREATE TABLE IF NOT EXISTS audit_events (
  id          BIGSERIAL PRIMARY KEY,
  group_id    TEXT NOT NULL,
  actor       TEXT NOT NULL,
  action      TEXT NOT NULL,     -- EXPENSE_CREATED, EXPENSE_UPDATED, EXPENSE_DELETED, SETTLEMENT_*, MEMBER_*
  entity_id   TEXT,
  detail      JSONB NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_group ON audit_events(group_id, id DESC);

-- Transactional outbox: events committed atomically with the financial change.
CREATE TABLE IF NOT EXISTS outbox (
  id          BIGSERIAL PRIMARY KEY,
  payload     JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
