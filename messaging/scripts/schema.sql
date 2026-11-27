-- PostgreSQL: relational + transactional data (NOT message history - that's Cassandra).

CREATE TABLE IF NOT EXISTS users (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS devices (
  id              TEXT PRIMARY KEY,
  user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  platform        TEXT NOT NULL DEFAULT 'unknown',
  push_token      TEXT,                       -- FCM / APNs token
  last_client_seq BIGINT NOT NULL DEFAULT 0,  -- highest client_seq accepted from this device
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS devices_user ON devices(user_id);

-- One row per chat. last_seq is the conversation sequence allocator: incrementing it
-- inside the accepting transaction gives a gap-free, strictly increasing conv_seq per chat.
CREATE TABLE IF NOT EXISTS conversations (
  id          TEXT PRIMARY KEY,               -- dm:<userA>:<userB> | grp:<uuid>
  type        TEXT NOT NULL CHECK (type IN ('direct', 'group')),
  name        TEXT,
  last_seq    BIGINT NOT NULL DEFAULT 0,
  created_by  TEXT REFERENCES users(id),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Membership + per-member receipt watermarks. Watermarks (not per-message status rows)
-- turn "delivered/read" into one small update per ack, regardless of how many messages it covers.
CREATE TABLE IF NOT EXISTS members (
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role            TEXT NOT NULL DEFAULT 'member',
  joined_seq      BIGINT NOT NULL DEFAULT 0,  -- members only see messages after they joined
  delivered_seq   BIGINT NOT NULL DEFAULT 0,  -- some device of this user has everything <= this
  read_seq        BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX IF NOT EXISTS members_user ON members(user_id);

-- Per-device sync cursor: "this device has every message <= synced_seq in this chat".
CREATE TABLE IF NOT EXISTS device_cursors (
  device_id       TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  synced_seq      BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (device_id, conversation_id)
);

-- Idempotency: a retried send (same client-generated message_id) returns the original result.
CREATE TABLE IF NOT EXISTS idempotency (
  message_id      UUID PRIMARY KEY,
  sender_device   TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  conv_seq        BIGINT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idempotency_created ON idempotency(created_at); -- for TTL cleanup

-- Transactional outbox: events are written in the SAME transaction as the state change,
-- then published to Kafka by the outbox publisher. No "committed but never published" gap.
CREATE TABLE IF NOT EXISTS outbox (
  id          BIGSERIAL PRIMARY KEY,
  topic       TEXT NOT NULL,
  key         TEXT NOT NULL,
  payload     JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
