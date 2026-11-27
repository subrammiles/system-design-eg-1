# Messaging System (WhatsApp-style chat)

Runnable implementation of the chat system design. It supports:
- 1:1 and group chat in real time over WebSockets;
- ✔ sent / ✔✔ delivered / blue ✔✔ read receipts;
- offline delivery with sync, and push notifications;
- multi-device, typing indicators, presence and media.

The core guarantee: **a message is never lost, never duplicated, and every member sees the same order.**

**Stack, as in the design:**
- Node.js WebSocket gateways (the doc suggests Go; same role);
- PostgreSQL for users, membership, sequences, idempotency and the outbox;
- Cassandra for message history;
- Kafka for events;
- Redis for presence, the device→gateway registry and gateway pub/sub;
- S3 for media.

All of it runs in docker-compose.

**Target scale from the design:**
- 100M users, 20M DAU, 5M concurrent connections;
- 1B messages/day, which is about 11.6K msg/s on average and about 40K msg/s at peak;
- about 20 TB/day of new media.

## Architecture

```
 client (local SQLite: pending sends, message_id dedupe, cursors)
   │ WebSocket (backoff + jitter reconnect)
   ▼
 ALB/NLB ─► Gateway ×N ── sockets in memory · heartbeats · rate limit
              │   ▲  └──► Redis: conn:{user} device→gateway · presence TTL
              │   └────── Redis pub/sub gw:{id}  ◄──────────────────────────────┐
              ▼ (gRPC in prod; HTTP here)                                       │
        Messaging Service ── BEGIN                                              │
              │               idempotency(message_id) · client_seq == last+1    │
              │               conversations.last_seq += 1   → conv_seq          │
              │               INSERT outbox · INSERT Cassandra (history)        │
              │             COMMIT → ACK sender ✔                               │
              ▼                                                                 │
        PostgreSQL outbox ─► Outbox Publisher (1 leader) ─► Kafka message-events │
                                                            │ key=conversation  │
                                                            ▼                   │
                                              Delivery Workers ×N ──────────────┘
                                                │ online device? → its gateway
                                                │ big group → fanout-chunks topic
                                                └ offline → push-notifications ─► Notification Worker (FCM/APNs)

 Reconnect: client ─sync(cursors)─► Messaging Service ─► Cassandra range per conversation
 Media:     client ─presigned PUT/GET─► S3 (bytes never pass through chat servers)
```

| Design component | Code |
|---|---|
| Connection gateway | [src/gateway/server.js](src/gateway/server.js) |
| Messaging service (accept, receipts, sync, typing) | [src/messaging-service/chatService.js](src/messaging-service/chatService.js), [server.js](src/messaging-service/server.js) |
| Conversation sequencer, idempotency, outbox | `acceptMessage()` in [chatService.js](src/messaging-service/chatService.js), [scripts/schema.sql](scripts/schema.sql) |
| Message store (Cassandra) | [src/shared/messageStore.js](src/shared/messageStore.js) |
| Outbox publisher | [src/outbox-publisher/worker.js](src/outbox-publisher/worker.js) |
| Delivery worker (routing, fan-out) | [src/delivery-worker/worker.js](src/delivery-worker/worker.js) |
| Notification worker | [src/notification-worker/worker.js](src/notification-worker/worker.js) |
| Presence + connection registry | [src/shared/presence.js](src/shared/presence.js) |
| Media service (presigned S3) | `/media/*` in [server.js](src/messaging-service/server.js) |
| Client (local store, retries, ordering, receipts) | [client/chatClient.js](client/chatClient.js) |

## Key design decisions

### 1. Store, then deliver, then ACK
`acceptMessage()` runs as one PostgreSQL transaction:

1. Check the idempotency table for this `message_id`.
2. Check that `client_seq` is the next one expected from this device.
3. Allocate `conv_seq`.
4. Insert the outbox event.
5. **Write the message to Cassandra while the transaction is still open.**
6. COMMIT.

Only after the commit does the sender get `send_ack` (✔). So an ACK means the message is in
PostgreSQL *and* Cassandra, and will reach Kafka through the outbox.

| Failure | Outcome |
|---|---|
| Cassandra write fails | ROLLBACK. The seq isn't consumed (no gaps), nothing is published, and the client retries |
| COMMIT fails after the Cassandra write | The orphan row's seq is reused by the next message, and the idempotent upsert overwrites it |
| ACK lost on the way back | The client retransmits the same `message_id`, and the server replies `duplicate: true` with the original seq |
| Crash between Kafka publish and outbox delete | The event is re-published. Consumers are idempotent |

The trade-off is that one chat's sequence row stays locked for a single Cassandra write.
That caps one conversation at a few hundred msg/s, while different conversations don't
contend at all.

### 2. Ordering: three identifiers, three jobs
| Field | Assigned by | Purpose |
|---|---|---|
| `message_id` (UUID) | client | Identity and dedupe at all three layers: sender retry, server idempotency, receiver's local store |
| `client_seq` | client (persisted per device) | Server accepts a device's messages **only in order** (`CLIENT_SEQ_GAP` otherwise), so "Msg1 lost, Msg2 shown" can't happen |
| `conv_seq` | server (`conversations.last_seq + 1` in the transaction) | The single order for the chat, gap-free. Every device sorts by it |

Timestamps are never used for ordering, because of clock skew. Clients track the highest
*contiguous* `conv_seq` per chat. A hole (getting 7 while holding up to 5) triggers a sync.

### 3. At-least-once delivery plus dedupe, never exactly-once
- **Real-time path:** Delivery Worker → Redis pub/sub → gateway. This is fire-and-forget on purpose: the message is already durable.
- **Recovery path:** whatever a device misses (offline, mid-reconnect, gateway crash) it gets back through **sync**, which reads from its cursor up to `last_seq` in Cassandra.
- **Redelivery:** duplicates are dropped by `message_id` on the client.

### 4. Receipts are watermarks, not per-message rows
Each member row holds `delivered_seq` and `read_seq`. A client sends a cumulative
`ack`/`read` with its highest contiguous seq. That's one small update however many messages
it covers, and a receipt event is sent only when the watermark actually advances.

The sender's UI computes the ticks:
- **✔✔ DELIVERED** when every other member's `delivered_seq` is at least the message's seq;
- **READ** when every other member's `read_seq` is.

Reading on your phone also clears unread on your laptop, because receipts go to all of a
user's devices.

### 5. Gateways: connections only, no history
- The live sockets stay in gateway memory. Redis holds only `device → gateway` and a 60 s presence TTL, refreshed on every pong.
- A missed heartbeat terminates a dead socket.
- Each connection has a send rate limit and a frame size cap.
- On shutdown a gateway closes its sockets with 1012. Clients then reconnect with exponential backoff and jitter, which avoids a thundering herd.
- A device connecting a second time replaces its old socket.

### 6. Fan-out
- **Storage is fan-out-on-read:** one copy per conversation in Cassandra plus per-member cursors. There are never per-member inbox copies.
- **Real-time push** goes to every online device of every member, including the sender's other devices.
- **Big groups** (over `LARGE_GROUP_THRESHOLD`) work differently:
  - the recipient list is split into chunks on the `fanout-chunks` topic, so the whole worker group shares one celebrity-sized fan-out;
  - receipts are not broadcast, and there's no push storm.

### 7. Offline users and push
A user with no online device gets a push-notification event. The notification worker:
- collapses bursts to one push per user per chat every 10 s;
- sends no message content (with E2EE the server couldn't read it anyway).

Push only wakes the app, and the app then syncs. FCM/APNs is never the source of truth.

### 8. Database choices (from the design)
| Data | Store | Why |
|---|---|---|
| Message history | Cassandra, `PRIMARY KEY ((conversation_id, bucket_id), conv_seq)` | Write-heavy, append-only, and "last N of chat X" is one partition read |
| Users, devices, membership, sequences, idempotency, outbox | PostgreSQL | Transactions and constraints. The outbox must commit atomically with the message |
| Presence, device→gateway, member cache, push collapse | Redis | Ephemeral, TTL-driven, very hot |
| Events | Kafka (keyed by `conversation_id`) | Durable, replayable, ordered per chat, consumers scale independently |
| Media | S3 (+ CloudFront) | Presigned upload/download URLs |

Messages are partitioned **by conversation, not `message_id`**, so opening a chat doesn't
fan out across shards. **Sequence-range buckets** (seq 0–999 → bucket 0, and so on) cap
partition size for hyperactive groups. History paging walks back through buckets.

## Run it

```bash
docker compose up -d --build          # ~1-2 min: Cassandra takes a while to boot
# Port clash? PG_HOST_PORT=5433 REDIS_HOST_PORT=6392 docker compose up -d --build

npm install
npm run demo                           # end-to-end scenario below, asserts every step
npm test                               # unit tests (no infra)
docker compose logs -f notification-worker   # push notifications for offline users
```

### Protocol
WebSocket: `ws://localhost:8081?token=<device token>` (gateway-1), or `:8082` (gateway-2).

| Client → server | Server → client |
|---|---|
| `{type:'send', message_id, conversation_id, client_seq, body?, media?}` | `send_ack {message_id, conv_seq, duplicate}` / `send_error {code, retryable, expected?}` |
| `{type:'ack' \| 'read', conversation_id, conv_seq}` (cumulative) | `message {...}` |
| `{type:'typing', conversation_id}` | `receipt {conversation_id, user_id, delivered_seq, read_seq}` |
| `{type:'sync', cursors: {conversation_id: seq}}` | `sync {conversations:[{messages, has_more}], receipts}`, `typing`, `hello` |

REST is on `:3001`: users and devices, conversations, history, presence, and media
presigned URLs. See the header of [server.js](src/messaging-service/server.js).

### What the demo verifies (all passing)
| Scenario | Result |
|---|---|
| 3 messages, sender on gw-1, receiver on gw-2 | Arrive in order (conv_seq 1, 2, 3). The sender's laptop gets copies |
| Receipts | ✔ → ✔✔ after the receiver's ack → READ after `read`. One watermark covers all 3 |
| Same `message_id` re-sent | `duplicate: true`, original conv_seq, receiver still has 3 |
| `client_seq` skips ahead | `CLIENT_SEQ_GAP`, with the expected seq |
| Typing | Delivered live, not stored |
| Group with an offline member | Message stays ✔ SENT. Push sent. Member connects, syncs both in order, and the message becomes ✔✔ |
| Receiver's network drops | Messages sent meanwhile arrive after auto-reconnect and sync |
| Sender offline | Messages queue as PENDING locally and flush in order on reconnect |
| 3 senders × 30 concurrent messages | All 4 devices show the identical order. conv_seq 1..96 has no gaps. Each sender's order is kept |
| History paging | Crosses Cassandra bucket partitions (demo uses `BUCKET_SIZE=50`) |
| Member added late | Sees only messages after joining |
| Media | Presigned PUT to S3, the message carries the key, the receiver downloads identical bytes |

Also verified by hand:
- **Outbox leader killed:** the standby took over and the full demo still passed.
- **Gateway-2 restarted mid-chat:** the client got 1012, reconnected with backoff, and received the 3 messages sent during the restart via sync (~1.3 s).

## Scaling (from the design, with planning numbers)
| Component | Sizing at 5M concurrent / 40K msg/s peak |
|---|---|
| Gateways | 5M ÷ ~75K conns each ≈ 67, so **85–100** with headroom |
| Cassandra | 40K writes/s at RF=3, with compaction, repair and history reads: **15–20 nodes** |
| Kafka | ~40 MB/s before replication: **8–10 brokers**. Partitions keyed by conversation |
| Redis | Presence for active sessions only (~4 GB raw): **3–6 shards** + replicas |
| PostgreSQL | Multi-AZ primary + replicas. Partition `outbox`/`idempotency` by time. Shard by `conversation_id` when the sequence-row write rate demands it |

Scale out by adding gateways (stateless apart from sockets), delivery workers (Kafka
consumer group), messaging-service replicas (stateless), and outbox publishers (standbys
for failover).

## Not implemented / simplifications
- **End-to-end encryption** (Signal protocol: X3DH + Double Ratchet). `body` is plaintext here. In production it would be an opaque ciphertext blob per recipient device, and the server never reads it.
- **Internal transport:** gateway → messaging service uses HTTP/JSON instead of gRPC.
- **One PostgreSQL primary** handles sequence allocation and the outbox. At the doc's peak scale that write rate needs sharding by `conversation_id`. Idempotency and outbox rows also need TTL cleanup jobs.
- **Presence fan-out** to contacts isn't pushed. Clients query `/presence/:user`.
- **Auth** is an HMAC device token instead of phone OTP + JWT. There's no WAF, CDN or multi-region setup here.
- **Infra stand-ins:** `adobe/s3mock` replaces S3, and Kafka/Cassandra run as single nodes with RF=1.
