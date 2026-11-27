// Reference client: what the mobile/web app does on its side of the protocol.
// (A real app keeps this state in SQLite; here it's in memory with the same structure.)
//
//   send():  client_seq++ (persisted) + UUID message_id -> local row PENDING -> transmit
//            -> wait for send_ack (✔ SENT). No ack within ackTimeout -> retransmit, same
//            message_id (the server dedupes). Unacked messages are re-sent in client_seq order
//            after every reconnect.
//   receive: dedupe by message_id, place by conv_seq, track the highest CONTIGUOUS seq per chat.
//            A hole (got 7, have up to 5) -> sync. Ack the contiguous seq (cumulative ack).
//   connect: reconnect with exponential backoff + jitter (no thundering herd), then
//            sync from local cursors.
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';

export function backoffDelay(attempt, { baseMs = 500, maxMs = 30_000, random = Math.random } = {}) {
  const exp = Math.min(maxMs, baseMs * 2 ** attempt);
  return Math.round(exp / 2 + random() * (exp / 2)); // "equal jitter"
}

export class LocalStore {
  constructor() {
    this.clientSeq = 0;           // persisted per device
    this.messages = new Map();    // conversation_id -> Map(conv_seq -> message)
    this.seenIds = new Set();     // message_id dedupe (receiver-side idempotency)
    this.cursors = {};            // conversation_id -> highest contiguous conv_seq held
    this.pending = new Map();     // message_id -> outgoing message not yet acked
    this.receipts = {};           // conversation_id -> user_id -> {delivered_seq, read_seq}
  }

  /** @returns {'new'|'duplicate'} and whether there is a gap before it */
  insert(m) {
    if (this.seenIds.has(m.message_id)) {
      // Our own message coming back via another path, or a redelivery. Still make sure it's placed.
      if (!this.messages.get(m.conversation_id)?.has(m.conv_seq)) this.place(m);
      return { result: 'duplicate', gap: this.hasGap(m.conversation_id) };
    }
    this.seenIds.add(m.message_id);
    this.place(m);
    return { result: 'new', gap: this.hasGap(m.conversation_id) };
  }

  place(m) {
    if (!this.messages.has(m.conversation_id)) this.messages.set(m.conversation_id, new Map());
    const conv = this.messages.get(m.conversation_id);
    conv.set(m.conv_seq, m);
    let c = this.cursors[m.conversation_id] ?? 0;
    while (conv.has(c + 1)) c++;
    this.cursors[m.conversation_id] = c;
  }

  setCursorFloor(conversationId, seq) {
    // e.g. joined a group at seq 40: nothing before 41 will ever arrive.
    if ((this.cursors[conversationId] ?? 0) < seq) this.cursors[conversationId] = seq;
    const conv = this.messages.get(conversationId);
    let c = this.cursors[conversationId];
    while (conv?.has(c + 1)) c++;
    this.cursors[conversationId] = c;
  }

  hasGap(conversationId) {
    const conv = this.messages.get(conversationId);
    if (!conv) return false;
    const max = Math.max(...conv.keys());
    return max > (this.cursors[conversationId] ?? 0);
  }

  timeline(conversationId) {
    return [...(this.messages.get(conversationId)?.values() ?? [])].sort((a, b) => a.conv_seq - b.conv_seq);
  }
}

export class ChatClient extends EventEmitter {
  constructor({ url, token, userId, deviceId, ackTimeoutMs = 3000, autoReconnect = true }) {
    super();
    Object.assign(this, { url, token, userId, deviceId, ackTimeoutMs, autoReconnect });
    this.store = new LocalStore();
    this.members = {};            // conversation_id -> [user ids]   (from REST /conversations)
    this.ws = null;
    this.attempt = 0;
    this.closedByUser = false;
    this.syncTimer = null;
    this.retryTimers = new Map();
  }

  setConversations(list) {
    for (const c of list) {
      this.members[c.conversation_id] = c.members;
      if (c.joined_seq) this.store.setCursorFloor(c.conversation_id, c.joined_seq);
    }
  }

  connect() {
    this.closedByUser = false;
    return new Promise((resolve) => {
      const ws = new WebSocket(`${this.url}?token=${encodeURIComponent(this.token)}`);
      this.ws = ws;
      ws.on('message', (data) => this.onFrame(JSON.parse(data)));
      ws.once('open', () => {
        this.attempt = 0;
        this.sync();
        // Re-send everything unacked, oldest first: the server accepts a device's messages
        // strictly in client_seq order.
        for (const p of [...this.store.pending.values()].sort((a, b) => a.client_seq - b.client_seq)) this.transmit(p);
        this.emit('connected');
        resolve();
      });
      ws.on('close', (code) => {
        this.emit('disconnected', code);
        if (this.closedByUser || !this.autoReconnect) return;
        const delay = backoffDelay(this.attempt++);
        setTimeout(() => this.connect().catch(() => {}), delay);
      });
      ws.on('error', () => {}); // 'close' follows
    });
  }

  close() {
    this.closedByUser = true;
    for (const t of this.retryTimers.values()) clearTimeout(t);
    clearTimeout(this.syncTimer);
    this.ws?.close();
  }

  /** Simulate losing the network without a clean close (server notices via heartbeat). */
  dropConnection() {
    this.ws?.terminate();
  }

  frame(f) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(f));
      return true;
    }
    return false;
  }

  /** Queue a message locally (PENDING) and send it. */
  send(conversationId, body, { media, messageId = randomUUID() } = {}) {
    const msg = {
      message_id: messageId, conversation_id: conversationId, client_seq: ++this.store.clientSeq,
      body, media, sender_id: this.userId, status: 'PENDING',
    };
    this.store.pending.set(msg.message_id, msg);
    this.transmit(msg);
    return msg;
  }

  transmit(msg) {
    clearTimeout(this.retryTimers.get(msg.message_id));
    this.frame({ type: 'send', ...msg, status: undefined, sender_id: undefined });
    // No ACK in time -> retransmit the SAME message_id (safe: server-side idempotency).
    msg.attempts = (msg.attempts ?? 0) + 1;
    this.retryTimers.set(msg.message_id, setTimeout(() => {
      if (this.store.pending.has(msg.message_id)) this.transmit(msg);
    }, Math.min(this.ackTimeoutMs * msg.attempts, 15_000)));
  }

  typing(conversationId) {
    this.frame({ type: 'typing', conversation_id: conversationId });
  }

  markRead(conversationId) {
    this.frame({ type: 'read', conversation_id: conversationId, conv_seq: this.store.cursors[conversationId] ?? 0 });
  }

  sync() {
    clearTimeout(this.syncTimer);
    this.syncTimer = null;
    this.frame({ type: 'sync', cursors: this.store.cursors });
  }

  requestSyncSoon() {
    if (!this.syncTimer) this.syncTimer = setTimeout(() => this.sync(), 200);
  }

  ackDelivered(conversationId) {
    this.frame({ type: 'ack', conversation_id: conversationId, conv_seq: this.store.cursors[conversationId] ?? 0 });
  }

  receive(m, via) {
    const { result, gap } = this.store.insert(m);
    if (result === 'new') this.emit('message', m, via);
    else this.emit('duplicate', m, via);
    if (gap) this.requestSyncSoon();
    return result;
  }

  onFrame(f) {
    switch (f.type) {
      case 'hello':
        this.emit('hello', f);
        break;
      case 'send_ack': {
        const p = this.store.pending.get(f.message_id);
        if (!p) break;
        this.store.pending.delete(f.message_id);
        clearTimeout(this.retryTimers.get(f.message_id));
        const sent = { ...p, conv_seq: f.conv_seq, created_at: f.created_at, sender_device: this.deviceId, status: 'SENT' };
        this.store.insert(sent);
        this.emit('sent', sent, f.duplicate);
        break;
      }
      case 'send_error': {
        const p = this.store.pending.get(f.message_id);
        if (!p) break;
        this.emit('send_error', p, f);
        if (f.code === 'CLIENT_SEQ_GAP') break; // an earlier message is still in flight; our timer retries
        if (!f.retryable) {
          this.store.pending.delete(f.message_id);
          clearTimeout(this.retryTimers.get(f.message_id));
          p.status = 'FAILED';
        }
        break;
      }
      case 'message':
        this.receive(f, 'push');
        this.ackDelivered(f.conversation_id);
        break;
      case 'sync': {
        for (const r of f.receipts ?? []) this.applyReceipt(r);
        const touched = new Set();
        for (const conv of f.conversations) {
          for (const m of conv.messages) this.receive(m, 'sync');
          touched.add(conv.conversation_id);
          if (conv.has_more) this.requestSyncSoon();
        }
        for (const c of touched) this.ackDelivered(c);
        this.emit('synced', f);
        break;
      }
      case 'receipt':
        this.applyReceipt(f);
        this.emit('receipt', f);
        break;
      case 'typing':
        this.emit('typing', f);
        break;
      default:
        this.emit(f.type, f);
    }
  }

  applyReceipt(r) {
    const conv = (this.store.receipts[r.conversation_id] ??= {});
    const prev = conv[r.user_id] ?? { delivered_seq: 0, read_seq: 0 };
    conv[r.user_id] = {
      delivered_seq: Math.max(prev.delivered_seq, r.delivered_seq),
      read_seq: Math.max(prev.read_seq, r.read_seq),
    };
  }

  /** ✔ SENT, ✔✔ DELIVERED (every other member has it), blue ✔✔ READ (every other member read it). */
  statusOf(message) {
    if (message.conv_seq == null) return message.status ?? 'PENDING';
    const others = (this.members[message.conversation_id] ?? []).filter((u) => u !== this.userId);
    if (!others.length) return 'SENT';
    const r = this.store.receipts[message.conversation_id] ?? {};
    if (others.every((u) => (r[u]?.read_seq ?? 0) >= message.conv_seq)) return 'READ';
    if (others.every((u) => (r[u]?.delivered_seq ?? 0) >= message.conv_seq)) return 'DELIVERED';
    return 'SENT';
  }
}
