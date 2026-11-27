// Connection Gateway: owns live WebSocket connections, never durable message history.
//
//   client ⇄ WebSocket ⇄ gateway ──HTTP (gRPC in prod)──► Messaging Service
//                          ▲
//                          └── Redis pub/sub channel gw:{id} ◄── Delivery Worker
//
// Responsibilities:
//   - authenticate the socket (device token) and register device -> gateway in Redis
//   - heartbeats (ping/pong): detect dead connections, refresh presence TTL
//   - forward client frames (send / ack / read / typing / sync) to the Messaging Service
//   - push frames published to this gateway's channel to the right local device sockets
//   - per-connection rate limiting, max frame size
//   - graceful drain on shutdown (clients reconnect elsewhere with backoff + jitter)
//
// Client frames:  {type:'send', message_id, conversation_id, client_seq, body?, media?}
//                 {type:'ack'|'read', conversation_id, conv_seq}
//                 {type:'typing', conversation_id}     {type:'sync', cursors?: {conv: seq}}
// Server frames:  hello, send_ack, send_error, message, receipt, typing, sync, error
import http from 'node:http';
import os from 'node:os';
import { WebSocketServer } from 'ws';
import { verifyToken } from '../shared/auth.js';
import { config } from '../shared/config.js';
import { createRedis } from '../shared/clients.js';
import { gatewayChannel, Presence } from '../shared/presence.js';

const GATEWAY_ID = process.env.GATEWAY_ID ?? `gw-${os.hostname()}`;
const { gateway: gw, messaging } = config;

const redis = createRedis();
const subscriber = createRedis('redis-sub');
const presence = new Presence(redis, { ttlSeconds: gw.presenceTtlSeconds });

/** deviceId -> WebSocket (one live socket per device; a new one replaces the old) */
const sockets = new Map();
let draining = false;

async function callMessaging(path, body) {
  const res = await fetch(`${messaging.url}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-internal-token': config.authSecret },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(json.error ?? `HTTP ${res.status}`);
    Object.assign(err, { status: res.status, payload: json });
    throw err;
  }
  return json;
}

const send = (ws, frame) => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(frame));
};

// Token bucket per connection for 'send' frames (anti-spam / abuse).
function allowSend(ws) {
  const now = Date.now();
  const rate = gw.sendRatePerSecond;
  ws.tokens = Math.min(rate * 2, (ws.tokens ?? rate * 2) + ((now - (ws.lastRefill ?? now)) / 1000) * rate);
  ws.lastRefill = now;
  if (ws.tokens < 1) return false;
  ws.tokens -= 1;
  return true;
}

async function handleFrame(ws, frame) {
  const who = { userId: ws.userId, deviceId: ws.deviceId };
  switch (frame.type) {
    case 'send': {
      if (!allowSend(ws)) {
        return send(ws, { type: 'send_error', message_id: frame.message_id, code: 'RATE_LIMITED', retryable: true });
      }
      try {
        const r = await callMessaging('/internal/messages', { ...frame, ...who });
        return send(ws, { type: 'send_ack', ...r });
      } catch (err) {
        const p = err.payload ?? {};
        return send(ws, {
          type: 'send_error', message_id: frame.message_id, code: p.code ?? 'UNAVAILABLE', error: p.error ?? err.message,
          retryable: p.retryable ?? true, expected: p.expected,
        });
      }
    }
    case 'ack':
    case 'read': {
      const r = await callMessaging('/internal/ack', {
        ...who, conversation_id: frame.conversation_id, conv_seq: frame.conv_seq, kind: frame.type === 'read' ? 'read' : 'delivered',
      });
      return send(ws, { type: `${frame.type}_ok`, conversation_id: frame.conversation_id, conv_seq: r.conv_seq ?? r.delivered_seq });
    }
    case 'typing': {
      const key = `typing:${frame.conversation_id}`;
      if (ws[key] && Date.now() - ws[key] < 2000) return; // throttle typing spam
      ws[key] = Date.now();
      await callMessaging('/internal/typing', { ...who, conversation_id: frame.conversation_id });
      return;
    }
    case 'sync': {
      const r = await callMessaging('/internal/sync', { ...who, cursors: frame.cursors ?? {} });
      return send(ws, { type: 'sync', ...r });
    }
    default:
      return send(ws, { type: 'error', code: 'UNKNOWN_TYPE', error: `unknown frame type ${frame.type}` });
  }
}

// ---- server ----
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(draining ? 503 : 200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ gateway: GATEWAY_ID, connections: sockets.size, draining }));
  }
  res.writeHead(404).end();
});

const wss = new WebSocketServer({ noServer: true, maxPayload: gw.maxFrameBytes });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  const token = url.searchParams.get('token') ?? (req.headers.authorization ?? '').replace(/^Bearer /i, '');
  const identity = verifyToken(token, config.authSecret);
  if (draining || !identity) {
    socket.write(`HTTP/1.1 ${draining ? '503 Service Unavailable' : '401 Unauthorized'}\r\n\r\n`);
    return socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, identity));
});

wss.on('connection', (ws, { userId, deviceId }) => {
  Object.assign(ws, { userId, deviceId, isAlive: true });

  const previous = sockets.get(deviceId);
  if (previous) previous.close(4000, 'replaced by a newer connection');
  sockets.set(deviceId, ws);

  // Handlers are attached synchronously (a client may send 'sync' the instant the socket
  // opens - frames emitted before a listener exists would be lost). Processing waits for
  // the registration via the per-socket chain instead.
  // Frames from one socket are processed in order (a client's sends keep their order).
  let chain = presence.register(userId, deviceId, GATEWAY_ID)
    .then(() => send(ws, { type: 'hello', gateway: GATEWAY_ID, user_id: userId, device_id: deviceId }))
    .catch((err) => console.error('[gateway] presence register failed:', err.message));

  ws.on('pong', () => {
    ws.isAlive = true;
    presence.refresh(userId, deviceId, GATEWAY_ID).catch(() => {});
  });

  ws.on('message', (data) => {
    let frame;
    try {
      frame = JSON.parse(data);
    } catch {
      return send(ws, { type: 'error', code: 'BAD_JSON' });
    }
    chain = chain.then(() => handleFrame(ws, frame)).catch((err) => {
      send(ws, { type: 'error', code: err.payload?.code ?? 'FAILED', error: err.payload?.error ?? err.message, ref: frame.type });
    });
  });

  ws.on('close', () => {
    if (sockets.get(deviceId) === ws) {
      sockets.delete(deviceId);
      presence.unregister(userId, deviceId, GATEWAY_ID).catch(() => {});
    }
  });
});

// Heartbeats: a socket that missed the previous ping is dead (half-open TCP, phone lost signal).
setInterval(() => {
  for (const ws of sockets.values()) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, gw.heartbeatMs).unref();

// Deliveries for devices connected here.
await subscriber.subscribe(gatewayChannel(GATEWAY_ID));
subscriber.on('message', (_channel, raw) => {
  try {
    const { device_id: deviceId, frame } = JSON.parse(raw);
    const ws = sockets.get(deviceId);
    if (ws) send(ws, frame);
    // else: device just disconnected - it will catch up via sync on reconnect.
  } catch (err) {
    console.error('[gateway] bad delivery payload', err.message);
  }
});

async function drain() {
  if (draining) return;
  draining = true;
  console.log(`[gateway ${GATEWAY_ID}] draining ${sockets.size} connections`);
  for (const ws of sockets.values()) ws.close(1012, 'service restart'); // clients reconnect with jitter
  await Promise.all([...sockets.values()].map((ws) => presence.unregister(ws.userId, ws.deviceId, GATEWAY_ID).catch(() => {})));
  setTimeout(() => process.exit(0), 500);
}
process.on('SIGTERM', drain);
process.on('SIGINT', drain);

server.listen(gw.port, () => console.log(`[gateway ${GATEWAY_ID}] listening on :${gw.port}`));
