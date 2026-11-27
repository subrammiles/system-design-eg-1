// HTTP API (API gateway concerns - auth, idempotency header, errors - plus the services).
//
//   POST   /users                                  {name, email?}  -> {user_id, token}
//   GET    /me/groups | /me/summary | /me/notifications
//   POST   /groups                                 {name, currency, members[], simplify_debts?}
//   GET    /groups/:id
//   POST   /groups/:id/members                     {user_id}        (admin)
//   DELETE /groups/:id/members/:userId                              (must be settled up)
//   POST   /groups/:id/expenses       Idempotency-Key   {description, total_amount, paid_by?, split_type?, participants?, currency?, exchange_rate?}
//   GET    /groups/:id/expenses?cursor=&limit=&include_deleted=
//   GET    /expenses/:id                           (with version history)
//   PATCH  /expenses/:id               {version, ...changes}    (reverse + re-apply)
//   DELETE /expenses/:id?version=                  (soft delete + reverse)
//   POST   /expenses/:id/restore
//   POST   /groups/:id/settlements    Idempotency-Key   {from_user, to_user, amount, method?, note?}
//   GET    /groups/:id/settlements
//   POST   /settlements/:id/confirm | /reject | /cancel
//   POST   /webhooks/payments          X-Signature: hmac(body)   {provider_ref, outcome: SUCCEEDED|FAILED}
//   GET    /groups/:id/balances                    pairwise + simplified (cached by balance_version)
//   GET    /groups/:id/activity                    audit log
//   GET    /groups/:id/reconcile?repair=true
//   GET    /groups/:id/stream                      Server-Sent Events: live group updates
import express from 'express';
import { createHmac, timingSafeEqual } from 'node:crypto';
import { verifyToken } from '../shared/auth.js';
import { config } from '../shared/config.js';
import { createPg, createRedis, retry } from '../shared/clients.js';
import { ApiError } from './financialTx.js';
import { computeUserSummary, SplitwiseService } from './service.js';

const pool = createPg();
await retry('postgres', () => pool.query('SELECT 1'));
const redis = createRedis();
const subscriber = createRedis('redis-sub');
const svc = new SplitwiseService({ pool, redis, config });

const app = express();
app.use(express.json({
  limit: '256kb',
  verify: (req, _res, buf) => { req.rawBody = buf; }, // for webhook signatures
}));
const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);
/** Financial writes return {status, body} (possibly a replayed idempotent response). */
const send = (res, out) => {
  if (out.replayed) res.set('Idempotent-Replay', 'true');
  res.status(out.status).json(out.body);
};

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

function auth(req, _res, next) {
  const token = (req.get('authorization') ?? '').replace(/^Bearer /i, '') || req.query.access_token; // SSE can't set headers
  req.userId = verifyToken(token, config.authSecret);
  if (!req.userId) return next(new ApiError(401, 'UNAUTHORIZED', 'valid token required'));
  next();
}
const idem = (req) => req.get('Idempotency-Key') ?? undefined;

// ---- users (registration stands in for a real identity provider) ----
app.post('/users', wrap(async (req, res) => res.status(201).json(await svc.createUser(req.body ?? {}))));
app.get('/me/groups', auth, wrap(async (req, res) => res.json(await svc.myGroups(req.userId))));
app.get('/me/summary', auth, wrap(async (req, res) => {
  // O(1) read-optimized view maintained by the balance projector; computed on a miss.
  const cached = await redis.get(`user_summary:${req.userId}`).catch(() => null);
  res.json(cached ? { ...JSON.parse(cached), source: 'cache' } : { ...(await computeUserSummary(pool, req.userId)), source: 'db' });
}));
app.get('/me/notifications', auth, wrap(async (req, res) =>
  res.json((await redis.lrange(`notifications:${req.userId}`, 0, 49)).map((n) => JSON.parse(n)))));

// ---- groups ----
app.post('/groups', auth, wrap(async (req, res) => res.status(201).json(await svc.createGroup(req.userId, req.body ?? {}))));
app.get('/groups/:id', auth, wrap(async (req, res) => res.json(await svc.getGroup(req.userId, req.params.id))));
app.post('/groups/:id/members', auth, wrap(async (req, res) => send(res, await svc.addMember(req.userId, req.params.id, req.body?.user_id))));
app.delete('/groups/:id/members/:userId', auth, wrap(async (req, res) => send(res, await svc.removeMember(req.userId, req.params.id, req.params.userId))));

// ---- expenses ----
app.post('/groups/:id/expenses', auth, wrap(async (req, res) => send(res, await svc.createExpense(req.userId, req.params.id, req.body, idem(req)))));
app.get('/groups/:id/expenses', auth, wrap(async (req, res) => res.json(await svc.listExpenses(req.userId, req.params.id, {
  cursor: req.query.cursor, limit: req.query.limit, includeDeleted: req.query.include_deleted === 'true',
}))));
app.get('/expenses/:id', auth, wrap(async (req, res) => res.json(await svc.getExpense(req.userId, req.params.id))));
app.patch('/expenses/:id', auth, wrap(async (req, res) => send(res, await svc.updateExpense(req.userId, req.params.id, req.body, idem(req)))));
app.delete('/expenses/:id', auth, wrap(async (req, res) => send(res, await svc.deleteExpense(req.userId, req.params.id, req.query.version))));
app.post('/expenses/:id/restore', auth, wrap(async (req, res) => send(res, await svc.restoreExpense(req.userId, req.params.id))));

// ---- settlements ----
app.post('/groups/:id/settlements', auth, wrap(async (req, res) => send(res, await svc.createSettlement(req.userId, req.params.id, req.body, idem(req)))));
app.get('/groups/:id/settlements', auth, wrap(async (req, res) => res.json(await svc.listSettlements(req.userId, req.params.id))));
for (const action of ['confirm', 'reject', 'cancel']) {
  app.post(`/settlements/:id/${action}`, auth, wrap(async (req, res) => send(res, await svc.transitionSettlement(req.userId, req.params.id, action))));
}
app.post('/webhooks/payments', wrap(async (req, res) => {
  const expected = createHmac('sha256', config.authSecret).update(req.rawBody ?? '').digest('hex');
  const given = String(req.get('x-signature') ?? '');
  if (given.length !== expected.length || !timingSafeEqual(Buffer.from(given), Buffer.from(expected))) {
    throw new ApiError(401, 'BAD_SIGNATURE', 'invalid webhook signature');
  }
  send(res, await svc.paymentWebhook(req.body ?? {}));
}));

// ---- balances & audit ----
app.get('/groups/:id/balances', auth, wrap(async (req, res) => res.json(await svc.getBalances(req.userId, req.params.id))));
app.get('/groups/:id/activity', auth, wrap(async (req, res) => res.json(await svc.activity(req.userId, req.params.id, req.query.limit))));
app.get('/groups/:id/reconcile', auth, wrap(async (req, res) => res.json(await svc.reconcile(req.userId, req.params.id, req.query.repair === 'true'))));

// ---- real-time: Server-Sent Events fed by the projector via Redis pub/sub ----
const streams = new Map(); // group_id -> Set<res>
subscriber.on('message', (channel, msg) => {
  for (const res of streams.get(channel.slice('group:'.length)) ?? []) res.write(`event: group\ndata: ${msg}\n\n`);
});
app.get('/groups/:id/stream', auth, wrap(async (req, res) => {
  await svc.assertMember(req.params.id, req.userId);
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  res.write(': connected\n\n');
  const id = req.params.id;
  if (!streams.has(id)) {
    streams.set(id, new Set());
    await subscriber.subscribe(`group:${id}`);
  }
  streams.get(id).add(res);
  const ping = setInterval(() => res.write(': ping\n\n'), 25_000);
  req.on('close', async () => {
    clearInterval(ping);
    const set = streams.get(id);
    set?.delete(res);
    if (set && !set.size) {
      streams.delete(id);
      await subscriber.unsubscribe(`group:${id}`).catch(() => {});
    }
  });
}));

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ code: 'BAD_JSON', error: 'invalid JSON' });
  const status = err.status ?? 500;
  if (status >= 500) console.error('[api]', err);
  res.status(status).json({ code: err.code ?? 'INTERNAL', error: status >= 500 ? 'internal error' : err.message, ...err.extra });
});

app.listen(config.port, () => console.log(`[api] listening on :${config.port}`));
