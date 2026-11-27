// Messaging Service HTTP API.
//
// Public (Authorization: Bearer <device token>, except registration):
//   POST /users                         {name}                     -> {user_id}
//   POST /users/:id/devices             {platform?, push_token?}   -> {device_id, token}
//   GET  /conversations                                            -> my chats + watermarks
//   POST /conversations/direct          {user_id}
//   POST /conversations/group           {name, members: []}
//   POST /conversations/:id/members     {user_id}                  (group admins)
//   GET  /conversations/:id/messages?before_seq=&limit=            history (Cassandra)
//   GET  /presence/:userId                                         online / last seen
//   POST /media/upload-url              {content_type, size}       -> presigned S3 PUT
//   GET  /media/download-url?key=                                  -> presigned S3 GET
//
// Internal (called by gateways; x-internal-token): the gateway already authenticated the socket.
//   POST /internal/messages | /internal/ack | /internal/sync | /internal/typing
import express from 'express';
import { randomUUID } from 'node:crypto';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { verifyToken } from '../shared/auth.js';
import { config } from '../shared/config.js';
import { createCassandra, createPg, createRedis, createS3, retry } from '../shared/clients.js';
import { MessageStore } from '../shared/messageStore.js';
import { Presence } from '../shared/presence.js';
import { ApiError, ChatService } from './chatService.js';

const pg = createPg();
const cassandra = createCassandra();
await retry('cassandra', () => cassandra.connect(), 90);
const redis = createRedis();
const presence = new Presence(redis, { ttlSeconds: config.gateway.presenceTtlSeconds });
const chat = new ChatService({ pg, store: new MessageStore(cassandra), redis, presence, config });
const s3Public = createS3(config.s3.publicEndpoint); // URLs handed to clients
const internalToken = config.authSecret;

const app = express();
app.use(express.json({ limit: '256kb' }));
const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

function auth(req, _res, next) {
  const token = (req.get('authorization') ?? '').replace(/^Bearer /i, '');
  const identity = verifyToken(token, config.authSecret);
  if (!identity) return next(new ApiError(401, 'UNAUTHORIZED', 'valid device token required'));
  req.user = identity;
  next();
}

function internal(req, _res, next) {
  if (req.get('x-internal-token') !== internalToken) return next(new ApiError(403, 'FORBIDDEN', 'internal only'));
  next();
}

// ---- registration (stand-in for phone-number OTP / identity provider) ----
app.post('/users', wrap(async (req, res) => res.status(201).json(await chat.createUser(req.body?.name))));
app.post('/users/:id/devices', wrap(async (req, res) =>
  res.status(201).json(await chat.registerDevice(req.params.id, { platform: req.body?.platform, pushToken: req.body?.push_token }))));

// ---- conversations ----
app.get('/conversations', auth, wrap(async (req, res) => res.json(await chat.conversationsOf(req.user.userId))));
app.post('/conversations/direct', auth, wrap(async (req, res) =>
  res.status(201).json(await chat.createDirect(req.user.userId, req.body?.user_id))));
app.post('/conversations/group', auth, wrap(async (req, res) =>
  res.status(201).json(await chat.createGroup(req.user.userId, req.body?.name, req.body?.members ?? []))));
app.post('/conversations/:id/members', auth, wrap(async (req, res) =>
  res.status(201).json(await chat.addMember(req.user.userId, req.params.id, req.body?.user_id))));
app.get('/conversations/:id/messages', auth, wrap(async (req, res) =>
  res.json(await chat.history(req.user.userId, req.params.id, req.query.before_seq, req.query.limit))));

app.get('/presence/:userId', auth, wrap(async (req, res) => res.json(await presence.status(req.params.userId))));

// ---- media: clients upload/download directly to S3; bytes never pass through chat servers ----
app.post('/media/upload-url', auth, wrap(async (req, res) => {
  const { content_type: contentType, size } = req.body ?? {};
  if (!/^(image|video|audio|application)\/[\w.+-]+$/.test(contentType ?? '')) throw new ApiError(400, 'BAD_TYPE', 'content_type required');
  if (!Number.isInteger(size) || size <= 0 || size > config.s3.maxUploadBytes) throw new ApiError(400, 'BAD_SIZE', 'size out of range');
  const key = `media/${req.user.userId}/${randomUUID()}`;
  const url = await getSignedUrl(s3Public,
    new PutObjectCommand({ Bucket: config.s3.bucket, Key: key, ContentType: contentType, ContentLength: size }),
    { expiresIn: 900 });
  res.json({ key, upload_url: url, method: 'PUT', headers: { 'content-type': contentType } });
}));

app.get('/media/download-url', auth, wrap(async (req, res) => {
  const key = String(req.query.key ?? '');
  if (!key.startsWith('media/')) throw new ApiError(400, 'BAD_KEY', 'invalid key');
  // Production: check the requester belongs to a conversation referencing this key; serve via CDN signed URL.
  res.json({ download_url: await getSignedUrl(s3Public, new GetObjectCommand({ Bucket: config.s3.bucket, Key: key }), { expiresIn: 900 }) });
}));

// ---- internal API used by gateways ----
app.post('/internal/messages', internal, wrap(async (req, res) => res.json(await chat.acceptMessage(req.body))));
app.post('/internal/ack', internal, wrap(async (req, res) => res.json(await chat.ack(req.body))));
app.post('/internal/sync', internal, wrap(async (req, res) => res.json(await chat.sync(req.body))));
app.post('/internal/typing', internal, wrap(async (req, res) => res.json(await chat.typing(req.body))));

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ code: 'BAD_JSON', error: 'invalid JSON' });
  const status = err.status ?? 500;
  if (status >= 500) console.error('[messaging]', err);
  res.status(status).json({
    code: err.code ?? 'INTERNAL',
    error: status >= 500 && !err.code ? 'internal error' : err.message,
    retryable: err.extra?.retryable ?? status >= 500,
    ...err.extra,
  });
});

app.listen(config.messaging.port, () => console.log(`[messaging] listening on :${config.messaging.port}`));
