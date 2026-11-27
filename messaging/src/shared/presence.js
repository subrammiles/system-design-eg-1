// Connection registry + presence in Redis. The live WebSocket objects stay in gateway memory;
// Redis only stores WHERE each device is connected.
//
//   conn:{user}               HASH   device_id -> gateway_id      (all devices that may be online)
//   presence:{user}:{device}  STRING gateway_id, TTL 60s           (refreshed on every heartbeat)
//   lastseen:{user}           STRING epoch ms of last disconnect / heartbeat
//   gw:{gateway_id}           pub/sub channel the delivery worker uses to reach a gateway
//
// A device counts as online only if its presence key is still alive. If a gateway crashes,
// its devices' presence keys expire within the TTL and they read as offline (stale `conn`
// entries are cleaned up lazily).

// Remove a device mapping only if it still points at this gateway: the device may already
// have reconnected to a different gateway.
const UNREGISTER_LUA = `
  if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
    redis.call('HDEL', KEYS[1], ARGV[1])
  end
  if redis.call('GET', KEYS[2]) == ARGV[2] then
    redis.call('DEL', KEYS[2])
  end
  return 1`;

export const gatewayChannel = (gatewayId) => `gw:${gatewayId}`;

export class Presence {
  constructor(redis, { ttlSeconds = 60 } = {}) {
    this.redis = redis;
    this.ttl = ttlSeconds;
    if (!redis.presenceUnregister) redis.defineCommand('presenceUnregister', { numberOfKeys: 2, lua: UNREGISTER_LUA });
  }

  register(userId, deviceId, gatewayId) {
    return this.redis.multi()
      .hset(`conn:${userId}`, deviceId, gatewayId)
      .set(`presence:${userId}:${deviceId}`, gatewayId, 'EX', this.ttl)
      .set(`lastseen:${userId}`, Date.now())
      .exec();
  }

  refresh(userId, deviceId, gatewayId) {
    return this.redis.multi()
      .set(`presence:${userId}:${deviceId}`, gatewayId, 'EX', this.ttl)
      .set(`lastseen:${userId}`, Date.now())
      .exec();
  }

  async unregister(userId, deviceId, gatewayId) {
    await this.redis.presenceUnregister(`conn:${userId}`, `presence:${userId}:${deviceId}`, deviceId, gatewayId);
    await this.redis.set(`lastseen:${userId}`, Date.now());
  }

  /** @returns {Promise<Array<{deviceId: string, gatewayId: string}>>} */
  async onlineDevices(userId) {
    const conns = await this.redis.hgetall(`conn:${userId}`);
    const devices = Object.keys(conns);
    if (!devices.length) return [];
    const alive = await this.redis.mget(devices.map((d) => `presence:${userId}:${d}`));
    const online = [];
    const stale = [];
    devices.forEach((d, i) => (alive[i] ? online.push({ deviceId: d, gatewayId: alive[i] }) : stale.push(d)));
    if (stale.length) await this.redis.hdel(`conn:${userId}`, ...stale);
    return online;
  }

  async status(userId) {
    const [online, lastSeen] = await Promise.all([this.onlineDevices(userId), this.redis.get(`lastseen:${userId}`)]);
    return {
      user_id: userId,
      online: online.length > 0,
      devices: online.map((d) => d.deviceId),
      last_seen: lastSeen ? Number(lastSeen) : null,
    };
  }
}
