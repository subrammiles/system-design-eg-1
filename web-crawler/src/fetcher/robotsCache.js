// robots.txt "handshake": fetched once per host, cached in Redis (shared by all fetchers).
//
//   2xx       -> parse rules
//   4xx       -> no restrictions (RFC 9309)
//   5xx/error -> assume full disallow, re-check soon (the site may be struggling)
import { ALLOW_ALL, DISALLOW_ALL, parseRobots } from '../shared/robots.js';

export class RobotsCache {
  constructor({ redis, http, agent, ttlSeconds }) {
    this.redis = redis;
    this.http = http;
    this.agent = agent;
    this.ttlSeconds = ttlSeconds;
    this.local = new Map(); // short in-process cache
  }

  async get(origin, host) {
    const local = this.local.get(host);
    if (local && local.expires > Date.now()) return local.robots;

    let robots;
    const cached = await this.redis.get(`robots:${host}`);
    if (cached) {
      robots = JSON.parse(cached);
    } else {
      let ttl = this.ttlSeconds;
      try {
        const res = await this.http.get(`${origin}/robots.txt`, { accept: 'text/plain' });
        if (res.status === 200) robots = parseRobots(res.body, this.agent);
        else if (res.status >= 400 && res.status < 500) robots = ALLOW_ALL;
        else {
          robots = DISALLOW_ALL;
          ttl = 300;
        }
      } catch {
        robots = DISALLOW_ALL;
        ttl = 300;
      }
      await this.redis.set(`robots:${host}`, JSON.stringify(robots), 'EX', ttl);
    }
    this.local.set(host, { robots, expires: Date.now() + 60_000 });
    return robots;
  }
}
