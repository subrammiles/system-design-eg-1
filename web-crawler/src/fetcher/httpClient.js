// HTTP client for fetchers.
//
// - Keep-alive connection pooling (undici Agent) -> no TCP/TLS handshake per page.
// - DNS cache: hostname -> IP for a few minutes (DNS is a hidden crawl bottleneck).
// - SSRF guard: refuse to connect to private/loopback/link-local addresses, since links
//   on the public web can point anywhere (e.g. http://169.254.169.254/ cloud metadata).
// - Hard limits: timeout and max body size; redirects are NOT followed automatically
//   (each target goes back through discovery/dedupe/robots).
import dns from 'node:dns';
import net from 'node:net';
import { Agent, fetch } from 'undici';

export function isPrivateIp(ip) {
  if (net.isIPv4(ip)) {
    const [a, b] = ip.split('.').map(Number);
    return a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127);
  }
  const v6 = ip.toLowerCase();
  if (v6.startsWith('::ffff:')) return isPrivateIp(v6.slice(7));
  return v6 === '::1' || v6 === '::' || v6.startsWith('fc') || v6.startsWith('fd') || v6.startsWith('fe80');
}

export function createDnsCache({ ttlMs, allowPrivate }) {
  const cache = new Map();
  const stats = { hits: 0, misses: 0 };

  function lookup(hostname, options, callback) {
    if (typeof options === 'function') [callback, options] = [options, {}];
    const hit = cache.get(hostname);
    const reply = (addrs) => {
      const usable = allowPrivate ? addrs : addrs.filter((a) => !isPrivateIp(a.address));
      if (!usable.length) {
        const err = new Error(`blocked private address for ${hostname}`);
        err.code = 'EPRIVATEADDR';
        return callback(err);
      }
      if (options.all) return callback(null, usable);
      return callback(null, usable[0].address, usable[0].family);
    };
    if (hit && hit.expires > Date.now()) {
      stats.hits++;
      return reply(hit.addrs);
    }
    stats.misses++;
    dns.lookup(hostname, { all: true }, (err, addrs) => {
      if (err) return callback(err);
      cache.set(hostname, { addrs, expires: Date.now() + ttlMs });
      reply(addrs);
    });
  }

  return { lookup, stats };
}

export class TooLargeError extends Error {}

export function createHttpClient({ userAgent, timeoutMs, maxBytes, dnsTtlMs, allowPrivateNetworks }) {
  const dnsCache = createDnsCache({ ttlMs: dnsTtlMs, allowPrivate: allowPrivateNetworks });
  const agent = new Agent({
    keepAliveTimeout: 30_000,
    connections: 4, // per origin - politeness also bounds this
    connect: { lookup: dnsCache.lookup, timeout: timeoutMs },
  });

  async function get(url, headers = {}) {
    const res = await fetch(url, {
      dispatcher: agent,
      redirect: 'manual',
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        'user-agent': userAgent,
        accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1',
        'accept-language': 'en',
        ...headers,
      },
    });

    let body = '';
    const type = res.headers.get('content-type') ?? '';
    const wantBody = res.status === 200 && (type.includes('html') || type.includes('text/plain'));
    if (wantBody && res.body) {
      const chunks = [];
      let size = 0;
      for await (const chunk of res.body) {
        size += chunk.length;
        if (size > maxBytes) {
          await res.body.cancel().catch(() => {});
          throw new TooLargeError(`body larger than ${maxBytes} bytes`);
        }
        chunks.push(chunk);
      }
      body = Buffer.concat(chunks).toString('utf8');
    } else {
      await res.body?.cancel().catch(() => {}); // free the connection
    }
    return { status: res.status, headers: res.headers, body, contentType: type };
  }

  return { get, dnsStats: dnsCache.stats };
}
