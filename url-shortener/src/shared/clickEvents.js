// Click-event queue on a Redis Stream (stand-in for Kafka / SQS / Kinesis).
//
//   Redirect -> XADD click-events -> consumer group -> analytics processor
//
// The redirect never waits on analytics: publish is fire-and-forget.
import { randomUUID } from 'node:crypto';

export class ClickEventPublisher {
  constructor({ redis, stream, maxLen }) {
    this.redis = redis;
    this.stream = stream;
    this.maxLen = maxLen;
  }

  publish(event) {
    const payload = { eventId: randomUUID(), ts: Date.now(), ...event };
    // MAXLEN ~ keeps the stream bounded; '~' lets Redis trim efficiently.
    this.redis
      .xadd(this.stream, 'MAXLEN', '~', this.maxLen, '*', 'data', JSON.stringify(payload))
      .catch((err) => console.error('[click-events] publish failed (dropped):', err.message));
  }
}

// Very small user-agent parser - enough for device/browser breakdowns.
export function parseUserAgent(ua = '') {
  const s = ua.toLowerCase();
  let device = 'desktop';
  if (/bot|crawler|spider|slurp|curl|wget|python-requests/.test(s)) device = 'bot';
  else if (/ipad|tablet/.test(s)) device = 'tablet';
  else if (/mobi|android|iphone/.test(s)) device = 'mobile';

  let browser = 'other';
  if (s.includes('edg/')) browser = 'edge';
  else if (s.includes('chrome/') && !s.includes('chromium')) browser = 'chrome';
  else if (s.includes('firefox/')) browser = 'firefox';
  else if (s.includes('safari/')) browser = 'safari';
  else if (s.includes('curl/')) browser = 'curl';

  return { device, browser };
}

function referrerHost(ref) {
  if (!ref) return 'direct';
  try {
    return new URL(ref).hostname || 'direct';
  } catch {
    return 'unknown';
  }
}

// Which click_stats counters one click increments.
export function metricsFor(event) {
  const { device, browser } = parseUserAgent(event.userAgent);
  const day = new Date(event.ts).toISOString().slice(0, 10);
  return [
    'total',
    `day#${day}`,
    `device#${device}`,
    `browser#${browser}`,
    `country#${event.country || 'unknown'}`,
    `referrer#${referrerHost(event.referrer)}`,
  ];
}
