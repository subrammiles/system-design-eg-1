// Adaptive recrawl policy: revisit pages at a rate that tracks how often they change.
//
//   changed   -> interval / 2   (news pages converge to minutes/hours)
//   unchanged -> interval * 2   (static pages drift towards weeks)
//
// bounded by [min, max]. A small jitter avoids every page of a site coming due together.
export function nextRecrawl({ previousInterval, changed, policy, now = Date.now(), jitter = Math.random() }) {
  const base = previousInterval ?? policy.initialSeconds;
  let interval = previousInterval == null ? base : changed ? base / 2 : base * 2;
  interval = Math.round(Math.min(policy.maxSeconds, Math.max(policy.minSeconds, interval)));
  const jittered = interval * (0.9 + 0.2 * jitter);
  return { interval, nextAt: now + Math.round(jittered * 1000) };
}
