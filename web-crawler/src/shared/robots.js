// robots.txt parsing + matching (RFC 9309).
//
// - Pick the group whose User-agent best matches our token, else "*".
// - Longest matching rule wins; on a tie, Allow wins.
// - Supports "*" wildcards and "$" end anchors, plus the non-standard Crawl-delay.

export function parseRobots(text, agent) {
  const groups = [];
  let current = null;
  let lastWasAgent = false;

  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.replace(/#.*/, '').trim();
    const idx = line.indexOf(':');
    if (idx < 0) continue;
    const key = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();

    if (key === 'user-agent') {
      if (!lastWasAgent) {
        current = { agents: [], rules: [], crawlDelay: null };
        groups.push(current);
      }
      current.agents.push(value.toLowerCase());
      lastWasAgent = true;
      continue;
    }
    lastWasAgent = false;
    if (!current) continue;
    if (key === 'allow' || key === 'disallow') {
      if (value === '' && key === 'disallow') continue; // "Disallow:" = allow everything
      current.rules.push({ allow: key === 'allow', path: value });
    } else if (key === 'crawl-delay') {
      const d = Number(value);
      if (Number.isFinite(d) && d >= 0) current.crawlDelay = d;
    }
  }

  const token = agent.toLowerCase();
  const group =
    groups.find((g) => g.agents.some((a) => a !== '*' && token.includes(a))) ??
    groups.find((g) => g.agents.includes('*'));

  return { rules: group?.rules ?? [], crawlDelay: group?.crawlDelay ?? null };
}

function ruleToRegex(path) {
  const anchored = path.endsWith('$');
  const body = (anchored ? path.slice(0, -1) : path)
    .split('*')
    .map((s) => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&'))
    .join('.*');
  return new RegExp(`^${body}${anchored ? '$' : ''}`);
}

export function isAllowed(robots, url) {
  const u = new URL(url);
  const target = u.pathname + u.search;
  let best = null;
  for (const rule of robots.rules) {
    if (!ruleToRegex(rule.path).test(target)) continue;
    const len = rule.path.length;
    if (!best || len > best.len || (len === best.len && rule.allow)) best = { len, allow: rule.allow };
  }
  return best ? best.allow : true;
}

export const ALLOW_ALL = { rules: [], crawlDelay: null };
export const DISALLOW_ALL = { rules: [{ allow: false, path: '/' }], crawlDelay: null };
