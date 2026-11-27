import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeUrl, rejectReason, urlPattern } from '../src/shared/urls.js';
import { isAllowed, parseRobots } from '../src/shared/robots.js';
import { bands, hamming, simhash } from '../src/shared/simhash.js';
import { nextRecrawl } from '../src/shared/recrawl.js';
import { bloomPositions } from '../src/shared/discovery.js';
import { isPrivateIp } from '../src/fetcher/httpClient.js';
import { parseHtml } from '../src/parser/parseHtml.js';

const scope = { maxDepth: 5, allowedHosts: [] };

test('normalizeUrl: equivalent URLs collapse, distinct ones do not', () => {
  const a = normalizeUrl('HTTP://Example.COM:80/About/?utm_source=x&ref=tw#team');
  assert.equal(a, 'http://example.com/About');
  assert.equal(normalizeUrl('http://example.com/About'), a);
  assert.equal(normalizeUrl('/p?b=2&a=1', 'https://x.com/dir/page'), 'https://x.com/p?a=1&b=2');
  assert.equal(normalizeUrl('../up', 'https://x.com/a/b/c'), 'https://x.com/a/up');
  assert.notEqual(normalizeUrl('https://x.com/About'), normalizeUrl('https://x.com/about')); // path case kept
  assert.equal(normalizeUrl('mailto:a@b.com'), null);
  assert.equal(normalizeUrl('javascript:void(0)'), null);
});

test('rejectReason: scope, depth, file types, side-effect links, traps', () => {
  const r = (u, extra = {}) => rejectReason(u, { depth: 1, ...scope, ...extra });
  assert.equal(r('https://x.com/page'), null);
  assert.equal(r('https://x.com/logo.PNG'), 'non_html');
  assert.equal(r('https://x.com/account/logout'), 'blocked_path');
  assert.equal(r('https://x.com/a', { depth: 6 }), 'max_depth');
  assert.equal(r('https://y.com/a', { allowedHosts: ['x.com'] }), 'out_of_scope');
  assert.equal(r('https://x.com/a/b/a/b/a'), 'trap_repeating_segment');
  assert.equal(r('https://x.com/f?a=1&b=1&c=1&d=1&e=1&f=1&g=1'), 'trap_query_params');
});

test('urlPattern groups generated URL spaces', () => {
  assert.equal(urlPattern('https://x.com/calendar/2026/11'), '/calendar/{n}/{n}');
  assert.equal(urlPattern('https://x.com/calendar/2031/4'), '/calendar/{n}/{n}');
  assert.equal(urlPattern('https://x.com/f?color=red&page=3'), '/f?color=*&page=*');
});

test('robots.txt: agent groups, longest match, allow on tie, wildcards, crawl-delay', () => {
  const txt = `
User-agent: badbot
Disallow: /

User-agent: *
Disallow: /private/
Allow: /private/public-note
Disallow: /*.json$
Disallow: /search?
Crawl-delay: 2.5
`;
  const ours = parseRobots(txt, 'sdcrawler');
  assert.equal(ours.crawlDelay, 2.5);
  assert.equal(isAllowed(ours, 'https://x.com/'), true);
  assert.equal(isAllowed(ours, 'https://x.com/private/secret'), false);
  assert.equal(isAllowed(ours, 'https://x.com/private/public-note'), true);
  assert.equal(isAllowed(ours, 'https://x.com/data.json'), false);
  assert.equal(isAllowed(ours, 'https://x.com/data.json?x=1'), true); // $ anchors the end
  assert.equal(isAllowed(ours, 'https://x.com/search?q=1'), false);
  const bad = parseRobots(txt, 'BadBot/2.0');
  assert.equal(isAllowed(bad, 'https://x.com/'), false);
  assert.equal(isAllowed(parseRobots('', 'x'), 'https://x.com/any'), true);
});

const VOCAB = ('system design crawler frontier politeness robots fetch parse index storage queue scheduler ' +
  'worker shard replica cache latency throughput bloom filter hash fingerprint graph traversal breadth ' +
  'depth priority freshness recrawl duplicate canonical domain host network bandwidth compression').split(' ');
function words(seed, n) {
  let x = (seed * 2654435761) % 2 ** 32;
  return Array.from({ length: n }, () => VOCAB[(x = (x * 1103515245 + 12345) % 2 ** 31) % VOCAB.length]).join(' ');
}
const LONG = words(42, 220);

test('simhash: one-word edits are usually within 3 bits, unrelated pages are far', () => {
  const a = simhash(LONG);
  assert.ok(hamming(a, simhash(LONG.replace(/\bcache\b/, 'cachet'))) <= 3);
  assert.equal(hamming(a, a), 0);
  const edits = [];
  const unrelated = [];
  for (let s = 1; s <= 60; s++) {
    const w = words(s, 220).split(' ');
    const base = simhash(w.join(' '));
    w[100] += 'zz';
    edits.push(hamming(base, simhash(w.join(' '))));
    unrelated.push(hamming(base, simhash(words(s + 1000, 220))));
  }
  edits.sort((x, y) => x - y);
  assert.ok(edits[30] <= 3, `median edit distance ${edits[30]}`);
  assert.ok(Math.min(...unrelated) > 10, `closest unrelated ${Math.min(...unrelated)}`);
});

test('LSH bands: distance <= k always shares at least one of k+1 bands (pigeonhole)', () => {
  const fp = simhash(LONG);
  for (const k of [3, 6]) {
    assert.equal(bands(fp, k + 1).length, k + 1);
    for (let trial = 0; trial < 200; trial++) {
      let other = fp;
      const flipped = new Set();
      while (flipped.size < k) flipped.add(Math.floor(Math.random() * 64));
      for (const bit of flipped) other ^= 1n << BigInt(bit);
      const mine = bands(fp, k + 1);
      assert.ok(bands(other, k + 1).some((b) => mine.includes(b)), `k=${k} bits=${[...flipped]}`);
    }
  }
});

test('adaptive recrawl: halves on change, doubles when unchanged, clamped', () => {
  const policy = { initialSeconds: 3600, minSeconds: 600, maxSeconds: 7200 };
  const first = nextRecrawl({ previousInterval: undefined, changed: true, policy, now: 0, jitter: 0.5 });
  assert.equal(first.interval, 3600);
  assert.equal(first.nextAt, 3600 * 1000);
  assert.equal(nextRecrawl({ previousInterval: 3600, changed: true, policy }).interval, 1800);
  assert.equal(nextRecrawl({ previousInterval: 3600, changed: false, policy }).interval, 7200);
  assert.equal(nextRecrawl({ previousInterval: 7200, changed: false, policy }).interval, 7200);
  assert.equal(nextRecrawl({ previousInterval: 700, changed: true, policy }).interval, 600);
});

test('bloom positions are deterministic and in range', () => {
  for (let i = 0; i < 200; i++) {
    assert.ok(bloomPositions(`k${i}`, 2 ** 27, 7).every((x) => Number.isInteger(x) && x >= 0 && x < 2 ** 27));
  }
  const p = bloomPositions('abc', 1024, 7);
  assert.equal(p.length, 7);
  assert.deepEqual(p, bloomPositions('abc', 1024, 7));
  assert.ok(p.every((x) => x >= 0 && x < 1024));
});

test('SSRF guard recognizes private addresses', () => {
  for (const ip of ['10.0.0.1', '127.0.0.1', '169.254.169.254', '172.20.1.1', '192.168.1.1', '::1', 'fd00::1', '::ffff:10.1.1.1']) {
    assert.equal(isPrivateIp(ip), true, ip);
  }
  for (const ip of ['8.8.8.8', '172.32.0.1', '2606:4700::1111']) assert.equal(isPrivateIp(ip), false, ip);
});

test('parseHtml: links, boilerplate removal, canonical, meta robots, base href, SPA detection', () => {
  const html = `<html><head><title> Hello </title><base href="https://x.com/docs/">
    <link rel="canonical" href="https://x.com/docs/page">
    <meta name="robots" content="noindex"></head>
    <body><nav><a href="/nav">nav</a> navigation text</nav>
    <main><p>Main content here.</p><a href="intro">Intro</a> <a href="#top">top</a>
    <a href="/ads" rel="nofollow sponsored">ad</a></main><script>var x=1</script>
    <footer>footer text</footer></body></html>`;
  const p = parseHtml(html, 'https://x.com/docs/page?x=1');
  assert.equal(p.title, 'Hello');
  assert.equal(p.base, 'https://x.com/docs/');
  assert.deepEqual(p.links.sort(), ['/nav', 'intro']);
  assert.equal(p.canonical, 'https://x.com/docs/page');
  assert.equal(p.noindex, true);
  assert.equal(p.nofollow, false);
  assert.equal(p.text, 'Main content here. Intro top ad');
  assert.equal(p.needsRender, false);

  const spa = parseHtml('<html><body><div id="root"></div><script src="/app.js"></script></body></html>', 'https://x.com/');
  assert.equal(spa.needsRender, true);
});
