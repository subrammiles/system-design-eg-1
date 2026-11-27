// A small local "web" for demoing the crawler safely. One process serves two hosts
// (site-a, site-b) chosen by the Host header, with pages built to exercise each crawler
// concern: robots.txt, spider traps, duplicate and near-duplicate content, redirects,
// errors, throttling, canonical tags, JS-only pages, nofollow and ETag/304.
import http from 'node:http';

const PORT = Number(process.env.PORT ?? 5000);
const WORDS = ('system design crawler frontier politeness robots fetch parse index storage queue ' +
  'scheduler worker shard replica cache latency throughput bloom filter hash fingerprint ' +
  'graph traversal breadth depth priority freshness recrawl duplicate canonical domain host ' +
  'network bandwidth compression archive metadata partition consistency availability').split(' ');

function words(seed, n) {
  let x = seed * 2654435761 % 2 ** 32;
  const out = [];
  for (let i = 0; i < n; i++) {
    x = (x * 1103515245 + 12345) % 2 ** 31;
    out.push(WORDS[x % WORDS.length]);
  }
  return out.join(' ');
}

const page = (title, body, head = '') =>
  `<!doctype html><html><head><title>${title}</title>${head}</head><body>
<header><nav><a href="/">Home</a> | <a href="/about">About</a></nav></header>
<main>${body}</main>
<footer>Test site footer &copy; 2026</footer></body></html>`;

const SHOE_TEXT = words(42, 220);
const counters = new Map();
const hit = (k) => {
  const n = (counters.get(k) ?? 0) + 1;
  counters.set(k, n);
  return n;
};

function siteA(path, req, res) {
  const send = (status, html, headers = {}) => {
    res.writeHead(status, { 'content-type': 'text/html; charset=utf-8', ...headers });
    res.end(html);
  };
  const [pathname, query = ''] = path.split('?');
  let m;

  if (pathname === '/robots.txt') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    return res.end('User-agent: badbot\nDisallow: /\n\nUser-agent: *\nDisallow: /private/\nAllow: /private/public-note\nCrawl-delay: 0.2\n');
  }

  if (pathname === '/') {
    return send(200, page('Site A home', `
      <h1>Site A</h1><p>${words(1, 60)}</p>
      <ul>
        <li><a href="/articles/1">Articles</a></li>
        <li><a href="/about?ref=twitter">About (tracking param)</a> <a href="/about/?utm_source=mail#team">About again</a></li>
        <li><a href="/mirror/about">Mirror of about (exact duplicate)</a></li>
        <li><a href="/products/shoes-a">Shoes A</a> <a href="/products/shoes-b">Shoes B (near duplicate)</a></li>
        <li><a href="/calendar/2026/1">Calendar (spider trap)</a></li>
        <li><a href="/private/secret">Private (robots disallowed)</a> <a href="/private/public-note">Allowed note</a></li>
        <li><a href="/flaky">Flaky (500 twice)</a> <a href="/slow-down">Throttled (429)</a></li>
        <li><a href="/old-page">Old page (301)</a> <a href="/missing">Missing (404)</a></li>
        <li><a href="/canonical?size=l&color=blue">Canonical variant</a></li>
        <li><a href="/spa">Single-page app</a> <a href="/nofollow-page">Nofollow page</a></li>
        <li><a href="/logo.png">Logo</a> <a href="/logout">Logout</a> <a href="mailto:hi@example.com">Mail</a></li>
        <li><a href="http://site-b:5000/">Site B</a> <a href="/ignored" rel="nofollow">ignored</a></li>
      </ul>`));
  }

  if (pathname === '/about' || pathname === '/mirror/about') {
    const etag = '"about-v1"';
    if (pathname === '/about' && req.headers['if-none-match'] === etag) {
      res.writeHead(304, { etag });
      return res.end();
    }
    return send(200, page('About', `<h1>About us</h1><p>${words(7, 80)}</p>`), pathname === '/about' ? { etag } : {});
  }

  if ((m = pathname.match(/^\/articles\/(\d+)$/))) {
    const n = Number(m[1]);
    if (n > 15) return send(404, page('Not found', 'no such article'));
    const links = [n > 1 && `<a href="/articles/${n - 1}">prev</a>`, n < 15 && `<a href="/articles/${n + 1}">next</a>`,
      `<a href="/articles/${((n * 7) % 15) + 1}">related</a>`].filter(Boolean).join(' ');
    return send(200, page(`Article ${n}`, `<h1>Article ${n}</h1><p>${words(100 + n, 150)}</p>${links}`));
  }

  if ((m = pathname.match(/^\/products\/shoes-(a|b)$/))) {
    // Identical except one word -> SimHash distance small, SHA-256 different.
    const text = m[1] === 'a' ? SHOE_TEXT : SHOE_TEXT.replace(/\bcache\b/, 'cachet');
    return send(200, page(`Shoes ${m[1]}`, `<h1>Running shoes</h1><p>${text}</p>`));
  }

  if ((m = pathname.match(/^\/calendar\/(\d+)\/(\d+)$/))) {
    // Infinite: every month links to the next one, forever.
    let [y, mo] = [Number(m[1]), Number(m[2]) + 1];
    if (mo > 12) [y, mo] = [y + 1, 1];
    return send(200, page(`Calendar ${m[1]}-${m[2]}`, `<h1>${m[1]}-${m[2]}</h1><a href="/calendar/${y}/${mo}">Next month</a>`));
  }

  if (pathname.startsWith('/private/')) return send(200, page('Private', `<p>${pathname}</p>`));

  if (pathname === '/flaky') {
    if (hit('flaky') <= 2) return send(500, page('Error', 'temporary failure'));
    return send(200, page('Flaky', `<p>Worked on attempt ${counters.get('flaky')}. ${words(55, 40)}</p>`));
  }

  if (pathname === '/slow-down') {
    if (hit('slow') === 1) return send(429, page('Too many requests', 'slow down'), { 'retry-after': '2' });
    return send(200, page('Throttled page', `<p>Now allowed. ${words(66, 40)}</p>`));
  }

  if (pathname === '/old-page') {
    res.writeHead(301, { location: '/new-page' });
    return res.end();
  }
  if (pathname === '/new-page') return send(200, page('New page', `<p>${words(77, 50)}</p>`));

  if (pathname === '/canonical') {
    return send(200, page('Canonical product', `<p>${words(88, 60)} ${query}</p>`,
      '<link rel="canonical" href="http://site-a:5000/canonical">'));
  }

  if (pathname === '/spa') {
    return send(200, '<!doctype html><html><head><title>App</title></head><body><div id="app"></div><script src="/app.js"></script></body></html>');
  }

  if (pathname === '/nofollow-page') {
    return send(200, page('Nofollow', '<p>Do not follow my links.</p><a href="/secret-from-nofollow">hidden</a>',
      '<meta name="robots" content="noindex, nofollow">'));
  }

  return send(404, page('Not found', 'not found'));
}

function siteB(path, _req, res) {
  const send = (status, html) => {
    res.writeHead(status, { 'content-type': 'text/html; charset=utf-8' });
    res.end(html);
  };
  const [pathname] = path.split('?');
  if (pathname === '/robots.txt') return send(404, 'none'); // 404 robots = everything allowed
  if (pathname === '/') {
    const posts = [1, 2, 3, 4, 5].map((n) => `<a href="/posts/${n}">Post ${n}</a>`).join(' ');
    return send(200, page('Site B blog', `<h1>Blog</h1><p>${words(200, 50)}</p>${posts} <a href="http://site-a:5000/">Back to A</a>`));
  }
  const m = pathname.match(/^\/posts\/(\d)$/);
  if (m) return send(200, page(`Post ${m[1]}`, `<p>${words(300 + Number(m[1]), 120)}</p><a href="/">Blog home</a>`));
  return send(404, page('Not found', 'not found'));
}

http.createServer((req, res) => {
  const host = (req.headers.host ?? '').split(':')[0];
  console.log(`${new Date().toISOString()} ${host} ${req.method} ${req.url} ua="${req.headers['user-agent']}"`);
  (host === 'site-b' ? siteB : siteA)(req.url, req, res);
}).listen(PORT, () => console.log(`[test-site] serving site-a / site-b on :${PORT}`));
