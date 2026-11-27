// Pure HTML parsing: title, main text (boilerplate removed), links, canonical, meta robots,
// and a heuristic for client-side-rendered pages that need a headless browser.
import * as cheerio from 'cheerio';

const BOILERPLATE = 'script, style, noscript, template, nav, header, footer, aside, form, iframe, svg';

export function parseHtml(html, pageUrl) {
  const $ = cheerio.load(html);

  let base = pageUrl;
  const baseHref = $('base[href]').attr('href');
  if (baseHref) {
    try {
      base = new URL(baseHref, pageUrl).toString();
    } catch { /* ignore bad <base> */ }
  }

  const robotsMeta = ($('meta[name="robots" i]').attr('content') ?? '').toLowerCase();
  const noindex = /\b(noindex|none)\b/.test(robotsMeta);
  const nofollow = /\b(nofollow|none)\b/.test(robotsMeta);

  const canonical = $('link[rel="canonical" i]').attr('href') ?? null;
  const title = $('title').first().text().trim().slice(0, 300);
  const scriptCount = $('script').length;

  const links = new Set();
  $('a[href]').each((_, el) => {
    const rel = ($(el).attr('rel') ?? '').toLowerCase();
    if (rel.includes('nofollow')) return;
    const href = $(el).attr('href').trim();
    if (href && !href.startsWith('#')) links.add(href);
  });

  $(BOILERPLATE).remove();
  // Join text nodes with spaces so adjacent block elements don't fuse ("end.Start").
  const text = $('body *').contents()
    .filter((_, n) => n.type === 'text')
    .map((_, n) => $(n).text())
    .get()
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();

  // "Shadow web": an app shell with almost no server-rendered text but scripts that build the DOM.
  const appShell = $('#app, #root, #__next, [data-reactroot], [ng-app]').length > 0;
  const needsRender = text.length < 200 && scriptCount > 0 && appShell;

  return { title, text, links: [...links], canonical, noindex, nofollow, base, needsRender };
}
