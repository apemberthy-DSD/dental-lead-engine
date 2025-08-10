const { request } = require('undici');

function normalizeUrl(url){
  if(!url) return null;
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.hostname}${u.pathname === '/' ? '' : u.pathname}`;
  } catch {
    try { return `https://${url}`; } catch { return null; }
  }
}

function stripHtml(html){
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchSiteText(url, timeoutMs = 12000){
  const norm = normalizeUrl(url);
  if(!norm) return '';
  const controller = new AbortController();
  const t = setTimeout(()=>controller.abort(), timeoutMs);
  try{
    const res = await request(norm, {
      method: 'GET',
      headers: { 'user-agent': 'Mozilla/5.0 (LeadEngineBot)' },
      signal: controller.signal
    });
    if(res.statusCode >= 400) return '';
    const html = await res.body.text();
    return stripHtml(html).slice(0, 15000);
  } catch {
    return '';
  } finally {
    clearTimeout(t);
  }
}

module.exports = { fetchSiteText };
