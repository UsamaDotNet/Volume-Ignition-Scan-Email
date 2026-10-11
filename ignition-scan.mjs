#!/usr/bin/env node
/**
 * VOLUME IGNITION — server-side scanner
 *
 * Runs on GitHub Actions on a schedule. Sweeps every USDT perp, applies the
 * same ignition logic as volume-squeeze.html, and emails any new signals.
 *
 * Nothing here touches a browser. Your laptop can be shut and your phone off —
 * the email is produced by GitHub's servers and waits in your inbox.
 *
 * Zero dependencies. Node 18+ (global fetch).
 *
 * ── DATA SOURCE ───────────────────────────────────────────────────────────
 * Binance geo-blocks US datacenter IPs with HTTP 451, and GitHub's hosted
 * runners are Azure US. So SOURCE=auto tries Binance first and falls back to
 * Bybit linear perps if Binance refuses the runner. The detector measures
 * every candle against that symbol's OWN baseline, so a different exchange's
 * absolute volume changes nothing about what fires — only usdMin is absolute,
 * and USD is USD on either venue.
 */

import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';

// ─────────────────────────── CONFIG ───────────────────────────
// Every value is overridable from the workflow's env block, so thresholds
// live in one place (the YAML) rather than buried in code.
const CFG = {
  source:    (process.env.SOURCE || 'auto').toLowerCase(),
  mode:      (process.env.MODE   || 'flexible').toLowerCase(),
  tf:        process.env.TF              || '15m',
  winLen:    int(process.env.WINDOW,      5),    // candles judged together
  baseLen:   int(process.env.BASELINE,    5),    // quiet candles sampled BEFORE the window
  cumMin:    num(process.env.CUM_MIN,     20),   // window total >= x baseline
  peakMin:   num(process.env.PEAK_MIN,    10),   // final candle >= x baseline
  elevMin:   num(process.env.ELEV_MIN,    2),    // a candle counts as "elevated" at this ratio
  minElev:   int(process.env.MIN_ELEV,    3),    // how many elevated candles are required
  expMin:    num(process.env.EXP_MIN,     2),    // newer half >= x older half
  vsPeak:    num(process.env.VS_PEAK,     0.4),  // final candle >= x window peak
  moveMin:   num(process.env.MOVE_MIN,    4),    // price move across window, %
  usdMin:    num(process.env.USD_MIN,     200000),
  stepMin:   num(process.env.STEP_MIN,    1.3),  // STRICT mode only: each candle >= x previous
  lockBars:  int(process.env.LOCKOUT,     20),   // bars of silence per symbol after a hit
  concurrency: int(process.env.CONCURRENCY, 6),
  statePath: process.env.STATE_PATH || '.state/ignition.json',
  dryRun:    process.env.DRY_RUN === 'true',
};

const MAIL = {
  key:  process.env.RESEND_API_KEY,
  from: process.env.MAIL_FROM || 'Volume Ignition <onboarding@resend.dev>',
  to:   process.env.MAIL_TO,
};

const TF_MS = { '1m':60e3, '5m':300e3, '15m':900e3, '30m':1800e3, '1h':3600e3, '4h':14400e3 };

function num(v, d) { const n = parseFloat(v); return Number.isFinite(n) ? n : d; }
function int(v, d) { const n = parseInt(v, 10); return Number.isFinite(n) ? n : d; }
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─────────────────────────── HTTP ───────────────────────────
async function req(host, path, tries = 3) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(host + path, {
        headers: { 'User-Agent': 'volume-ignition-scanner/1.1' },
      });
      if (res.status === 429 || res.status === 418) {
        const wait = 2000 * (i + 1);
        console.warn(`  rate limited (${res.status}), backing off ${wait}ms`);
        await sleep(wait);
        continue;
      }
      // 451 = restricted location. 403 from these hosts means the same thing.
      if (res.status === 451 || res.status === 403) {
        const e = new Error(`restricted location (HTTP ${res.status})`);
        e.geo = true;
        throw e;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
      if (e.geo) throw e;
      await sleep(400 * (i + 1));
    }
  }
  throw lastErr;
}

// ───────────────────── SOURCE ADAPTERS ─────────────────────
// Each adapter normalises candles to { t, o, c, v, q }:
//   t = open time ms, o = open, c = close, v = base volume, q = quote (USD) volume
// Oldest first, newest last, forming candle included (the detector drops it).

const SOURCES = {
  binance: {
    label: 'Binance USDT perps',
    hosts: [
      'https://fapi.binance.com',
      'https://fapi1.binance.com',
      'https://fapi2.binance.com',
      'https://fapi3.binance.com',
    ],
    ping: '/fapi/v1/ping',
    tvSymbol: s => `BINANCE:${s}.P`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h' },
    async universe(get) {
      const info = await get('/fapi/v1/exchangeInfo');
      return (info.symbols || [])
        .filter(s => s.contractType === 'PERPETUAL' && s.status === 'TRADING' && s.quoteAsset === 'USDT')
        .map(s => ({ symbol: s.symbol, base: s.baseAsset }));
    },
    async candles(get, sym, tf, limit) {
      const k = await get(`/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=${limit}`, 2);
      if (!Array.isArray(k)) return [];
      // [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, ...]
      return k.map(r => ({
        t: +r[0], o: +r[1], c: +r[4], v: +r[5], q: +r[7],
      }));
    },
  },

  bybit: {
    label: 'Bybit USDT perps',
    hosts: ['https://api.bybit.com', 'https://api.bytick.com'],
    ping: '/v5/market/time',
    tvSymbol: s => `BYBIT:${s}.P`,
    tfMap: { '1m':'1','5m':'5','15m':'15','30m':'30','1h':'60','4h':'240' },
    async universe(get) {
      const out = [];
      let cursor = '';
      for (let page = 0; page < 6; page++) {
        const qs = `category=linear&limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
        const j = await get(`/v5/market/instruments-info?${qs}`);
        if (j.retCode !== 0) throw new Error(`bybit instruments retCode ${j.retCode} ${j.retMsg || ''}`);
        for (const s of (j.result?.list || [])) {
          if (s.status === 'Trading' && s.quoteCoin === 'USDT' && s.contractType === 'LinearPerpetual') {
            out.push({ symbol: s.symbol, base: s.baseCoin });
          }
        }
        cursor = j.result?.nextPageCursor || '';
        if (!cursor) break;
      }
      return out;
    },
    async candles(get, sym, tf, limit) {
      const j = await get(`/v5/market/kline?category=linear&symbol=${sym}&interval=${tf}&limit=${limit}`, 2);
      if (j.retCode !== 0) throw new Error(`retCode ${j.retCode}`);
      const list = j.result?.list || [];
      // [start, open, high, low, close, volume, turnover] — NEWEST FIRST on Bybit
      return list.map(r => ({
        t: +r[0], o: +r[1], c: +r[4], v: +r[5], q: +r[6],
      })).reverse();
    },
  },
};

/** Resolve a working source + host, honouring SOURCE=auto|binance|bybit. */
async function resolveSource(want) {
  const order = want === 'auto' ? ['binance', 'bybit']
              : SOURCES[want]   ? [want]
              : null;
  if (!order) throw new Error(`unknown SOURCE "${want}" — use auto, binance or bybit`);

  const notes = [];
  let tfOk = 0;
  for (const name of order) {
    const src = SOURCES[name];
    if (!src.tfMap[CFG.tf]) {
      notes.push(`${name}: timeframe ${CFG.tf} not supported`);
      continue;
    }
    tfOk++;
    for (const host of src.hosts) {
      try {
        await req(host, src.ping, 1);
        const get = (path, tries = 3) => req(host, path, tries);
        console.log(`source: ${src.label} via ${host}`);
        return { name, src, host, get, tf: src.tfMap[CFG.tf] };
      } catch (e) {
        notes.push(`${host} — ${e.message}`);
        console.warn(`  ${host} unreachable (${e.message})`);
      }
    }
  }
  // Distinguish "nobody supports this timeframe" from "nobody answered us",
  // because the fixes are completely different.
  if (tfOk === 0) {
    throw new Error(
      `No data source supports TF="${CFG.tf}".\n  ${notes.join('\n  ')}\n\n` +
      `Supported: ${Object.keys(TF_MS).join(', ')}. Set TF in the workflow's env block.`
    );
  }
  throw new Error(
    `No market data host was reachable.\n  ${notes.join('\n  ')}\n\n` +
    (want === 'auto'
      ? 'Every venue refused this runner. If the errors say "restricted location",\n' +
        'the runner IP is geo-blocked — run this on a self-hosted runner or a non-US VPS instead.'
      : `Try SOURCE=auto so the scan can fall back to the other venue.`)
  );
}

// ─────────────────────── IGNITION DETECTION ───────────────────────
// Mirrors volume-squeeze.html. Returns null unless every gate passes.
// Exported so the logic can be tested against fixtures without network access.
export function detect(candles, c) {
  if (!Array.isArray(candles) || candles.length < c.winLen + c.baseLen + 2) return null;

  // Drop the still-forming candle — a half-built bar must never fake a rush
  const cl = candles.slice(0, candles.length - 1);
  const n  = cl.length;
  if (n < c.winLen + c.baseLen) return null;

  const win  = cl.slice(n - c.winLen);                           // oldest -> newest
  const base = cl.slice(n - c.winLen - c.baseLen, n - c.winLen);  // strictly BEFORE the window

  const baseVol = base.reduce((s, x) => s + (x.v || 0), 0) / base.length;
  if (!(baseVol > 0)) return null;

  const vols = win.map(x => x.v || 0);
  if (vols.some(v => !(v > 0))) return null;

  const ratios = vols.map(v => v / baseVol);
  const nv     = ratios.length;
  const rLast  = ratios[nv - 1];
  const rPeak  = Math.max(...ratios);
  const rCum   = vols.reduce((a, b) => a + b, 0) / baseVol;
  const elev   = ratios.filter(r => r >= c.elevMin).length;

  const half   = Math.floor(nv / 2);
  const oldSum = vols.slice(0, half).reduce((a, b) => a + b, 0);
  const newSum = vols.slice(half).reduce((a, b) => a + b, 0);
  const expand = oldSum > 0 ? newSum / oldSum : Infinity;

  const winOpen   = win[0].o || 0;
  const lastClose = win[nv - 1].c || 0;
  const pMove     = winOpen > 0 ? ((lastClose - winOpen) / winOpen) * 100 : 0;
  const lastUsd   = win[nv - 1].q || 0;
  const winUsd    = win.reduce((s, x) => s + (x.q || 0), 0);

  // Gates shared by both modes
  if (pMove   < c.moveMin) return null;  // volume must be directional, not churn
  if (lastUsd < c.usdMin)  return null;  // ignore dust markets with big ratios
  if (rCum    < c.cumMin)  return null;  // window must be heavy overall
  if (rLast   < c.peakMin) return null;  // final candle still strong

  if (c.mode === 'strict') {
    // Every candle must exceed the one before it by stepMin.
    for (let i = 1; i < nv; i++) {
      if (!(vols[i] >= vols[i - 1] * c.stepMin)) return null;
    }
  } else {
    if (elev   < c.minElev) return null;                      // kills the single lone spike
    if (expand < c.expMin)  return null;                      // kills flat-elevated mid-rally
    if (rPeak > 0 && rLast < rPeak * c.vsPeak) return null;   // kills the fading tail
  }

  return {
    ratios, rCum, rLast, elev, expand, pMove, winUsd, lastUsd,
    close: lastClose,
    barTs: win[nv - 1].t,
  };
}

// ─────────────────────────── STATE ───────────────────────────
async function loadState(p) {
  try { return JSON.parse(await readFile(p, 'utf8')); }
  catch { return { lastSignal: {} }; }
}
async function saveState(p, s) {
  try { await mkdir(dirname(p), { recursive: true }); await writeFile(p, JSON.stringify(s, null, 2)); }
  catch (e) { console.warn('state save failed:', e.message); }
}

// ─────────────────────────── EMAIL ───────────────────────────
function fmtNum(v) {
  if (!v || !Number.isFinite(v)) return '0';
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return v.toFixed(0);
}
const r2s = r => (r >= 10 ? r.toFixed(0) : r.toFixed(1)) + '×';

export function buildEmail(hits, cfg, srcLabel, tvSymbol) {
  const ist = new Date(Date.now() + 5.5 * 3600e3).toISOString().replace('T', ' ').slice(0, 16);
  const rows = hits.map(h => {
    const chain = h.ratios.map(r => {
      const col = r >= 30 ? '#f85149' : r >= 12 ? '#d29922' : r >= 2 ? '#58a6ff' : '#6e7681';
      return `<span style="color:${col};font-weight:600">${r2s(r)}</span>`;
    }).join('<span style="color:#3d4a5c"> &rarr; </span>');
    const tv = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol(h.symbol))}`;
    return `
    <tr>
      <td style="padding:12px 10px;border-bottom:1px solid #1c2333">
        <a href="${tv}" style="color:#e6edf3;font:600 15px ui-monospace,monospace;text-decoration:none">${h.base}</a>
        <div style="color:#3d4a5c;font:400 10px ui-monospace,monospace;margin-top:3px">${h.symbol} &middot; ${cfg.tf}</div>
      </td>
      <td style="padding:12px 10px;border-bottom:1px solid #1c2333;font:400 12px ui-monospace,monospace;white-space:nowrap">${chain}</td>
      <td style="padding:12px 10px;border-bottom:1px solid #1c2333;font:700 14px ui-monospace,monospace;color:#d29922;text-align:right">${h.rCum.toFixed(0)}&times;</td>
      <td style="padding:12px 10px;border-bottom:1px solid #1c2333;font:400 12px ui-monospace,monospace;color:#c9d1d9;text-align:right">$${fmtNum(h.winUsd)}</td>
      <td style="padding:12px 10px;border-bottom:1px solid #1c2333;font:600 12px ui-monospace,monospace;text-align:right;color:${h.pMove >= 0 ? '#3fb950' : '#f85149'}">${h.pMove >= 0 ? '+' : ''}${h.pMove.toFixed(1)}%</td>
    </tr>`;
  }).join('');

  const rules = cfg.mode === 'strict'
    ? `Each candle must be &ge;${cfg.stepMin}&times; the one before it, window total &ge;${cfg.cumMin}&times; baseline, final candle &ge;${cfg.peakMin}&times; baseline, price +${cfg.moveMin}%+.`
    : `Needs total &ge;${cfg.cumMin}&times; baseline, final candle &ge;${cfg.peakMin}&times; and &ge;${cfg.vsPeak}&times; the window peak, at least ${cfg.minElev} candles above ${cfg.elevMin}&times;, newer half &ge;${cfg.expMin}&times; older half, price +${cfg.moveMin}%+.`;

  return `<!doctype html><html><body style="margin:0;padding:0;background:#080b10">
<div style="max-width:680px;margin:0 auto;padding:22px 16px;font-family:-apple-system,Segoe UI,Roboto,sans-serif">
  <div style="font:600 11px ui-monospace,monospace;letter-spacing:3px;color:#d29922">ARB//IGNITION</div>
  <div style="font:700 21px/1.3 -apple-system,Segoe UI,Roboto,sans-serif;color:#e6edf3;margin:8px 0 3px">
    ${hits.length} volume ignition${hits.length > 1 ? 's' : ''} detected
  </div>
  <div style="font:400 11px ui-monospace,monospace;color:#3d4a5c;margin-bottom:18px">${ist} IST &middot; ${srcLabel} &middot; ${cfg.tf} &middot; ${cfg.mode}</div>

  <table style="width:100%;border-collapse:collapse;background:#0d1117;border:1px solid #1c2333;border-radius:8px;overflow:hidden">
    <tr style="background:#111823">
      <th style="padding:9px 10px;text-align:left;font:600 9px ui-monospace,monospace;letter-spacing:1px;color:#3d4a5c;text-transform:uppercase">Symbol</th>
      <th style="padding:9px 10px;text-align:left;font:600 9px ui-monospace,monospace;letter-spacing:1px;color:#3d4a5c;text-transform:uppercase">Escalation vs baseline</th>
      <th style="padding:9px 10px;text-align:right;font:600 9px ui-monospace,monospace;letter-spacing:1px;color:#3d4a5c;text-transform:uppercase">Total</th>
      <th style="padding:9px 10px;text-align:right;font:600 9px ui-monospace,monospace;letter-spacing:1px;color:#3d4a5c;text-transform:uppercase">Window $</th>
      <th style="padding:9px 10px;text-align:right;font:600 9px ui-monospace,monospace;letter-spacing:1px;color:#3d4a5c;text-transform:uppercase">Move</th>
    </tr>
    ${rows}
  </table>

  <div style="margin-top:16px;padding:12px 14px;background:#0d1117;border:1px solid #1c2333;border-radius:8px;font:400 11px/1.7 ui-monospace,monospace;color:#3d4a5c">
    Window of ${cfg.winLen} candles judged against the ${cfg.baseLen} quiet candles before it. ${rules}
    Each symbol stays silent for ${cfg.lockBars} bars after a hit.
  </div>
  <div style="margin-top:10px;font:400 10px ui-monospace,monospace;color:#2a3442">
    Signal, not advice. Verify on the chart before acting.
  </div>
</div></body></html>`;
}

async function sendEmail(hits, cfg, srcLabel, tvSymbol) {
  if (!MAIL.key || !MAIL.to) {
    console.log('\n(email not configured — set RESEND_API_KEY and MAIL_TO)');
    return false;
  }
  const subject = hits.length === 1
    ? `Ignition: ${hits[0].base}  ${r2s(hits[0].rCum)} vol, ${hits[0].pMove >= 0 ? '+' : ''}${hits[0].pMove.toFixed(1)}%`
    : `Ignition: ${hits.length} symbols  (${hits.slice(0, 4).map(h => h.base).join(', ')}${hits.length > 4 ? '…' : ''})`;

  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${MAIL.key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: MAIL.from,
      to: MAIL.to.split(',').map(s => s.trim()).filter(Boolean),
      subject,
      html: buildEmail(hits, cfg, srcLabel, tvSymbol),
    }),
  });
  if (!res.ok) {
    console.error('email failed:', res.status, (await res.text()).slice(0, 300));
    return false;
  }
  console.log(`email sent to ${MAIL.to}`);
  return true;
}

// ─────────────────────────── MAIN ───────────────────────────
async function main() {
  const c = CFG;
  if (c.mode !== 'flexible' && c.mode !== 'strict') {
    throw new Error(`unknown MODE "${c.mode}" — use flexible or strict`);
  }
  console.log(`Volume Ignition — ${c.tf}, ${c.mode} mode, window ${c.winLen}, baseline ${c.baseLen}`);

  const { src, get, tf } = await resolveSource(c.source);

  const universe = await src.universe(get);
  console.log(`universe: ${universe.length} perps`);
  if (!universe.length) throw new Error('empty universe — the exchange returned no tradable perps');

  const state = await loadState(c.statePath);
  state.lastSignal = state.lastSignal || {};
  const lockMs = c.lockBars * (TF_MS[c.tf] || 900e3);

  const limit = Math.max(c.winLen + c.baseLen + 5, 30);
  const hits = [];
  let scanned = 0, errors = 0;

  // Small worker pool — fast enough to finish inside a candle, gentle enough
  // not to trip the exchange's weight limits.
  const queue = universe.slice();
  async function worker() {
    while (queue.length) {
      const u = queue.shift();
      try {
        const candles = await src.candles(get, u.symbol, tf, limit);
        const d = detect(candles, c);
        scanned++;
        if (!d) continue;

        const prev = state.lastSignal[u.symbol];
        if (prev && d.barTs - prev < lockMs) continue;   // still locked out
        state.lastSignal[u.symbol] = d.barTs;
        hits.push({ ...d, symbol: u.symbol, base: u.base });
        console.log(`  HIT ${u.base.padEnd(12)} ${d.ratios.map(r2s).join(' -> ')}  total ${d.rCum.toFixed(0)}x  ${d.pMove >= 0 ? '+' : ''}${d.pMove.toFixed(1)}%`);
      } catch (e) {
        errors++;
        if (e.geo) throw e;
      }
    }
  }
  await Promise.all(Array.from({ length: c.concurrency }, worker));

  console.log(`\nscanned ${scanned}/${universe.length}  errors ${errors}  hits ${hits.length}`);

  // A run that mostly failed must not be mistaken for a quiet market, and must
  // not persist a state file built from partial data.
  if (errors > universe.length * 0.5) {
    throw new Error(`more than half the requests failed (${errors}) — treating this run as unreliable`);
  }

  // Forget lockouts older than a day so the state file stays small
  const cutoff = Date.now() - 86400e3;
  for (const [k, v] of Object.entries(state.lastSignal)) if (v < cutoff) delete state.lastSignal[k];
  await saveState(c.statePath, state);

  if (!hits.length) { console.log('no new ignitions'); return; }

  hits.sort((a, b) => b.rCum - a.rCum);
  if (c.dryRun) {
    console.log('\nDRY_RUN — email suppressed');
    return;
  }
  await sendEmail(hits, c, src.label, src.tvSymbol);
}

// Only sweep when run directly, so a test file can import detect() above.
// Compared through realpath: a naive `file://${argv[1]}` string match breaks
// whenever the path contains a symlink, and the failure mode is the worst one
// available here — the process exits 0 having scanned nothing, which on a cron
// is indistinguishable from a quiet market.
function isMainModule() {
  try {
    const self = realpathSync(fileURLToPath(import.meta.url));
    return !!process.argv[1] && self === realpathSync(process.argv[1]);
  } catch {
    return import.meta.url === `file://${process.argv[1]}`;
  }
}

if (isMainModule()) {
  main().catch(e => {
    console.error('\nSCAN FAILED:', e.message);
    process.exit(1);
  });
}
