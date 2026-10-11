#!/usr/bin/env node
/**
 * VOLUME IGNITION — server-side scanner
 *
 * Runs on GitHub Actions on a schedule. Sweeps every USDT perp on the chosen
 * venue, applies the same ignition logic as volume-squeeze.html, and emails
 * any new signals.
 *
 * Nothing here touches a browser. Your laptop can be shut and your phone off —
 * the email is produced by GitHub's servers and waits in your inbox.
 *
 * Zero dependencies. Node 18+ (global fetch).
 *
 * ── WHY NOT BINANCE ───────────────────────────────────────────────────────
 * Binance futures returns HTTP 451 to GitHub's runners (Azure US IPs), and
 * Bybit returns 403 from the same addresses. Both were probed and confirmed
 * blocked. Gate.io, MEXC, OKX, Bitget, Binance spot and Hyperliquid all
 * answered, so the scanner reads from those instead.
 *
 * This costs nothing in accuracy: the detector measures every candle against
 * that symbol's OWN baseline, so it is a ratio, and ratios don't care which
 * venue produced them — or even whether volume is denominated in coins or
 * contracts, as long as the unit is consistent down the column. Only usdMin
 * is absolute, and that reads the quote-volume field.
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
  concurrency: int(process.env.CONCURRENCY, 4),
  rateMs:      int(process.env.RATE_MS,     80), // min ms between request starts
  // Floor works backwards from usdMin: a symbol must be CAPABLE of an
  // ignition candle worth usdMin. Set this too high and you filter out the
  // small caps where ignitions actually happen, leaving only large liquid
  // names whose volume never spikes 20x — a scan that can never fire.
  min24h:      num(process.env.MIN_24H_USD, 500000),
  asterOnly:   process.env.ASTER_ONLY !== 'false',    // only alert on tradeable names
  asterHost:   process.env.ASTER_HOST || 'https://fapi.asterdex.com',
  maxSymbols:  int(process.env.MAX_SYMBOLS, 0),  // 0 = no cap
  statePath: process.env.STATE_PATH || '.state/ignition.json',
  dryRun:    process.env.DRY_RUN === 'true',
  testEmail: process.env.TEST_EMAIL === 'true',
};

const MAIL = {
  key:  process.env.RESEND_API_KEY,
  from: process.env.MAIL_FROM || 'Volume Ignition <onboarding@resend.dev>',
  to:   process.env.MAIL_TO,
};

const TF_MS = { '1m':60e3, '5m':300e3, '15m':900e3, '30m':1800e3, '1h':3600e3, '4h':14400e3 };
const tfMs  = () => TF_MS[CFG.tf] || 900e3;

function num(v, d) { const n = parseFloat(v); return Number.isFinite(n) ? n : d; }
function int(v, d) { const n = parseInt(v, 10); return Number.isFinite(n) ? n : d; }
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─────────────────────────── HTTP ───────────────────────────
// Gate.io lists over a thousand perps. Firing those off as fast as the worker
// pool can manage trips the venue's rate limiter within seconds, so every
// request passes through a global minimum-interval gate. Concurrency controls
// how many are in flight; this controls how fast they START.
let _nextSlot = 0;
let rateLimitHits = 0;
async function throttle() {
  const gap = CFG.rateMs;
  if (gap <= 0) return;
  const now = Date.now();
  const at = Math.max(now, _nextSlot);
  _nextSlot = at + gap;
  if (at > now) await sleep(at - now);
}

async function req(host, path, opts = {}) {
  const tries = opts.tries ?? 3;
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      await throttle();
      const res = await fetch(host + path, {
        method: opts.method || 'GET',
        headers: {
          'User-Agent': 'volume-ignition-scanner/2.1',
          ...(opts.body ? { 'Content-Type': 'application/json' } : {}),
          ...(opts.headers || {}),
        },
        body: opts.body,
      });
      if (res.status === 429 || res.status === 418) {
        // Record the error BEFORE continuing. Without this, a request that is
        // rate-limited on every single attempt falls out of the loop with
        // lastErr still undefined and throws `undefined`, which then explodes
        // in the caller's `catch (e) { e.geo }`.
        lastErr = new Error(`rate limited (HTTP ${res.status})`);
        lastErr.rateLimited = true;
        rateLimitHits++;
        const wait = 1500 * Math.pow(2, i) + Math.floor(Math.random() * 400);
        if (rateLimitHits <= 5) console.warn(`  rate limited (${res.status}), backing off ${wait}ms`);
        else if (rateLimitHits === 6) console.warn('  (further rate-limit warnings suppressed)');
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
      if (e && e.geo) throw e;
      await sleep(400 * (i + 1));
    }
  }
  // Never throw undefined — every caller assumes it received an Error.
  throw lastErr || new Error(`request failed after ${tries} attempts: ${path.slice(0, 60)}`);
}

// ───────────────────── SOURCE ADAPTERS ─────────────────────
// Each adapter returns candles as { t, o, c, v, q }:
//   t = open time ms · o = open · c = close · v = volume · q = quote (USD) volume
//
// `v` need not be in coins. Contracts are fine — the detector only ever
// compares a symbol's volume to its own baseline, so the unit cancels. `q`
// does need to be dollars, since usdMin is an absolute gate; where a venue
// doesn't publish it, the adapter derives it as v × close.
//
// Order and sanity are enforced centrally by normalise(), so an adapter that
// returns newest-first (OKX does) needs no special handling.

function normalise(rows) {
  return (rows || [])
    .filter(r => r && Number.isFinite(r.t) && r.c > 0 && Number.isFinite(r.v) && r.v >= 0)
    .sort((a, b) => a.t - b.t);
}

export const SOURCES = {
  // ── Confirmed reachable from GitHub runners ──────────────────────────
  gate: {
    label: 'Gate.io USDT perps',
    hosts: ['https://api.gateio.ws'],
    ping: '/api/v4/futures/usdt/contracts?limit=1',
    tvSymbol: s => `GATEIO:${s.replace('_', '')}.P`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h' },
    async universe(get) {
      const c = await get('/api/v4/futures/usdt/contracts');
      return (c || [])
        .filter(x => x && x.name && /_USDT$/.test(x.name) && !x.in_delisting)
        .map(x => ({ symbol: x.name, base: x.name.replace(/_USDT$/, '') }));
    },
    async tickers(get) {
      const t = await get('/api/v4/futures/usdt/tickers');
      const m = new Map();
      for (const x of (t || [])) {
        if (!x || !x.contract) continue;
        const q = Math.abs(+x.volume_24h_quote) || Math.abs(+x.volume_24h_settle) || 0;
        m.set(x.contract, q);
      }
      return m;
    },
    async candles(get, sym, tf, limit) {
      const k = await get(`/api/v4/futures/usdt/candlesticks?contract=${sym}&interval=${tf}&limit=${limit}`, { tries: 2 });
      // { t (sec), v (size), c, h, l, o, sum (quote volume) } — numbers or strings
      return normalise((k || []).map(r => {
        const c = Math.abs(+r.c) || 0, v = Math.abs(+r.v) || 0, sum = Math.abs(+r.sum) || 0;
        return { t: (+r.t) * 1000, o: Math.abs(+r.o) || 0, c, v, q: sum > 0 ? sum : v * c };
      }));
    },
  },

  mexc: {
    label: 'MEXC USDT perps',
    hosts: ['https://contract.mexc.com'],
    ping: '/api/v1/contract/ping',
    tvSymbol: s => `MEXC:${s.replace('_', '')}.P`,
    tfMap: { '1m':'Min1','5m':'Min5','15m':'Min15','30m':'Min30','1h':'Min60','4h':'Hour4' },
    async universe(get) {
      const c = await get('/api/v1/contract/detail');
      return (c.data || [])
        .filter(x => x && x.symbol && /_USDT$/.test(x.symbol) && (x.state === 0 || x.state === undefined))
        .map(x => ({ symbol: x.symbol, base: x.symbol.replace(/_USDT$/, '') }));
    },
    async tickers(get) {
      const t = await get('/api/v1/contract/ticker');
      const m = new Map();
      for (const x of (t.data || [])) {
        if (x && x.symbol) m.set(x.symbol, Math.abs(+x.amount24) || 0);
      }
      return m;
    },
    async candles(get, sym, tf, limit) {
      const start = Math.floor((Date.now() - (limit + 2) * tfMs()) / 1000);
      const k = await get(`/api/v1/contract/kline/${sym}?interval=${tf}&start=${start}`, { tries: 2 });
      // parallel arrays: { time[], open[], close[], high[], low[], vol[], amount[] }
      const d = k.data || {};
      const t = d.time || [];
      return normalise(t.map((ts, i) => {
        const c = +((d.close || [])[i]) || 0, v = +((d.vol || [])[i]) || 0;
        const amt = +((d.amount || [])[i]) || 0;
        return { t: ts * 1000, o: +((d.open || [])[i]) || 0, c, v, q: amt > 0 ? amt : v * c };
      }));
    },
  },

  okx: {
    label: 'OKX USDT swaps',
    hosts: ['https://www.okx.com'],
    ping: '/api/v5/public/time',
    tvSymbol: s => `OKX:${s.replace('-USDT-SWAP', 'USDT')}.P`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1H','4h':'4H' },
    async universe(get) {
      const c = await get('/api/v5/public/instruments?instType=SWAP');
      return (c.data || [])
        .filter(x => x && x.settleCcy === 'USDT' && x.state === 'live')
        .map(x => ({ symbol: x.instId, base: x.instId.replace(/-USDT-SWAP$/, '') }));
    },
    async tickers(get) {
      const t = await get('/api/v5/market/tickers?instType=SWAP');
      const m = new Map();
      for (const x of (t.data || [])) {
        if (!x || !x.instId) continue;
        const last = +x.last || 0;
        m.set(x.instId, (+x.volCcy24h || 0) * last || (+x.vol24h || 0) * last);
      }
      return m;
    },
    async candles(get, sym, tf, limit) {
      const k = await get(`/api/v5/market/candles?instId=${sym}&bar=${tf}&limit=${limit}`, { tries: 2 });
      // [ts, o, h, l, c, vol(contracts), volCcy(base), volCcyQuote(USD), confirm] — newest first
      return normalise((k.data || []).map(r => {
        const c = +r[4] || 0, v = +r[5] || 0;
        return { t: +r[0], o: +r[1] || 0, c, v, q: (+r[7] || +r[6] || 0) || v * c };
      }));
    },
  },

  bitget: {
    label: 'Bitget USDT perps',
    hosts: ['https://api.bitget.com'],
    ping: '/api/v2/public/time',
    tvSymbol: s => `BITGET:${s}.P`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1H','4h':'4H' },
    async universe(get) {
      const c = await get('/api/v2/mix/market/contracts?productType=USDT-FUTURES');
      return (c.data || [])
        .filter(x => x && x.symbol)
        .map(x => ({ symbol: x.symbol, base: x.baseCoin || x.symbol.replace(/USDT$/, '') }));
    },
    async tickers(get) {
      const t = await get('/api/v2/mix/market/tickers?productType=USDT-FUTURES');
      const m = new Map();
      for (const x of (t.data || [])) {
        if (!x || !x.symbol) continue;
        m.set(x.symbol, Math.abs(+x.usdtVolume) || (Math.abs(+x.baseVolume) || 0) * (+x.lastPr || 0));
      }
      return m;
    },
    async candles(get, sym, tf, limit) {
      const k = await get(`/api/v2/mix/market/candles?symbol=${sym}&productType=USDT-FUTURES&granularity=${tf}&limit=${limit}`, { tries: 2 });
      // [ts, o, h, l, c, baseVol, quoteVol, ...]
      return normalise((k.data || []).map(r => {
        const c = +r[4] || 0, v = +r[5] || 0;
        return { t: +r[0], o: +r[1] || 0, c, v, q: (+r[6] || 0) || v * c };
      }));
    },
  },

  'binance-spot': {
    label: 'Binance spot (USDT)',
    hosts: ['https://data-api.binance.vision'],
    ping: '/api/v3/ping',
    tvSymbol: s => `BINANCE:${s}`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h' },
    async universe(get) {
      const i = await get('/api/v3/exchangeInfo');
      return (i.symbols || [])
        .filter(s => s.status === 'TRADING' && s.quoteAsset === 'USDT' && s.isSpotTradingAllowed !== false)
        .map(s => ({ symbol: s.symbol, base: s.baseAsset }));
    },
    async tickers(get) {
      const t = await get('/api/v3/ticker/24hr');
      const m = new Map();
      for (const x of (t || [])) {
        if (x && x.symbol) m.set(x.symbol, Math.abs(+x.quoteVolume) || 0);
      }
      return m;
    },
    async candles(get, sym, tf, limit) {
      const k = await get(`/api/v3/klines?symbol=${sym}&interval=${tf}&limit=${limit}`, { tries: 2 });
      return normalise((k || []).map(r => ({ t: +r[0], o: +r[1] || 0, c: +r[4] || 0, v: +r[5] || 0, q: +r[7] || 0 })));
    },
  },

  hyperliquid: {
    label: 'Hyperliquid perps',
    hosts: ['https://api.hyperliquid.xyz'],
    ping: '/info',
    pingOpts: { method: 'POST', body: JSON.stringify({ type: 'meta' }) },
    tvSymbol: s => `HYPERLIQUID:${s}USD.P`,
    tfMap: { '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','4h':'4h' },
    async universe(get) {
      const m = await get('/info', { method: 'POST', body: JSON.stringify({ type: 'meta' }) });
      return (m.universe || [])
        .filter(x => x && x.name && !x.isDelisted)
        .map(x => ({ symbol: x.name, base: x.name }));
    },
    async tickers(get) {
      // [ meta, assetCtxs ] — the two arrays are positionally aligned
      const r = await get('/info', { method: 'POST', body: JSON.stringify({ type: 'metaAndAssetCtxs' }) });
      const names = r?.[0]?.universe || [];
      const ctxs  = r?.[1] || [];
      const m = new Map();
      names.forEach((u, i) => { if (u && u.name) m.set(u.name, Math.abs(+ctxs[i]?.dayNtlVlm) || 0); });
      return m;
    },
    async candles(get, sym, tf, limit) {
      const end = Date.now();
      const k = await get('/info', { method: 'POST', tries: 2, body: JSON.stringify({
        type: 'candleSnapshot',
        req: { coin: sym, interval: tf, startTime: end - (limit + 2) * tfMs(), endTime: end },
      }) });
      // [{ t, T, s, i, o, c, h, l, v, n }]
      return normalise((k || []).map(r => {
        const c = +r.c || 0, v = +r.v || 0;
        return { t: +r.t, o: +r.o || 0, c, v, q: v * c };
      }));
    },
  },

  // ── Blocked from GitHub runners, kept for use elsewhere ───────────────
  // Selectable with SOURCE=binance / SOURCE=bybit on a non-US VPS or a
  // self-hosted runner. Deliberately excluded from the "auto" chain.
  binance: {
    label: 'Binance USDT perps',
    hosts: ['https://fapi.binance.com', 'https://fapi1.binance.com', 'https://fapi2.binance.com'],
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
      const k = await get(`/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=${limit}`, { tries: 2 });
      return normalise((k || []).map(r => ({ t: +r[0], o: +r[1] || 0, c: +r[4] || 0, v: +r[5] || 0, q: +r[7] || 0 })));
    },
  },

  bybit: {
    label: 'Bybit USDT perps',
    hosts: ['https://api.bybit.com', 'https://api.bytick.com'],
    ping: '/v5/market/time',
    tvSymbol: s => `BYBIT:${s}.P`,
    tfMap: { '1m':'1','5m':'5','15m':'15','30m':'30','1h':'60','4h':'240' },
    async universe(get) {
      const out = []; let cursor = '';
      for (let page = 0; page < 6; page++) {
        const qs = `category=linear&limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
        const j = await get(`/v5/market/instruments-info?${qs}`);
        if (j.retCode !== 0) throw new Error(`bybit instruments retCode ${j.retCode}`);
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
      const j = await get(`/v5/market/kline?category=linear&symbol=${sym}&interval=${tf}&limit=${limit}`, { tries: 2 });
      if (j.retCode !== 0) throw new Error(`retCode ${j.retCode}`);
      // [start, o, h, l, c, volume, turnover] — newest first
      return normalise((j.result?.list || []).map(r => {
        const c = +r[4] || 0, v = +r[5] || 0;
        return { t: +r[0], o: +r[1] || 0, c, v, q: (+r[6] || 0) || v * c };
      }));
    },
  },
};

// Tried in this order when SOURCE=auto. Binance and Bybit are omitted
// because they are confirmed geo-blocked from GitHub's runners.
const AUTO_ORDER = ['gate', 'mexc', 'okx', 'bitget', 'binance-spot', 'hyperliquid'];

// ───────────────────── ASTER TRADEABILITY FILTER ─────────────────────
// The volume signal is best read from a liquid venue — a 20x candle on
// Gate.io means the market genuinely moved. But a signal you cannot trade is
// just noise at 3am, so the universe is narrowed to names Aster also lists.
// Aster runs a Binance-compatible API, so this is a single request.
//
// Venues spell the same pair differently (OGN_USDT, OGN-USDT-SWAP, OGNUSDT),
// so matching is done on letters and digits only.
const normSym = s => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

async function asterSymbols() {
  const j = await req(CFG.asterHost, '/fapi/v1/exchangeInfo', { tries: 2 });
  const out = new Set();
  for (const s of (j.symbols || [])) {
    if (!s || !s.symbol) continue;
    const trading = !s.status || s.status === 'TRADING';
    const perp    = !s.contractType || s.contractType === 'PERPETUAL';
    if (trading && perp) out.add(normSym(s.symbol));
  }
  return out;
}

/** Resolve a working source + host. */
async function resolveSource(want) {
  const order = want === 'auto' ? AUTO_ORDER
              : SOURCES[want]   ? [want]
              : null;
  if (!order) {
    throw new Error(`unknown SOURCE "${want}" — use auto, or one of: ${Object.keys(SOURCES).join(', ')}`);
  }

  const notes = [];
  let tfOk = 0;
  for (const name of order) {
    const src = SOURCES[name];
    if (!src.tfMap[CFG.tf]) { notes.push(`${name}: timeframe ${CFG.tf} not supported`); continue; }
    tfOk++;
    for (const host of src.hosts) {
      try {
        await req(host, src.ping, { tries: 1, ...(src.pingOpts || {}) });
        const get = (path, opts) => req(host, path, opts);
        console.log(`source: ${src.label} via ${host}`);
        return { name, src, host, get, tf: src.tfMap[CFG.tf] };
      } catch (e) {
        notes.push(`${host} — ${e.message}`);
        console.warn(`  ${host} unreachable (${e.message})`);
      }
    }
  }

  // "Nobody supports this timeframe" and "nobody answered" need different fixes.
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
        'the runner IP is geo-blocked — run this on a self-hosted runner or a non-US VPS.'
      : `Try SOURCE=auto so the scan can fall back to another venue.`)
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

export function buildEmail(hits, cfg, srcLabel, tvSymbol, isTest = false) {
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

  // A test email must be impossible to mistake for a real signal.
  const banner = isTest ? `
  <div style="margin-bottom:16px;padding:11px 13px;background:#1c1606;border:1px solid #4a3a08;border-radius:8px;font:600 12px/1.6 ui-monospace,monospace;color:#d29922">
    TEST EMAIL &middot; the rows below are made-up sample data, not live signals.<br>
    <span style="font-weight:400;color:#8b7a3a">Receiving this confirms your Resend key, secrets and inbox all work.</span>
  </div>` : '';

  return `<!doctype html><html><body style="margin:0;padding:0;background:#080b10">
<div style="max-width:680px;margin:0 auto;padding:22px 16px;font-family:-apple-system,Segoe UI,Roboto,sans-serif">
  <div style="font:600 11px ui-monospace,monospace;letter-spacing:3px;color:#d29922">ARB//IGNITION</div>
  <div style="font:700 21px/1.3 -apple-system,Segoe UI,Roboto,sans-serif;color:#e6edf3;margin:8px 0 3px">
    ${isTest ? 'Test email' : `${hits.length} volume ignition${hits.length > 1 ? 's' : ''} detected`}
  </div>
  ${banner}
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
    Each symbol stays silent for ${cfg.lockBars} bars after a hit.${cfg.asterOnly
      ? ' Universe limited to names also listed on Aster perps, so everything here is tradeable there.'
      : ''}
  </div>
  <div style="margin-top:10px;font:400 10px ui-monospace,monospace;color:#2a3442">
    Signal, not advice. Verify on the chart before acting.
  </div>
</div></body></html>`;
}

async function sendEmail(hits, cfg, srcLabel, tvSymbol, isTest = false) {
  if (!MAIL.key || !MAIL.to) {
    console.error('email NOT sent — secrets missing:');
    console.error(`  RESEND_API_KEY: ${MAIL.key ? 'set' : 'MISSING'}`);
    console.error(`  MAIL_TO:        ${MAIL.to  ? 'set' : 'MISSING'}`);
    console.error('  Repo Settings -> Secrets and variables -> Actions. Names are case-sensitive.');
    return false;
  }
  const subject = isTest
    ? 'TEST — Volume Ignition email is working'
    : hits.length === 1
      ? `Ignition: ${hits[0].base}  ${r2s(hits[0].rCum)} vol, ${hits[0].pMove >= 0 ? '+' : ''}${hits[0].pMove.toFixed(1)}%`
      : `Ignition: ${hits.length} symbols  (${hits.slice(0, 4).map(h => h.base).join(', ')}${hits.length > 4 ? '…' : ''})`;

  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${MAIL.key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: MAIL.from,
      to: MAIL.to.split(',').map(s => s.trim()).filter(Boolean),
      subject,
      html: buildEmail(hits, cfg, srcLabel, tvSymbol, isTest),
    }),
  });

  const body = await res.text();
  if (!res.ok) {
    // A bare status code sends you hunting. Name the actual cause.
    console.error(`email failed: HTTP ${res.status}`);
    console.error(`  response: ${body.slice(0, 400)}`);
    if (res.status === 401 || res.status === 403) {
      console.error('  CAUSE: the RESEND_API_KEY secret is wrong, revoked, or has a stray space.');
      console.error('         If you deleted the old key in Resend, the secret still holds it —');
      console.error('         create a new key and UPDATE the secret with it.');
    } else if (res.status === 422) {
      console.error('  CAUSE: Resend refused the recipient. The free onboarding@resend.dev sender');
      console.error('         only delivers to the address your Resend account was created with.');
      console.error('         Make MAIL_TO exactly that address, or verify your own domain.');
    } else if (res.status === 429) {
      console.error('  CAUSE: Resend rate limit — free tier allows 100 emails/day.');
    }
    return false;
  }
  let id = '';
  try { id = JSON.parse(body).id || ''; } catch {}
  console.log(`email accepted by Resend${id ? ` (id ${id})` : ''} -> ${MAIL.to}`);
  console.log('If it does not arrive, check spam, and check Resend -> Emails for the delivery status.');
  return true;
}

// ─────────────────────────── MAIN ───────────────────────────
export async function main() {
  const c = CFG;
  if (c.mode !== 'flexible' && c.mode !== 'strict') {
    throw new Error(`unknown MODE "${c.mode}" — use flexible or strict`);
  }
  console.log(`Volume Ignition — ${c.tf}, ${c.mode} mode, window ${c.winLen}, baseline ${c.baseLen}`);

  // TEST_EMAIL proves the delivery chain — key, secrets, inbox, spam rules —
  // without waiting for the market to produce a signal. Deliberately runs
  // BEFORE any market request, so "is email working" stays a separate
  // question from "can this runner reach the exchange".
  if (c.testEmail) {
    console.log('TEST_EMAIL — sending a sample alert, no market data fetched');
    const sample = [
      { symbol:'OGN_USDT',   base:'OGN',   ratios:[3.2,2.3,19,9.3,46], rCum:80, winUsd:294e6,  pMove:12.4 },
      { symbol:'PENGU_USDT', base:'PENGU', ratios:[2.1,4.4,8.8,17,31], rCum:63, winUsd:88.4e6, pMove:7.8  },
    ];
    const ok = await sendEmail(sample, c, 'sample data', s => `GATEIO:${s.replace('_', '')}.P`, true);
    if (!ok) throw new Error('test email did not send — see the cause above');
    return;
  }

  const { name, src, get, tf } = await resolveSource(c.source);

  let universe = await src.universe(get);
  console.log(`universe: ${universe.length} perps`);
  if (!universe.length) throw new Error('empty universe — the exchange returned no tradable perps');

  // Narrow to names you can actually trade on Aster. Every guard here fails
  // OPEN — an unreachable Aster or a symbol-shape mismatch scans the full
  // universe and says so, rather than silently alerting on nothing. Silence
  // is the one failure mode you would never notice.
  if (c.asterOnly) {
    try {
      const aster = await asterSymbols();
      const before = universe.length;
      const kept = aster.size >= 5 ? universe.filter(u => aster.has(normSym(u.symbol))) : [];
      if (aster.size < 5) {
        console.warn(`Aster returned only ${aster.size} symbols — ignoring the filter, scanning unfiltered`);
      } else if (kept.length < 5) {
        console.warn(`Aster filter matched only ${kept.length} of ${before} — symbol shapes may not line up; scanning unfiltered`);
      } else {
        universe = kept;
        console.log(`Aster filter: ${before} -> ${universe.length} also listed on Aster perps (Aster lists ${aster.size})`);
      }
    } catch (e) {
      console.warn(`Aster filter skipped (${e.message}) — scanning unfiltered.`);
      console.warn('  If this repeats, Aster may be unreachable from the runner; set ASTER_ONLY=false to stop trying.');
    }
  }

  // Drop illiquid listings before fetching a single candle. Gate.io alone
  // lists over a thousand perps and most are dust that could never clear
  // USD_MIN anyway — scanning them only burns rate limit. One ticker request
  // replaces hundreds of pointless candle requests.
  if (c.min24h > 0 && typeof src.tickers === 'function') {
    try {
      const vol = await src.tickers(get);
      if (vol && vol.size) {
        const before = universe.length;
        const kept = universe.filter(u => (vol.get(u.symbol) || 0) >= c.min24h);
        // Guard against a misread ticker field, which reads as zero volume
        // everywhere and would silently empty the universe. Judge by what was
        // REMOVED, not by the absolute count — after the Aster filter a
        // legitimate universe can be small, and keeping all 7 of 7 is a pass,
        // not a failure.
        if (kept.length === 0) {
          console.warn(`liquidity filter removed everything (${before} symbols) — the 24h volume field looks misread; scanning unfiltered`);
        } else {
          // Cutting three quarters of the universe is worth flagging: what
          // survives a high floor is the liquid majors, whose volume never
          // spikes enough to fire. Too high a floor is a silent scanner.
          if (kept.length < before * 0.25) {
            console.warn(`liquidity filter is aggressive: ${before} -> ${kept.length} (${(100 * kept.length / before).toFixed(0)}%).`);
            console.warn('  A high floor leaves only large caps, which rarely ignite. Consider lowering MIN_24H_USD.');
          }
          universe = kept;
          console.log(`liquidity filter: ${before} -> ${universe.length} with 24h volume >= $${fmtNum(c.min24h)}`);
        }
      }
    } catch (e) {
      console.warn(`liquidity filter skipped (${e.message}) — scanning the full universe`);
    }
  }

  if (c.maxSymbols > 0 && universe.length > c.maxSymbols) {
    universe = universe.slice(0, c.maxSymbols);
    console.log(`  capped to ${universe.length} (MAX_SYMBOLS)`);
  }

  const limit = Math.max(c.winLen + c.baseLen + 5, 30);

  // Adapter sanity check. Field names and units differ at every venue, and a
  // wrong guess reads as zero volume on EVERY symbol — a scan that completes
  // cleanly and finds nothing, which is indistinguishable from a quiet market.
  // Fail loudly here instead of reporting silence for weeks.
  const probe = await src.candles(get, universe[0].symbol, tf, limit);
  // Judge the last CLOSED candles, never the forming one. Seconds after a
  // candle opens it can legitimately show zero volume, and failing on that
  // would abort a perfectly healthy scan at :00, :15, :30 and :45.
  const closed = probe.slice(0, -1).slice(-5);
  const newest = closed[closed.length - 1];
  console.log(`parse check ${universe[0].symbol}: ${probe.length} candles, newest closed ${JSON.stringify(newest)}`);
  if (!closed.length || !closed.some(x => x.v > 0 && x.c > 0)) {
    throw new Error(`the "${name}" adapter parsed no usable price/volume across ${closed.length} closed candles — the venue's response shape has changed`);
  }
  if (!closed.some(x => x.q > 0)) {
    console.warn('  WARNING: quote volume is zero, so the USD_MIN gate will reject everything.');
  }

  const state = await loadState(c.statePath);
  state.lastSignal = state.lastSignal || {};
  const lockMs = c.lockBars * tfMs();

  const hits = [];
  let scanned = 0, errors = 0;

  // Small worker pool — fast enough to finish inside a candle, gentle enough
  // not to trip the venue's rate limits.
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
        console.log(`  HIT ${u.base.padEnd(12)} ${d.ratios.map(r2s).join(' -> ')}  total ${d.rCum.toFixed(0)}x  ${d.pMove >= 0 ? '+' : ''}${d.pMove.toFixed(1)}%  $${fmtNum(d.winUsd)}`);
      } catch (e) {
        errors++;
        if (e && e.geo) throw e;   // e can be anything a throw site produced
        if (errors <= 3) console.warn(`  ${u.symbol}: ${e && e.message ? e.message : e}`);
      }
    }
  }
  await Promise.all(Array.from({ length: c.concurrency }, worker));

  console.log(`\nscanned ${scanned}/${universe.length}  errors ${errors}  hits ${hits.length}` +
              (rateLimitHits ? `  (rate limited ${rateLimitHits}x — raise RATE_MS if this is high)` : ''));

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
  if (c.dryRun) { console.log('\nDRY_RUN — email suppressed'); return; }
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
