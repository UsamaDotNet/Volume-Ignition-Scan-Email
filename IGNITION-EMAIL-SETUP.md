# Volume Ignition — email alerts without anything running

TradingView can email you with your laptop shut because the check happens on
*their* servers, not in your browser. This does the same thing with GitHub's
servers doing the checking.

You end up with two independent layers:

| | Runs where | When it fires | Needs |
|---|---|---|---|
| **Browser alerts** | Your laptop / phone | Page open | nothing new — already built |
| **Email alerts** | GitHub's servers | Always, every 15 min | 15 minutes of setup below |

They do not know about each other. Both can fire for the same signal; that's
intended — one catches you at the desk, the other catches you asleep.

---

## Your current setup is not touched

Zero edits to `arb-dashboard.html`, `watchlist.html`, `scanner.html` or
`volume-squeeze.html`. Nothing is removed, nothing is rewired. This adds two
new files that your pages never load and your browser never runs.

**Strongest version of that guarantee:** put these two files in a **separate
repo** from the one Cloudflare Pages builds. Then there is no shared build, no
shared deploy, and literally no mechanism by which this can affect your live
site. That separate repo also wants to be **public** — see the minutes note
below. Your API key lives in GitHub Secrets, which stay encrypted and hidden
even on a public repo.

You *can* drop them into your existing repo instead and it will work fine —
Cloudflare ignores `.github/` — but then the scanner file gets served as a
public static file alongside your pages, and you inherit the private-repo
minutes limit.

---

## What I need from you

Two things, both free:

1. **A Resend API key** — the email sender.
2. **Your email address** — where alerts land. (Resend's free test sender only
   delivers to the address you sign up with, so use the one you actually read.)

That's it. No card, no domain, no server.

---

## Setup

### 1. Resend (3 minutes)

1. Sign up at **resend.com** with the email you want alerts at.
2. **API Keys** → **Create API Key** → permission **Sending access**.
3. Copy it. It starts `re_` and is shown **once**.

Free tier at time of writing is 3,000 emails/month, 100/day. The scanner
batches every hit into one email per run, so even a wild day is a handful.

You don't need to verify a domain. The workflow sends from
`onboarding@resend.dev`, Resend's test sender, which delivers to your signup
address only. If you later want it coming from your own domain, verify the
domain in Resend and change `MAIL_FROM` in the workflow — nothing else changes.

### 2. The repo (4 minutes)

Create a new **public** repo — call it `ignition-alerts`. Add two files:

```
ignition-alerts/
├── ignition-scan.mjs                      ← at the root
└── .github/workflows/ignition-alerts.yml  ← exactly this path
```

The path of the YAML matters. GitHub only looks in `.github/workflows/`.

### 3. Secrets (2 minutes)

In that repo: **Settings → Secrets and variables → Actions → New repository
secret**. Add two.

| Name | Value |
|---|---|
| `RESEND_API_KEY` | the `re_...` key |
| `MAIL_TO` | your email (comma-separate for several) |

Spelling must match exactly — a typo here fails silently with
`(email not configured)` in the log rather than an error.

### 4. First run — dry, on purpose

**Actions** tab → **Volume Ignition alerts** → **Run workflow** → set
**dry_run** to `true` → **Run**.

Dry first because your lockout memory starts empty, so that first run reports
*everything* currently qualifying, not just what's new. On a trending day
that could be a dozen symbols. Look at the log, see the count, then decide
whether to tighten thresholds before letting it email you.

A good log looks like:

```
Volume Ignition — 15m, flexible mode, window 5, baseline 5
source: Binance USDT perps via https://fapi.binance.com
universe: 512 perps
  HIT OGN          3.2× -> 2.3× -> 19× -> 9.3× -> 46×  total 80x  +12.4%
scanned 512/512  errors 0  hits 1
DRY_RUN — email suppressed
```

Then run it again with **dry_run** `false`. If the email arrives, you're done —
the cron takes over and it runs every 15 minutes forever.

---

## Two things that can go wrong, and what happens

### Binance blocks the runner

Binance returns HTTP 451 to datacenter IPs in restricted regions, and GitHub's
hosted runners are Azure US. There's a real chance Binance simply refuses.

The scanner handles this itself. `SOURCE=auto` tries all four Binance hosts,
and if they all refuse it **falls back to Bybit USDT perps** automatically and
keeps scanning. You'll see it in the log:

```
  https://fapi.binance.com unreachable (restricted location (HTTP 451))
  ...
source: Bybit USDT perps via https://api.bybit.com
```

This fallback is sound rather than a bodge: the detector measures every candle
against *that symbol's own* baseline, so it's a ratio, and ratios don't care
which venue produced them. A rush is a rush. Only `USD_MIN` is absolute, and a
dollar is a dollar on either exchange. The one real difference is coverage —
Bybit lists a somewhat different set of perps, so a Binance-only listing won't
be scanned while on fallback.

If *both* venues refuse the runner, the scan fails loudly with the reason
instead of quietly reporting "no signals". That distinction matters: a silent
scanner looks exactly like a quiet market.

### Actions minutes

This is the one real cost trap.

Every 15 minutes is **2,880 runs/month**, and GitHub bills a minimum of one
minute per run.

- **Public repo** → standard runners are **unlimited and free**. Nothing to think about.
- **Private repo, free plan** → **2,000 minutes/month**. You'd run dry around day 20.

Hence the public-repo recommendation. If you'd rather keep it private, change
the cron to `'3,33 * * * *'` — every 30 minutes, ~1,440 runs, comfortably
inside the free allowance. The cost is that you may see a signal one candle
later than the browser would show it.

---

## Honest limitations

**GitHub's scheduler is not punctual.** Free-tier cron can be delayed, and
occasionally by 10–20 minutes under load. It will not miss signals — the window
is still there on the next run — but the email is not a to-the-second thing.
When timing matters, the browser alert is your fast path and the email is the
safety net. That's the right division of labour anyway.

**Scheduled workflows get switched off after 60 days of repo inactivity.**
GitHub emails you first. Any commit, or one manual **Run workflow**, resets the
clock. Worth a calendar nudge every couple of months.

**One email per run, not per symbol.** All hits in a sweep arrive as one email
with a row each. Each symbol then goes quiet for `LOCKOUT` bars (20 × 15m = 5
hours) so a day-long rally doesn't mail you twenty times.

**The lockout lives in the Actions cache.** GitHub evicts caches untouched for
7 days, and the workflow refreshes it every run, so it persists in practice. If
it ever is evicted you'd get one larger catch-up email, not a flood.

---

## About the mode — worth reading before you go live

You settled on **strict escalation** for the Pine indicator. While testing the
server-side logic against fixtures, I found something that explains why OGN
stopped showing a dot:

**Strict mode with a 5-candle window cannot match OGN.** Strict requires every
candle in the window to beat the one before it. OGN escalated for *three*
candles — 14.36M → 81.7M → 198M — sitting on two flat 3–6M candles before them.
Those flat candles are inside a 5-wide window, they don't escalate, and strict
rejects the whole thing. It isn't a threshold being slightly off; the shape
can't fit the test.

Verified directly, same OGN numbers through all three configurations:

| Pattern | Flexible, window 5 | Strict, window 5 | Strict, window 3 |
|---|---|---|---|
| OGN `quiet, quiet, 14.36, 81.7, 198` | **fires** | — | **fires** |
| Your messy case `14, 10, 81, 40, 200` | **fires** | — | — |
| Clean 5-step `8, 14, 30, 81, 198` | **fires** | **fires** | **fires** |
| Lone spike, fading tail, flat-elevated, no price move, dust market | — | — | — |

So if you want strict, set **`WINDOW: '3'`** in the workflow. The window has to
be as wide as the escalation you're describing and no wider. Mismatch that and
it goes silent on exactly the pattern you built it for.

The workflow ships on **flexible / window 5**, which fires on OGN *and* on your
`14 → 10 → 81 → 40 → 200` case. The decoys stay rejected under either setting.

Same correction applies to the Pine script if you switch it to strict — set its
"Rush window" input to 3. Also worth noting: the Pine currently uses
`elevMin 1.8` while the web screener and this scanner use `2.0`. Say the word
and I'll align them.

---

## Tuning

Every threshold is in the workflow's `env:` block, so you change numbers in the
YAML and never touch the code. Commit, and the next run uses them.

| Too many emails | Too few |
|---|---|
| `CUM_MIN` 20 → 30 | `CUM_MIN` 20 → 12 |
| `MOVE_MIN` 4 → 6 | `MOVE_MIN` 4 → 2 |
| `USD_MIN` 200000 → 500000 | `USD_MIN` 200000 → 100000 |
| `MIN_ELEV` 3 → 4 | `ELEV_MIN` 2 → 1.8 |
| `LOCKOUT` 20 → 40 | |

Change one at a time and give it a day. Moving three at once leaves you unable
to tell which one did it.

To scan a different timeframe, set `TF` to `5m`, `30m`, `1h` or `4h` and move
the cron to match — `TF: '1h'` wants `cron: '3 * * * *'`, not every 15 minutes.

---

## Cheat sheet

| Symptom in the log | Cause | Fix |
|---|---|---|
| `(email not configured)` | secret name typo | check `RESEND_API_KEY` / `MAIL_TO` spelling |
| `email failed: 403` | bad or revoked key | new key in Resend, update the secret |
| `email failed: 422` | `MAIL_TO` isn't your Resend signup address | use that address, or verify a domain |
| `source: Bybit ...` | Binance refused the runner | nothing — working as designed |
| `No market data host was reachable` | both venues refused | self-hosted runner or a non-US VPS |
| `more than half the requests failed` | rate limited | drop `CONCURRENCY` 6 → 3 |
| nothing runs at all | Actions disabled on new repos | Actions tab → enable workflows |
| `scanned 0/0` | empty universe | check `TF` is one of the supported values |
