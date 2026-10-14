# S&P 500 IR RSS Feed Finder

Downloads the S&P 500 constituent list, enriches each company with Yahoo Finance
data, and discovers RSS/Atom feeds for the companies' Investor Relations (IR)
publications.

## Data sources

| Step | Source | What we take |
|------|--------|--------------|
| 1 | Wikipedia *List of S&P 500 companies* | ticker, name, GICS sector |
| 2 | Yahoo Finance (via `yfinance`) | long name, sector, industry, website |
| 3 | The company's IR website | IR page + verified RSS/Atom feed URL |

## How feed discovery works

For each company we derive candidate IR URLs from the Yahoo-listed website
(`investor.<domain>`, `<domain>/investors`, `<domain>/investor-relations`, ...),
then:

1. Pick the best IR landing page (URL or page text must look like IR).
2. Collect feed URLs via HTML autodiscovery (`<link rel="alternate">`), RSS/feed
   anchors, and a scan of likely press-release / news subpages.
3. Probe a list of well-known IR feed paths (`/rss`, `/press-releases/rss`, ...).
4. Verify every candidate with `feedparser` (must parse and contain a title or
   entries) before reporting it.

`feed_status` values:

- `verified`   – a feed was found and parsed successfully
- `not_found`  – IR page reached, but no valid feed found
- `blocked`    – the IR site returned 403 / blocked automated access
- `no_ir_page` – no IR page could be reached
- `no_website` – Yahoo had no website for the company

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Usage

```bash
# Full S&P 500 run
.venv/bin/python sp500_ir_feeds.py

# Quick test
.venv/bin/python sp500_ir_feeds.py --limit 25

# Specific tickers
.venv/bin/python sp500_ir_feeds.py --tickers AAPL,MSFT,KO

# List + Yahoo data only (skip feed discovery)
.venv/bin/python sp500_ir_feeds.py --no-feed-discovery
```

Options: `--workers N` (default 8), `--no-cache` (ignore the Yahoo cache).

## Outputs

Written to `output/`:

- `sp500_ir_feeds.csv`
- `sp500_ir_feeds.json`

Yahoo Finance responses are cached in `.cache/yahoo.json`, so re-running feed
discovery only does not re-hit Yahoo.

## Notes / limitations

- Not every company publishes an IR RSS feed. Many large-cap IR sites use email
  alerts (Q4, Nasdaq IR, etc.) instead of RSS, so `not_found` is a legitimate,
  common result.
- Some IR sites aggressively block automated clients (Cloudflare -> 403). Those
  are flagged `blocked` for manual review.
- This tool performs polite, low-volume HTTP requests; respect each site's
  terms of service and `robots.txt` when running at scale.

---

# Comprehensive IR data (`sp500_ir_sources.py`)

RSS alone only covers ~19% of the S&P 500 (and many IR sites block bots). This
script layers **universal** sources so every constituent is covered.

| Source | Coverage | What we get |
|--------|----------|-------------|
| SEC EDGAR submissions API | 503/503 | CIK, links to latest 10-K / 10-Q / 8-K / DEF 14A, 1-year filing counts, per-company Atom feed, latest earnings 8-K + its EX-99 press-release exhibit |
| Google News RSS | 503/503 | Company-specific news/press feed (verified, with entry count) |
| IR website (prior run) | 94/503 | IR landing page + native IR RSS feed |

```bash
.venv/bin/python sp500_ir_sources.py             # full run
.venv/bin/python sp500_ir_sources.py --limit 25
.venv/bin/python sp500_ir_sources.py --no-exhibit
```

Outputs: `output/sp500_ir_sources.csv` / `.json`.

## Alternative sources evaluated

| Approach | Verdict |
|----------|---------|
| **SEC EDGAR** (submissions JSON + Atom + XBRL) | ✅ Chosen — authoritative, free, 100% coverage |
| **Google News RSS search** | ✅ Chosen — 100% coverage, no key, per-company feed |
| Wire services (Business Wire, PR Newswire, GlobeNewswire) | ⚠️ Rich press releases, but 403 to bots; need TLS impersonation (`curl_cffi`) or a paid license |
| Commercial news APIs (FMP, Finnhub, Polygon, Marketaux, NewsAPI) | ⚠️ Great quality, need API keys / paid tiers |
| IR platform providers (Q4, Nasdaq IR, EQS/DFIN) | ⚠️ Host a large share of IR sites with predictable URL patterns, but no public API |
| IR site `sitemap.xml` instead of RSS | ⚠️ Works when RSS is absent; more parsing, less standardized |
| Headless browser (Playwright) | ⚠️ Beats Cloudflare/JS sites, but heavy and slow at 500-company scale |
| EDGAR full-text search API (`efts.sec.gov`) | ✅ Works; good for topic-specific press-release hunting |

## Notes / limitations

- SEC fair-access policy is ~10 requests/sec. `sp500_ir_sources.py` throttles to
  ~4 req/sec and backs off on HTTP 429. `www.sec.gov/Archives` can still return
  429 after bursts; parsed results are cached in `.cache/edgar/` so re-runs only
  fetch what is missing.
- A handful of constituents report earnings via 6-K/20-F instead of an 8-K
  item 2.02, so `earnings_release_url` may be empty for foreign issuers.

---

# Full IR data platform (`ir/` package + Flask app)

The scripts above are one-shot CSV exporters. The `ir` package builds a
resumable **SQLite** database over the **entire SEC-listed universe**
(10,445 tickers — a superset of every US ticker on Yahoo Finance), gathering IR
data from every viable source, and serves it through a Flask website.

## Database (`output/ir.db`)

| Table | Contents |
|-------|----------|
| `companies` | ticker, name, CIK, exchange, sector/industry, IR website + IR page, S&P 500 flag |
| `filings` | recent SEC filings (10-K/10-Q/8-K/DEF 14A/...) with `is_earnings` flag |
| `press_releases` | `google_news`, `prnewswire`, `globenewswire`, and full-text `edgar_8k` earnings releases |
| `metrics` | financials from XBRL: revenue, gross/operating/net income, diluted EPS — US-GAAP (`10-Q`/`10-K`) with automatic **IFRS (`ifrs-full:`) fallback for foreign 20-F/40-F/6-K filers** (home currency preserved) |
| `feeds` | discovered feeds (IR native RSS, EDGAR Atom, Google News) with verification |
| `sync` | per-company/per-stage status → runs are **resumable** and skip completed work |

## Collector

```bash
# Whole S&P 500 (fast, good demo data)
.venv/bin/python -m ir.collect --universe sp500 --workers 6

# Entire SEC universe (~10,445 tickers; long-running)
.venv/bin/python -m ir.collect --universe all --workers 6

# Subsets / stages
.venv/bin/python -m ir.collect --tickers AAPL,MSFT,KO --stages edgar,financials
.venv/bin/python -m ir.collect --universe sp500 --stages ir_rss,wires --force
```

Stages: `yahoo, ir_rss, edgar, news, wires, financials`. Everything is cached
(`.cache/`) and idempotent; re-running resumes from `sync`.

## Web app

```bash
.venv/bin/python ir_app.py        # http://localhost:5000
```

- `/` — searchable, filterable company grid (S&P 500 / has-RSS filters, paging)
- `/company/<TICKER>` — profile, feeds, financials table, filings, press releases
  (earnings releases expandable inline)
- `/stats` — dataset coverage
- `/api/companies`, `/api/company/<TICKER>` — JSON

## Convenience wrapper

```bash
./run.sh collect-sp500 | collect-all | refine-sp500 | web | feeds | stats
```

## Ticker + feeds CSV

`export_feeds.py` writes a diff-friendly `output/ticker_feeds.csv` mapping every
ticker to its discovered feeds (one column per feed kind, verified feeds only).
This file is committed to the repo so consumers can grab it directly:

```bash
.venv/bin/python export_feeds.py            # -> output/ticker_feeds.csv
./run.sh feeds
```

Columns: `ticker, name, exchange, in_sp500, edgar_atom, google_news, ir_rss`
(extra feed kinds appear automatically as new columns).

## Notes / limitations

- Universe backbone is SEC `company_tickers.json` (10,445 entries, all with CIK).
  Nasdaq Trader symbol files add exchange labels but are intermittently
  rate-limited, so they're best-effort with disk caching.
- Wire sources: **PR Newswire** and **GlobeNewswire** work via `curl_cffi` TLS
  impersonation; **Business Wire** hard-blocks bots (HTTP 403) and is skipped.
- Full-universe `wires` is the slowest stage (~hours at 10k companies); run the
  S&P 500 first, then expand.

