#!/usr/bin/env python3
"""
Download the S&P 500 constituent list, enrich each company with Yahoo Finance
data, and attempt to discover an RSS/Atom feed for the company's Investor
Relations (IR) publications.

Sources
-------
* Constituent list : Wikipedia "List of S&P 500 companies" (first table).
* Company details  : Yahoo Finance (via yfinance) -> long name, sector, website.
* IR RSS feed      : feed autodiscovery (<link rel="alternate"> on the IR page),
                     scanning IR pages for RSS/feed links, and probing a list of
                     well-known IR feed paths. Every candidate is verified with
                     feedparser before being reported.

Usage
-----
    python sp500_ir_feeds.py                 # full run
    python sp500_ir_feeds.py --limit 25      # first 25 tickers (quick test)
    python sp500_ir_feeds.py --tickers AAPL,MSFT,KO
    python sp500_ir_feeds.py --no-feed-discovery   # only list + Yahoo data

Outputs (in ./output):
    sp500_ir_feeds.csv
    sp500_ir_feeds.json
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
import feedparser
from bs4 import BeautifulSoup

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(HERE, ".cache")
OUTPUT_DIR = os.path.join(HERE, "output")

# Subdomains that commonly host an IR site.
IR_SUBDOMAINS = ["investor", "investors", "ir"]
# Paths on the corporate domain that commonly host the IR section.
IR_PATHS = [
    "/investors",
    "/investor-relations",
    "/investor",
    "/ir",
    "/about/investors",
    "/company/investors",
    "/corporate/investors",
    "/en/investors",
    "/investors-relations",
]
# Well-known feed endpoints appended to an IR base URL.
COMMON_FEED_PATHS = [
    "/rss",
    "/feed",
    "/feed.xml",
    "/rss.xml",
    "/atom.xml",
    "/news/rss",
    "/news/feed",
    "/news/rss.xml",
    "/press-releases/rss",
    "/press-releases/feed",
    "/rss/news",
    "/rss/press-releases",
    "/rss/press-releases.xml",
    "/investor-relations/rss",
    "/en/rss",
    "/news-events/press-releases/rss",
]

REQUEST_TIMEOUT = 15
MAX_FEED_CANDIDATES = 12  # cap verifications per company

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


SESSION = make_session()


def http_get(url: str, session: requests.Session | None = None,
             timeout: int = REQUEST_TIMEOUT, retries: int = 2):
    """GET a URL, returning the Response or None on failure."""
    session = session or SESSION
    for attempt in range(retries + 1):
        try:
            return session.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
    return None


# --------------------------------------------------------------------------- #
# Step 1: S&P 500 constituents
# --------------------------------------------------------------------------- #
def fetch_sp500() -> list[dict]:
    log("[1/3] Fetching S&P 500 constituents from Wikipedia ...")
    r = http_get(WIKI_URL)
    if r is None or r.status_code != 200:
        raise RuntimeError(f"Failed to fetch Wikipedia ({getattr(r,'status_code',None)})")
    tables = pd.read_html(io.StringIO(r.text))
    df = tables[0]
    df = df[["Symbol", "Security", "GICS Sector"]].copy()
    df.columns = ["ticker", "name", "sector"]
    df["ticker"] = df["ticker"].astype(str).str.strip()
    companies = df.to_dict("records")
    log(f"      found {len(companies)} constituents")
    return companies


# --------------------------------------------------------------------------- #
# Step 2: Yahoo Finance enrichment (cached)
# --------------------------------------------------------------------------- #
def _load_cache(name: str) -> dict:
    path = os.path.join(CACHE_DIR, name)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cache(name: str, data: dict) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1)
    os.replace(tmp, path)


def enrich_one(ticker: str, cache: dict, use_cache: bool = True) -> dict:
    if use_cache and ticker in cache:
        return cache[ticker]
    import yfinance as yf

    out = {"ticker": ticker, "yahoo_name": None, "yahoo_sector": None,
           "yahoo_website": None, "yahoo_industry": None, "yahoo_ok": False}
    for attempt in range(2):
        try:
            info = yf.Ticker(ticker).info or {}
            out["yahoo_name"] = info.get("longName") or info.get("shortName")
            out["yahoo_sector"] = info.get("sector")
            out["yahoo_industry"] = info.get("industry")
            out["yahoo_website"] = info.get("website")
            out["yahoo_ok"] = bool(out["yahoo_name"] or out["yahoo_website"])
            break
        except Exception:
            if attempt == 0:
                time.sleep(1.0)
    return out


def enrich_yahoo(companies: list[dict], workers: int, use_cache: bool) -> None:
    log(f"[2/3] Enriching {len(companies)} companies via Yahoo Finance "
        f"({workers} workers) ...")
    cache = _load_cache("yahoo.json") if use_cache else {}
    todo = [c for c in companies if not (use_cache and c["ticker"] in cache)]
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(enrich_one, c["ticker"], cache, use_cache): c
                   for c in todo}
        for fut in as_completed(futures):
            c = futures[fut]
            try:
                info = fut.result()
            except Exception:
                info = {"yk_ok": False}
            c.update(info)
            cache[c["ticker"]] = info
            done += 1
            if done % 25 == 0 or done == len(todo):
                log(f"      enriched {done}/{len(todo)}")
    # apply cache to the rest
    for c in companies:
        if c["ticker"] in cache and "yahoo_ok" not in c:
            c.update(cache[c["ticker"]])
    if use_cache:
        _save_cache("yahoo.json", cache)


# --------------------------------------------------------------------------- #
# Step 3: IR RSS feed discovery
# --------------------------------------------------------------------------- #
def normalize_domain(website: str | None) -> str | None:
    if not website:
        return None
    if not website.startswith(("http://", "https://")):
        website = "https://" + website
    host = urlparse(website).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def candidate_ir_bases(website: str | None) -> list[str]:
    domain = normalize_domain(website)
    if not domain:
        return []
    bases: list[str] = []
    root = domain.split(":", 1)[0]
    for sub in IR_SUBDOMAINS:
        bases.append(f"https://{sub}.{root}")
    for path in IR_PATHS:
        bases.append(f"https://www.{root}{path}")
        bases.append(f"https://{root}{path}")
    # de-dup preserving order
    seen, out = set(), []
    for b in bases:
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def extract_feed_links(html: str, base_url: str) -> list[str]:
    out: list[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("link", href=True):
        rel = link.get("rel") or []
        rel = [r.lower() for r in (rel if isinstance(rel, list) else [rel])]
        typ = (link.get("type") or "").lower()
        href = link["href"].lower()
        if ("rss" in typ or "atom" in typ or "rss" in href or "feed" in href) \
                and "alternate" in rel:
            out.append(urljoin(base_url, link["href"]))
    for a in soup.find_all("a", href=True):
        href = a["href"]
        low = href.lower()
        text = (a.get_text() or "").lower()
        if re.search(r"(rss|\.xml|/feed|feed\.)", low) and \
                ("rss" in low or "feed" in low or "rss" in text or "feed" in text):
            out.append(urljoin(base_url, href))
    # de-dup
    seen, dedup = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


def validate_feed(url: str) -> dict:
    """Return {ok, entries, title, type} for a candidate feed URL."""
    r = http_get(url, timeout=REQUEST_TIMEOUT, retries=1)
    if r is None or r.status_code != 200:
        return {"ok": False}
    ctype = (r.headers.get("Content-Type") or "").lower()
    head = r.content[:400].lstrip().lower()
    looks_xml = (b"<rss" in head or b"<feed" in head or b"<?xml" in head)
    if not ("xml" in ctype or "rss" in ctype or "atom" in ctype or looks_xml):
        return {"ok": False}
    try:
        parsed = feedparser.parse(r.content)
    except Exception:
        return {"ok": False}
    entries = len(parsed.entries)
    title = (parsed.feed.get("title") if parsed.feed else None)
    if entries > 0 or (parsed.feed and parsed.feed.get("title")):
        return {"ok": True, "entries": entries, "title": title,
                "type": parsed.version or "rss"}
    return {"ok": False}


IR_URL_RE = re.compile(
    r"(^|\.)(investor|investors|ir)\."
    r"|/(investor|investors|ir)(-?relations)?(/|$)",
    re.I,
)
IR_TEXT_RE = re.compile(r"investor relations|shareholder|investors", re.I)
NEWS_LINK_RE = re.compile(
    r"press[-_ ]?releas|financial|quarterly|results|news|events", re.I
)


def looks_like_ir(final_url: str, html: str) -> bool:
    """Heuristic: does this page actually look like an IR page?"""
    if IR_URL_RE.search(final_url):
        return True
    return bool(IR_TEXT_RE.search(html[:20000]))


def find_news_pages(html: str, base_url: str, limit: int = 3) -> list[str]:
    """Internal links likely to hold press releases / financial news."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    host = urlparse(base_url).netloc
    found, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        if not NEWS_LINK_RE.search(href + " " + text):
            continue
        absu = urljoin(base_url, href)
        p = urlparse(absu)
        if p.scheme not in ("http", "https") or p.netloc != host:
            continue
        if absu.rstrip("/") == base_url.rstrip("/") or absu in seen:
            continue
        seen.add(absu)
        found.append(absu)
        if len(found) >= limit:
            break
    return found


def discover_feed(company: dict) -> dict:
    result = {"ir_page": None, "feed_url": None, "feed_title": None,
              "feed_entries": None, "feed_status": "none"}
    website = company.get("yahoo_website")
    bases = candidate_ir_bases(website)
    if not bases:
        result["feed_status"] = "no_website"
        return result

    ir_page = ir_base = ir_html = None
    fallback = None  # (page, base, html) first reachable, even if generic
    blocked = 0
    attempted = 0
    for base in bases[:8]:
        attempted += 1
        r = http_get(base, timeout=10, retries=0)
        if r is None:
            continue
        if r.status_code == 403:
            blocked += 1
            if blocked >= 3 and not fallback:
                break
            continue
        if r.status_code != 200:
            continue
        ctype = (r.headers.get("Content-Type") or "html").lower()
        if "html" not in ctype:
            continue
        if fallback is None:
            fallback = (r.url, base, r.text)
        if looks_like_ir(r.url, r.text) or looks_like_ir(base, ""):
            ir_page, ir_base, ir_html = r.url, base, r.text
            break

    if ir_page is None and fallback is not None:
        ir_page, ir_base, ir_html = fallback

    if ir_page is None:
        result["feed_status"] = "blocked" if blocked else "no_ir_page"
        return result

    result["ir_page"] = ir_page

    # 1) feed links directly on the IR landing page
    candidates = extract_feed_links(ir_html, ir_page)

    # 2) feed links on likely press-release / news subpages
    for page in find_news_pages(ir_html, ir_page):
        sub = http_get(page, timeout=10, retries=0)
        if sub is not None and sub.status_code == 200 and "html" in (
            sub.headers.get("Content-Type") or "html"
        ).lower():
            candidates.extend(extract_feed_links(sub.text, sub.url))

    # 3) common IR feed paths on the IR base
    for path in COMMON_FEED_PATHS:
        candidates.append(urljoin(ir_base + "/", path.lstrip("/")))

    # de-dup preserving priority order
    seen, ordered = set(), []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            ordered.append(u)

    for url in ordered[:MAX_FEED_CANDIDATES]:
        v = validate_feed(url)
        if v.get("ok"):
            result["feed_url"] = url
            result["feed_title"] = v.get("title")
            result["feed_entries"] = v.get("entries")
            result["feed_status"] = "verified"
            return result

    result["feed_status"] = "not_found"
    return result


def discover_feeds(companies: list[dict], workers: int) -> None:
    log(f"[3/3] Discovering IR RSS feeds for {len(companies)} companies "
        f"({workers} workers) ...")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(discover_feed, c): c for c in companies}
        for fut in as_completed(futures):
            c = futures[fut]
            try:
                c.update(fut.result())
            except Exception:
                c.update({"feed_status": "error"})
            done += 1
            if done % 25 == 0 or done == len(companies):
                log(f"      discovered {done}/{len(companies)}")


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
COLUMNS = [
    "ticker", "name", "sector", "yahoo_name", "yahoo_sector",
    "yahoo_industry", "yahoo_website", "ir_page", "feed_status",
    "feed_url", "feed_title", "feed_entries",
]


def write_outputs(companies: list[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(companies)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[COLUMNS]
    csv_path = os.path.join(OUTPUT_DIR, "sp500_ir_feeds.csv")
    json_path = os.path.join(OUTPUT_DIR, "sp500_ir_feeds.json")
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)
    log(f"\nWrote {csv_path}")
    log(f"Wrote {json_path}")

    if "feed_status" in df:
        counts = df["feed_status"].value_counts(dropna=False).to_dict()
        log("\nFeed status summary:")
        for status, n in counts.items():
            log(f"  {status:<12} {n}")


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, help="only process the first N tickers")
    ap.add_argument("--tickers", help="comma-separated tickers to process")
    ap.add_argument("--workers", type=int, default=8, help="concurrent workers")
    ap.add_argument("--no-cache", action="store_true", help="ignore yahoo cache")
    ap.add_argument("--no-feed-discovery", action="store_true",
                    help="skip IR RSS discovery")
    args = ap.parse_args()

    companies = fetch_sp500()

    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers.split(",") if t.strip()}
        companies = [c for c in companies if c["ticker"].upper() in wanted]
    if args.limit:
        companies = companies[: args.limit]

    enrich_yahoo(companies, args.workers, use_cache=not args.no_cache)

    if not args.no_feed_discovery:
        discover_feeds(companies, args.workers)
    else:
        for c in companies:
            c.setdefault("feed_status", "skipped")

    write_outputs(companies)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\nInterrupted")
        sys.exit(130)
