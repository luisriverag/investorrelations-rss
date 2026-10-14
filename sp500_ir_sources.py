#!/usr/bin/env python3
"""
Gather comprehensive Investor Relations information for every S&P 500 company
from multiple authoritative sources (not just the company IR website RSS feed).

Why: only ~19% of S&P 500 companies expose an IR RSS feed, and many block
automated clients. This script layers *universal* sources on top:

  1. SEC EDGAR  - filings for 100% of constituents (10-K, 10-Q, 8-K earnings
                  releases, proxy, etc.) plus a per-company Atom feed.
  2. Google News RSS - a company-specific press/news feed for 100%.
  3. Company IR website + RSS - from the prior sp500_ir_feeds run.

Outputs (in ./output):
    sp500_ir_sources.csv
    sp500_ir_sources.json
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
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import feedparser

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(HERE, ".cache")
EDGAR_CACHE = os.path.join(CACHE_DIR, "edgar")
OUTPUT_DIR = os.path.join(HERE, "output")
PRIOR_CSV = os.path.join(OUTPUT_DIR, "sp500_ir_feeds.csv")

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
GOOGLE_NEWS = ("https://news.google.com/rss/search?q={q}"
               "&hl=en-US&gl=US&ceid=US:en")

# SEC requires a descriptive User-Agent with contact info.
SEC_UA = "SP500-IR-Research/1.0 (research; contact: ir-research@example.com)"
BROWSER_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def session(ua: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua, "Accept-Encoding": "gzip, deflate"})
    return s


SEC = session(SEC_UA)

# SEC fair-access: stay well under 10 req/s and back off on 429.
SEC_MIN_INTERVAL = 0.25          # ~4 requests/second
_sec_lock = threading.Lock()
_sec_last = [0.0]


def _sec_throttle() -> None:
    with _sec_lock:
        wait = SEC_MIN_INTERVAL - (time.monotonic() - _sec_last[0])
        if wait > 0:
            time.sleep(wait)
        _sec_last[0] = time.monotonic()


def sec_get(url: str, timeout: int = 30, retries: int = 5):
    """GET with SEC-specific throttling and 429 backoff."""
    r = None
    for attempt in range(retries + 1):
        _sec_throttle()
        try:
            r = SEC.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            r = None
        if r is not None and r.status_code == 429:
            ra = r.headers.get("Retry-After")
            delay = float(ra) if (ra and ra.replace(".", "").isdigit()) \
                else min(60.0, 3.0 * (attempt + 1))
            log(f"      429 from SEC ({url.split('/')[2]}), backing off {delay:.0f}s")
            time.sleep(delay)
            continue
        if r is not None:
            return r
        time.sleep(1.0 * (attempt + 1))
    return r


def get(url: str, sess: requests.Session, timeout: int = 30, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            return sess.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
    return None


# --------------------------------------------------------------------------- #
# Base list
# --------------------------------------------------------------------------- #
def fetch_sp500() -> list[dict]:
    log("[1/4] Fetching S&P 500 constituents + CIKs from Wikipedia ...")
    r = get(WIKI_URL, SEC)
    df = pd.read_html(io.StringIO(r.text))[0]
    df = df[["Symbol", "Security", "GICS Sector", "CIK"]].copy()
    df.columns = ["ticker", "name", "sector", "cik"]
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["cik"] = df["cik"].apply(
        lambda v: f"{int(v):010d}" if pd.notna(v) and str(v).strip().isdigit() else None
    )
    rows = df.to_dict("records")
    log(f"      {len(rows)} constituents, {sum(1 for r in rows if r['cik'])} with CIK")
    return rows


# --------------------------------------------------------------------------- #
# Source 1: SEC EDGAR
# --------------------------------------------------------------------------- #
def edgar_submissions(cik: str) -> dict | None:
    os.makedirs(EDGAR_CACHE, exist_ok=True)
    cache_path = os.path.join(EDGAR_CACHE, f"{cik}.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass
    r = sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json", timeout=30)
    if r is None or r.status_code != 200:
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    with open(cache_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    return data


def _filing_url(cik: str, accession: str, primary: str) -> str:
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{primary}")


def _exhibit_score(name: str) -> int:
    """Heuristic score: how likely is this file the earnings press release?"""
    low = name.lower()
    score = 0
    if "exhibit" in low or re.search(r"ex[-_]?x?9", low):
        score += 10
    if "99" in low:
        score += 5
    if "release" in low or "press" in low:
        score += 4
    if "earnings" in low or "results" in low:
        score += 2
    if re.search(r"slide|presentation|supplement|transcript|webcast|math", low):
        score -= 6
    return score


def find_press_release_exhibit(cik: str, accession: str) -> str | None:
    """Locate the EX-99 press-release exhibit inside an 8-K filing."""
    acc = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.json"
    r = sec_get(url, timeout=20, retries=3)
    if r is None or r.status_code != 200:
        return None
    try:
        items = r.json().get("directory", {}).get("item", [])
    except ValueError:
        return None
    best, best_score = None, 0
    for it in items:
        name = (it.get("name") or "")
        low = name.lower()
        if not low.endswith((".htm", ".html", ".txt")):
            continue
        if "index" in low or "xbrl" in low or re.fullmatch(r"r\d+\.htm", low):
            continue
        s = _exhibit_score(name)
        if s > best_score:
            best, best_score = name, s
    if best:
        return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                f"{acc}/{best}")
    return None


def parse_edgar(cik: str, with_exhibit: bool = True) -> dict:
    os.makedirs(EDGAR_CACHE, exist_ok=True)
    suffix = "" if with_exhibit else "_noex"
    parsed_path = os.path.join(EDGAR_CACHE, f"{cik}_parsed{suffix}.json")
    if os.path.exists(parsed_path):
        try:
            with open(parsed_path, "r", encoding="utf-8") as fh:
                cached = json.load(fh)
            incomplete = (
                with_exhibit
                and cached.get("earnings_release_url")
                and not cached.get("earnings_press_release_url")
            )
            if not incomplete:
                return cached
        except (json.JSONDecodeError, OSError):
            pass
    out = _parse_edgar_uncached(cik, with_exhibit)
    if out.get("edgar_ok"):
        tmp = parsed_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(out, fh)
        os.replace(tmp, parsed_path)
    return out


def _parse_edgar_uncached(cik: str, with_exhibit: bool = True) -> dict:
    out = {"cik": cik, "edgar_ok": False}
    data = edgar_submissions(cik)
    if not data:
        return out
    out["edgar_ok"] = True
    out["edgar_company_url"] = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&"
        f"CIK={cik}&type=&dateb=&owner=include&count=40")
    out["edgar_atom_8k_url"] = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&"
        f"CIK={cik}&type=8-K&output=atom")
    out["edgar_json_url"] = f"https://data.sec.gov/submissions/CIK{cik}.json"

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accs = recent.get("accessionNumber", [])
    docs = recent.get("primaryDocument", [])
    items = recent.get("items", [""] * len(forms))

    latest = {"10-K": None, "10-Q": None, "8-K": None, "DEF 14A": None}
    latest_earnings = None
    counts = {"10-K": 0, "10-Q": 0, "8-K": 0}
    cutoff = time.time() - 365 * 86400

    for i, form in enumerate(forms):
        acc = accs[i]
        doc = docs[i]
        url = _filing_url(cik, acc, doc)
        if form in latest and latest[form] is None:
            latest[form] = (dates[i], url)
        if form == "8-K" and latest_earnings is None and "2.02" in (items[i] or ""):
            latest_earnings = (dates[i], url, acc)
        try:
            d = time.mktime(time.strptime(dates[i], "%Y-%m-%d"))
            if d >= cutoff and form in counts:
                counts[form] += 1
        except (ValueError, TypeError):
            pass

    if latest["10-K"]:
        out["latest_10k_date"], out["latest_10k_url"] = latest["10-K"]
    if latest["10-Q"]:
        out["latest_10q_date"], out["latest_10q_url"] = latest["10-Q"]
    if latest["8-K"]:
        out["latest_8k_date"], out["latest_8k_url"] = latest["8-K"]
    if latest["DEF 14A"]:
        out["latest_proxy_date"], out["latest_proxy_url"] = latest["DEF 14A"]
    out["filings_1y_10k"] = counts["10-K"]
    out["filings_1y_10q"] = counts["10-Q"]
    out["filings_1y_8k"] = counts["8-K"]

    if latest_earnings:
        out["earnings_release_date"] = latest_earnings[0]
        out["earnings_release_url"] = latest_earnings[1]
        if with_exhibit:
            ex = find_press_release_exhibit(cik, latest_earnings[2])
            if ex:
                out["earnings_press_release_url"] = ex
    return out


# --------------------------------------------------------------------------- #
# Source 2: Google News RSS
# --------------------------------------------------------------------------- #
def google_news(company_name: str) -> dict:
    q = f'"{company_name}" (press release OR earnings OR dividend OR investor)'
    url = GOOGLE_NEWS.format(q=urllib.parse.quote(q))
    r = get(url, session(BROWSER_UA), timeout=25, retries=1)
    if r is None or r.status_code != 200:
        return {"gnews_rss": url, "gnews_entries": 0, "gnews_ok": False}
    parsed = feedparser.parse(r.content)
    n = len(parsed.entries)
    return {"gnews_rss": url, "gnews_entries": n, "gnews_ok": n > 0,
            "gnews_latest": parsed.entries[0].title if parsed.entries else None}


# --------------------------------------------------------------------------- #
# Merge with prior IR-RSS run
# --------------------------------------------------------------------------- #
def load_prior() -> dict:
    if not os.path.exists(PRIOR_CSV):
        return {}
    df = pd.read_csv(PRIOR_CSV)
    df = df.where(pd.notna(df), None)
    return {r["ticker"]: r for r in df.to_dict("records")}


# --------------------------------------------------------------------------- #
def run(companies: list[dict], workers: int, with_exhibit: bool = True) -> None:
    log(f"[2/4] Pulling SEC EDGAR filings for {len(companies)} companies ...")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(parse_edgar, c["cik"], with_exhibit): c
                for c in companies if c["cik"]}
        for f in as_completed(futs):
            c = futs[f]
            try:
                c.update(f.result())
            except Exception:
                c.update({"edgar_ok": False})
            done += 1
            if done % 50 == 0 or done == len(futs):
                log(f"      EDGAR {done}/{len(futs)}")

    log(f"[3/4] Building Google News RSS for {len(companies)} companies ...")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(google_news, c["name"]): c for c in companies}
        for f in as_completed(futs):
            c = futs[f]
            try:
                c.update(f.result())
            except Exception:
                c.update({"gnews_ok": False, "gnews_entries": 0})
            done += 1
            if done % 50 == 0 or done == len(futs):
                log(f"      GNews {done}/{len(futs)}")

    log("[4/4] Merging prior IR-website results and writing output ...")
    prior = load_prior()
    for c in companies:
        p = prior.get(c["ticker"], {})
        c["ir_website"] = p.get("yahoo_website")
        c["ir_page"] = p.get("ir_page")
        c["ir_rss"] = p.get("feed_url") if p.get("feed_status") == "verified" else None

    cols = [
        "ticker", "name", "sector", "cik",
        "ir_website", "ir_page", "ir_rss",
        "edgar_company_url", "edgar_atom_8k_url", "edgar_json_url",
        "latest_10k_date", "latest_10k_url",
        "latest_10q_date", "latest_10q_url",
        "latest_8k_date", "latest_8k_url",
        "earnings_release_date", "earnings_release_url", "earnings_press_release_url",
        "latest_proxy_date", "latest_proxy_url",
        "filings_1y_10k", "filings_1y_10q", "filings_1y_8k",
        "gnews_rss", "gnews_entries", "gnews_latest",
    ]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(companies)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    df = df[cols]
    df.to_csv(os.path.join(OUTPUT_DIR, "sp500_ir_sources.csv"), index=False)
    df.to_json(os.path.join(OUTPUT_DIR, "sp500_ir_sources.json"),
               orient="records", indent=2)

    log("\nCoverage:")
    log(f"  EDGAR filings        {df['edgar_json_url'].notna().sum():>4}/{len(df)}")
    log(f"  Google News RSS      {(df['gnews_entries'] > 0).sum():>4}/{len(df)}")
    log(f"  IR website RSS       {df['ir_rss'].notna().sum():>4}/{len(df)}")
    log(f"  earnings 8-K found   {df['earnings_release_url'].notna().sum():>4}/{len(df)}")
    log(f"  press-release exhibit{df['earnings_press_release_url'].notna().sum():>5}/{len(df)}")
    log(f"\nWrote {os.path.join(OUTPUT_DIR, 'sp500_ir_sources.csv')}")
    log(f"Wrote {os.path.join(OUTPUT_DIR, 'sp500_ir_sources.json')}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--tickers")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--no-exhibit", action="store_true",
                    help="skip fetching 8-K press-release exhibits")
    args = ap.parse_args()

    companies = fetch_sp500()
    if args.tickers:
        want = {t.strip().upper() for t in args.tickers.split(",")}
        companies = [c for c in companies if c["ticker"].upper() in want]
    if args.limit:
        companies = companies[: args.limit]

    if args.no_exhibit:
        global find_press_release_exhibit
        find_press_release_exhibit = lambda *a, **k: None

    run(companies, args.workers, with_exhibit=not args.no_exhibit)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\nInterrupted")
        sys.exit(130)
