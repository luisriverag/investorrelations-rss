"""
Pipeline orchestrator: builds the SQLite IR dataset from all sources.

Stages:
    yahoo       - sector/industry/website via yfinance (cached)
    ir_rss      - IR website landing page + native RSS feed
    edgar       - filings, earnings 8-K press-release full text, Atom feed
    news        - Google News + Bing News + Yahoo Finance per-company RSS
    wires       - GlobeNewswire / PR Newswire / Business Wire press releases
    financials  - XBRL quarterly revenue / net income / EPS

Usage:
    python -m ir.collect --universe sp500
    python -m ir.collect --universe all --limit 500 --workers 6
    python -m ir.collect --tickers AAPL,MSFT --stages edgar,news --force
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import db, sources, universe

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAHOO_CACHE = os.path.join(sources.CACHE_DIR, "yahoo.json")
SP500_CSV = os.path.join(HERE, "output", "sp500_ir_feeds.csv")
SP500_SOURCES = os.path.join(HERE, "output", "sp500_ir_sources.json")

ALL_STAGES = ["yahoo", "ir_rss", "edgar", "news", "wires", "financials"]
DB_LOCK = threading.Lock()


def _pool(todo, work, workers, label, every=50):
    """Run `work` over `todo` concurrently, surfacing (not swallowing) errors."""
    done = errs = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, c) for c in todo]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as e:
                errs += 1
                if errs <= 5:
                    sources.log(f"    {label} ERROR {type(e).__name__}: {e}")
            done += 1
            if done % every == 0:
                sources.log(f"    {label} {done}/{len(todo)}")
    if errs:
        sources.log(f"    {label} errors: {errs}")


def _load_yahoo_cache() -> dict:
    try:
        with open(YAHOO_CACHE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_yahoo_cache(cache: dict) -> None:
    tmp = YAHOO_CACHE + ".tmp"
    os.makedirs(os.path.dirname(YAHOO_CACHE), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)
    os.replace(tmp, YAHOO_CACHE)


def yahoo_info(ticker: str, cache: dict) -> dict:
    if ticker in cache:
        return cache[ticker]
    import yfinance as yf
    out = {"yahoo_name": None, "yahoo_sector": None, "yahoo_industry": None,
           "yahoo_website": None, "yahoo_ok": False}
    try:
        info = yf.Ticker(ticker).info or {}
        out["yahoo_name"] = info.get("longName") or info.get("shortName")
        out["yahoo_sector"] = info.get("sector")
        out["yahoo_industry"] = info.get("industry")
        out["yahoo_website"] = info.get("website")
        out["yahoo_ok"] = bool(out["yahoo_name"] or out["yahoo_website"])
    except Exception:
        pass
    return out


# --------------------------------------------------------------------------- #
# Seed from the earlier S&P 500 runs (fast path, no network)
# --------------------------------------------------------------------------- #
def seed_from_prior(con):
    import csv
    if not os.path.exists(SP500_CSV):
        return 0
    n = 0
    with open(SP500_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            t = str(row["ticker"]).strip().upper().replace(".", "-")
            db.upsert_company(con, t, name=row.get("yahoo_name") or row.get("name"),
                              sector=row.get("yahoo_sector") or row.get("sector"),
                              industry=row.get("yahoo_industry"),
                              ir_website=row.get("yahoo_website"),
                              ir_page=row.get("ir_page"), in_sp500=1)
            if row.get("feed_status") == "verified" and row.get("feed_url"):
                db.upsert_feed(con, t, "ir_rss", row["feed_url"],
                               row.get("feed_title"),
                               int(float(row["feed_entries"])) if row.get("feed_entries") else 0,
                               verified=1)
                with DB_LOCK:
                    db.set_sync(con, t, "ir_rss", "ok", "seed")
            n += 1
    con.commit()
    return n


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #
def stage_yahoo(con, comps, workers, force):
    cache = _load_yahoo_cache()
    todo = [c for c in comps
            if force or db.sync_status(con, c["ticker"], "yahoo") != "ok"]
    sources.log(f"  [yahoo] {len(todo)} companies")
    done = 0

    def work(c):
        info = yahoo_info(c["ticker"], cache)
        with DB_LOCK:
            db.upsert_company(con, c["ticker"], name=c.get("name") or info.get("yahoo_name"),
                              sector=info.get("yahoo_sector") or c.get("sector"),
                              industry=info.get("yahoo_industry"),
                              ir_website=info.get("yahoo_website"),
                              cik=c.get("cik"), in_sp500=c.get("in_sp500", 0))
            db.set_sync(con, c["ticker"], "yahoo",
                        "ok" if info.get("yahoo_ok") else "empty")
            con.commit()
        cache[c["ticker"]] = info
        return info.get("yahoo_ok")

    _pool(todo, work, workers, "yahoo")
    _save_yahoo_cache(cache)

def stage_ir_rss(con, comps, workers, force):
    todo = [c for c in comps if c.get("ir_website") and
            (force or db.sync_status(con, c["ticker"], "ir_rss") != "ok")]
    sources.log(f"  [ir_rss] {len(todo)} companies")
    done = 0

    def work(c):
        res = sources.discover_ir(c["ir_website"])
        with DB_LOCK:
            if res["ir_page"]:
                db.upsert_company(con, c["ticker"], ir_page=res["ir_page"])
            if res["feed_url"]:
                db.upsert_feed(con, c["ticker"], "ir_rss", res["feed_url"],
                               res["feed_title"], res["feed_entries"], verified=1)
            if res.get("press_sitemap"):
                db.upsert_feed(con, c["ticker"], "press_sitemap",
                               res["press_sitemap"], "press sitemap", None,
                               verified=1)
            db.set_sync(con, c["ticker"], "ir_rss",
                        "ok" if (res["feed_url"] or res.get("press_sitemap"))
                        else "empty")
            con.commit()
        return bool(res["feed_url"])

    _pool(todo, work, workers, "ir_rss")

def stage_edgar(con, comps, workers, force, earnings_text=True):
    todo = [c for c in comps if c.get("cik") and
            (force or db.sync_status(con, c["ticker"], "edgar") != "ok")]
    sources.log(f"  [edgar] {len(todo)} companies (earnings_text={earnings_text})")
    done = 0

    def work(c):
        filings, meta = sources.build_filings(c["cik"])
        if filings is None:
            with DB_LOCK:
                db.set_sync(con, c["ticker"], "edgar", "error")
                con.commit()
            return False
        prs = []
        if earnings_text and meta and meta.get("earnings_acc"):
            ex = sources.find_exhibit(c["cik"], meta["earnings_acc"])
            if ex:
                text = sources.fetch_text(ex)
                if text:
                    prs.append({"source": "edgar_8k", "title":
                                f"Earnings release {meta['earnings_acc']}",
                                "url": ex, "published": None, "text": text})
        with DB_LOCK:
            db.insert_filings(con, c["ticker"], filings)
            if prs:
                db.insert_press_releases(con, c["ticker"], prs)
            if meta and meta.get("company"):
                db.upsert_company(con, c["ticker"], name=c.get("name"),
                                  cik=c["cik"])
            db.upsert_feed(con, c["ticker"], "edgar_atom",
                           f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={c['cik']}&type=8-K&output=atom",
                           entries=None, verified=1)
            db.set_sync(con, c["ticker"], "edgar", "ok")
            con.commit()
        return True

    _pool(todo, work, workers, "edgar", every=25)


def stage_news(con, comps, workers, force):
    todo = [c for c in comps
            if force or db.sync_status(con, c["ticker"], "news") != "ok"]
    sources.log(f"  [news] {len(todo)} companies")
    done = 0

    def work(c):
        name = c.get("name") or c["ticker"]
        ticker = c["ticker"]
        gurl, git = sources.google_news(name)
        burl, bit = sources.bing_news(name)
        yurl, yit = sources.yahoo_finance_rss(ticker)
        all_items = git + bit + yit
        with DB_LOCK:
            if all_items:
                db.insert_press_releases(con, ticker, all_items)
            db.upsert_feed(con, ticker, "google_news", gurl,
                           entries=len(git), verified=1 if git else 0)
            db.upsert_feed(con, ticker, "bing_news", burl,
                           entries=len(bit), verified=1 if bit else 0)
            db.upsert_feed(con, ticker, "yahoo_news", yurl,
                           entries=len(yit), verified=1 if yit else 0)
            db.set_sync(con, ticker, "news",
                        "ok" if all_items else "empty")
            con.commit()
        return bool(all_items)

    _pool(todo, work, workers, "news")


def stage_wires(con, comps, workers, force):
    todo = [c for c in comps
            if force or db.sync_status(con, c["ticker"], "wires") != "ok"]
    sources.log(f"  [wires] {len(todo)} companies")
    done = 0

    def work(c):
        name = c.get("name") or c["ticker"]
        items = sources.wire_news(name)
        with DB_LOCK:
            if items:
                db.insert_press_releases(con, c["ticker"], items)
            db.set_sync(con, c["ticker"], "wires",
                        "ok" if items else "empty")
            con.commit()
        return bool(items)

    _pool(todo, work, workers, "wires")


def stage_financials(con, comps, workers, force):
    todo = [c for c in comps if c.get("cik") and
            (force or db.sync_status(con, c["ticker"], "financials") != "ok")]
    sources.log(f"  [financials] {len(todo)} companies")
    done = 0

    def work(c):
        metrics = sources.xbrl_metrics(c["cik"])
        with DB_LOCK:
            if metrics:
                db.insert_metrics(con, c["ticker"], metrics)
            db.set_sync(con, c["ticker"], "financials",
                        "ok" if metrics else "empty")
            con.commit()
        return bool(metrics)

    _pool(todo, work, workers, "financials", every=25)


STAGE_FUNCS = {
    "yahoo": stage_yahoo,
    "ir_rss": stage_ir_rss,
    "edgar": stage_edgar,
    "news": stage_news,
    "wires": stage_wires,
    "financials": stage_financials,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--universe", choices=["sp500", "all"], default="sp500")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--tickers")
    ap.add_argument("--stages", default=",".join(ALL_STAGES))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--no-earnings-text", action="store_true")
    ap.add_argument("--include-etfs", action="store_true")
    ap.add_argument("--no-export", action="store_true",
                    help="skip refreshing output/ticker_feeds.csv at the end")
    args = ap.parse_args()

    con = db.connect()
    db.init(con)

    sources.log("[universe] building ticker list ...")
    comps = universe.build_universe(include_etfs=args.include_etfs)
    for c in comps:
        db.upsert_company(con, c["ticker"], name=c.get("name"),
                          exchange=c.get("exchange"), cik=c.get("cik"),
                          sector=c.get("sector"), country=c.get("country"),
                          in_sp500=c.get("in_sp500", 0))
    con.commit()
    sources.log(f"           {len(comps)} tickers")

    if args.universe == "sp500":
        comps = [c for c in comps if c.get("in_sp500")]
    if args.tickers:
        want = {t.strip().upper() for t in args.tickers.split(",")}
        comps = [c for c in comps if c["ticker"] in want]
    if args.limit:
        comps = comps[: args.limit]

    if args.universe == "sp500":
        n = seed_from_prior(con)
        sources.log(f"[seed] loaded {n} prior S&P 500 rows")

    # `comps` is the base universe slice; profile fields (ir_website, name,
    # cik, ...) are refreshed from the DB before each stage so later stages
    # (e.g. ir_rss after yahoo) see values written by earlier stages.
    def merged_comps():
        con.row_factory = db.sqlite3.Row
        dbmap = {r["ticker"]: dict(r) for r in
                 con.execute("SELECT * FROM companies").fetchall()}
        return [{**c, **dbmap.get(c["ticker"], {})} for c in comps]

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    for stage in stages:
        if stage not in STAGE_FUNCS:
            sources.log(f"unknown stage: {stage}")
            continue
        sources.log(f"[stage] {stage}")
        fn = STAGE_FUNCS[stage]
        cs = merged_comps()
        if stage == "edgar":
            fn(con, cs, args.workers, args.force,
               not args.no_earnings_text)
        else:
            fn(con, cs, args.workers, args.force)

    if not args.no_export:
        from .export import export_feeds
        path, n = export_feeds()
        sources.log(f"[export] {n} rows -> {os.path.relpath(path, HERE)}")

    sources.log("[done]")


if __name__ == "__main__":
    main()
