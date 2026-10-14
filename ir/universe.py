"""Build the ticker universe: Nasdaq Trader symbol directory + S&P 500 + SEC CIKs."""
from __future__ import annotations

import io
import os
import re
import time

import requests
import pandas as pd

from . import sources

NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ_CACHE = os.path.join(sources.CACHE_DIR, "nasdaq")

BROWSER = {"User-Agent": sources.BROWSER_UA}


def _fetch_symbol_file(url: str, cache_name: str, header: str) -> str:
    """Fetch a Nasdaq Trader symbol file, falling back to a disk cache."""
    os.makedirs(NASDAQ_CACHE, exist_ok=True)
    path = os.path.join(NASDAQ_CACHE, cache_name)
    for attempt in range(3):
        try:
            r = requests.get(url, headers=BROWSER, timeout=30)
        except requests.RequestException:
            r = None
        if (r is not None and r.status_code == 200 and len(r.text) > 5000
                and r.text.startswith(header)):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(r.text)
            return r.text
        time.sleep(2.0 * (attempt + 1))
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    return ""


def _yahoo_symbol(sym: str) -> str:
    return sym.strip().upper().replace(".", "-").replace("^", "")


def load_sec_tickers() -> dict:
    """ticker -> {cik, name} from SEC (all SEC registrants with a ticker)."""
    r = sources.sec_get(SEC_TICKERS)
    if r is None or r.status_code != 200:
        return {}
    out = {}
    for v in r.json().values():
        t = _yahoo_symbol(str(v.get("ticker", "")))
        if t:
            out[t] = {"cik": f"{int(v['cik_str']):010d}",
                      "name": str(v.get("title", "")).strip()}
    return out


def load_sp500() -> dict:
    r = requests.get(WIKI_SP500, headers=BROWSER, timeout=30)
    df = pd.read_html(io.StringIO(r.text))[0]
    out = {}
    for _, row in df.iterrows():
        t = _yahoo_symbol(str(row["Symbol"]))
        cik = f"{int(row['CIK']):010d}" if pd.notna(row.get("CIK")) and \
            str(row["CIK"]).strip().isdigit() else None
        out[t] = {"ticker": t, "name": str(row["Security"]).strip(),
                  "sector": str(row["GICS Sector"]).strip(), "cik": cik,
                  "in_sp500": 1}
    return out


def load_nasdaq_listed() -> list:
    text = _fetch_symbol_file(NASDAQ_LISTED, "nasdaqlisted.txt", "Symbol|")
    lines = [ln for ln in text.splitlines() if "|" in ln]
    if lines and lines[0].startswith("Symbol|"):
        lines = lines[1:]
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) < 3 or p[0] == "File Creation Time" or not p[0].strip():
            continue
        if (p[3] if len(p) > 3 else "N") == "Y":   # test issue
            continue
        is_etf = (p[6] if len(p) > 6 else "N") == "Y"
        out.append({"ticker": _yahoo_symbol(p[0]), "name": p[1].strip(),
                    "exchange": "Nasdaq", "is_etf": is_etf})
    return out


EXCHANGES = {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca",
             "Z": "Cboe", "V": "IEX", "M": "NYSE Chicago"}


def load_other_listed() -> list:
    text = _fetch_symbol_file(OTHER_LISTED, "otherlisted.txt", "ACT Symbol|")
    lines = [ln for ln in text.splitlines() if "|" in ln]
    if lines and lines[0].startswith("ACT Symbol|"):
        lines = lines[1:]
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) < 3 or p[0] == "File Creation Time" or not p[0].strip():
            continue
        if (p[4] if len(p) > 4 else "N") == "Y":   # test issue
            continue
        is_etf = (p[6] if len(p) > 6 else "N") == "Y"
        out.append({"ticker": _yahoo_symbol(p[0]), "name": p[1].strip(),
                    "exchange": EXCHANGES.get(p[2], p[2]) if len(p) > 2 else None,
                    "is_etf": is_etf})
    return out


def build_universe(include_etfs: bool = False) -> list:
    """
    Universe = all SEC-registered tickers (backbone, with CIK), overlaid with
    S&P 500 metadata and, when reachable, Nasdaq/NYSE listing info.
    """
    sec = load_sec_tickers()
    sp = load_sp500()
    try:
        listings = load_nasdaq_listed() + load_other_listed()
    except Exception:
        listings = []

    exchange_by_ticker = {}
    for item in listings:
        if item["is_etf"] and not include_etfs:
            continue
        t = item["ticker"]
        if not t or not re.match(r"^[A-Z0-9\-]+$", t):
            continue
        exchange_by_ticker.setdefault(t, item["exchange"])

    by_ticker: dict = {}

    def ensure(t):
        return by_ticker.setdefault(t, {
            "ticker": t, "name": None, "exchange": None, "cik": None,
            "sector": None, "industry": None, "ir_website": None, "ir_page": None,
            "country": None, "in_sp500": 0})

    for t, meta in sec.items():
        if not re.match(r"^[A-Z0-9\-]+$", t):
            continue
        c = ensure(t)
        c["name"] = c["name"] or meta["name"]
        c["cik"] = c["cik"] or meta["cik"]
        c["exchange"] = c["exchange"] or exchange_by_ticker.get(t)

    for t, meta in sp.items():
        c = ensure(t)
        c.update(name=meta["name"], sector=meta["sector"], cik=meta["cik"],
                 in_sp500=1, exchange=c["exchange"] or exchange_by_ticker.get(t))

    for t, c in by_ticker.items():
        if not c["cik"] and t in sec:
            c["cik"] = sec[t]["cik"]
        if not c["name"]:
            c["name"] = t

    return sorted(by_ticker.values(), key=lambda c: c["ticker"])
