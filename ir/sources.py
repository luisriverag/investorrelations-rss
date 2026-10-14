"""
Source collectors: SEC EDGAR, XBRL financials, Google News, wire services
(Business Wire / PR Newswire / GlobeNewswire via curl_cffi), and IR-site RSS.
"""
from __future__ import annotations

import json
import os
import re
import time
import hashlib
import threading
import datetime
import warnings
import urllib.parse
from urllib.parse import urljoin, urlparse

import requests
import feedparser
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from curl_cffi import requests as cq

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(HERE, ".cache")
EDGAR_CACHE = os.path.join(CACHE_DIR, "edgar")
WIRE_CACHE = os.path.join(CACHE_DIR, "wires")
IR_CACHE = os.path.join(CACHE_DIR, "ir")

SEC_UA = "SP500-IR-Research/1.0 (research; contact: ir-research@example.com)"
BROWSER_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

SEC = requests.Session()
SEC.headers.update({"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"})

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
SEC_MIN_INTERVAL = 0.25
_sec_lock = threading.Lock()
_sec_last = [0.0]


def _sec_throttle():
    with _sec_lock:
        wait = SEC_MIN_INTERVAL - (time.monotonic() - _sec_last[0])
        if wait > 0:
            time.sleep(wait)
        _sec_last[0] = time.monotonic()


def sec_get(url, timeout=30, retries=5):
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
            log(f"      429 SEC {url.split('/')[2]}, backoff {delay:.0f}s")
            time.sleep(delay)
            continue
        if r is not None:
            return r
        time.sleep(1.0 * (attempt + 1))
    return r


def cf_get(url, timeout=30, retries=2, impersonate="chrome"):
    """curl_cffi GET with browser TLS impersonation (for wires/news sites)."""
    r = None
    for attempt in range(retries + 1):
        try:
            r = cq.get(url, impersonate=impersonate, timeout=timeout,
                       allow_redirects=True)
        except Exception:
            r = None
        if r is not None and r.status_code < 400:
            return r
        if attempt < retries:
            time.sleep(0.8 * (attempt + 1))
    return r


def plain_get(url, timeout=25, retries=1):
    for attempt in range(retries + 1):
        try:
            return requests.get(url, headers={"User-Agent": BROWSER_UA},
                                timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            if attempt < retries:
                time.sleep(0.5)
    return None


def http_text(url, timeout=15):
    """GET a URL, falling back to curl_cffi TLS impersonation on block (403/None).

    Many IR sites sit behind Cloudflare and return 403 to plain requests but
    serve fine to a browser-impersonating client.
    """
    r = plain_get(url, timeout=timeout)
    if r is not None and r.status_code < 400:
        return r
    rc = cf_get(url, timeout=timeout, retries=0)
    if rc is not None and rc.status_code < 400:
        return rc
    return r if r is not None else rc


# --------------------------------------------------------------------------- #
# SEC EDGAR
# --------------------------------------------------------------------------- #
def edgar_submissions(cik):
    os.makedirs(EDGAR_CACHE, exist_ok=True)
    path = os.path.join(EDGAR_CACHE, f"{cik}.json")
    cached = _read_json(path)
    if cached is not None:
        return cached
    r = sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if r is None or r.status_code != 200:
        return None
    return r.json()


def _filing_url(cik, accession, doc):
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{doc}")


FORMS_OF_INTEREST = {"10-K", "10-Q", "8-K", "DEF 14A", "20-F", "6-K", "S-1",
                     "424B4", "DEFA14A", "SC 13D", "SC 13G"}


def build_filings(cik, max_filings=120):
    data = edgar_submissions(cik)
    if not data:
        return None, None
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    reports = recent.get("reportDate", [])
    accs = recent.get("accessionNumber", [])
    docs = recent.get("primaryDocument", [])
    items = recent.get("items", [""] * len(forms))

    filings = []
    latest = {}
    earnings_acc = None
    for i, form in enumerate(forms):
        if form not in FORMS_OF_INTEREST:
            continue
        acc = accs[i]
        url = _filing_url(cik, acc, docs[i]) if docs[i] else \
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace('-','')}/"
        is_earn = 1 if (form == "8-K" and "2.02" in (items[i] or "")) else 0
        filings.append({
            "form": form, "filing_date": dates[i], "report_date": reports[i],
            "accession": acc, "url": url, "items": items[i] or "",
            "is_earnings": is_earn,
        })
        if form not in latest:
            latest[form] = {"date": dates[i], "url": url, "accession": acc}
        if is_earn and earnings_acc is None:
            earnings_acc = acc
        if len(filings) >= max_filings:
            break
    return filings, {"latest": latest, "earnings_acc": earnings_acc,
                     "company": data.get("name"),
                     "sic": data.get("sicDescription")}


def _exhibit_score(name):
    low = name.lower()
    s = 0
    if "exhibit" in low or re.search(r"ex[-_]?x?9", low):
        s += 10
    if "99" in low:
        s += 5
    if "release" in low or "press" in low:
        s += 4
    if "earnings" in low or "results" in low:
        s += 2
    if re.search(r"slide|presentation|supplement|transcript|webcast", low):
        s -= 6
    return s


def find_exhibit(cik, accession):
    acc = accession.replace("-", "")
    r = sec_get(f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.json",
                timeout=20, retries=3)
    if r is None or r.status_code != 200:
        return None
    try:
        items = r.json().get("directory", {}).get("item", [])
    except ValueError:
        return None
    best, best_s = None, 0
    for it in items:
        name = it.get("name") or ""
        low = name.lower()
        if not low.endswith((".htm", ".html", ".txt")):
            continue
        if "index" in low or "xbrl" in low or re.fullmatch(r"r\d+\.htm", low):
            continue
        s = _exhibit_score(name)
        if s > best_s:
            best, best_s = name, s
    if best:
        return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{best}")
    return None


def fetch_text(url, max_chars=120000):
    """Download a filing document and return cleaned plain text."""
    r = sec_get(url, timeout=30, retries=2)
    if r is None or r.status_code != 200:
        return None
    try:
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        soup = BeautifulSoup(r.text, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()
    text = " ".join(soup.get_text(" ").split())
    return text[:max_chars]


# --------------------------------------------------------------------------- #
# XBRL financials
# --------------------------------------------------------------------------- #
US_REVENUE_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
]
US_METRIC_TAGS = {
    "revenue": US_REVENUE_TAGS,
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
}

# IFRS taxonomy for foreign private issuers (20-F / 40-F / 6-K filers).
IFRS_METRIC_TAGS = {
    "revenue": ["Revenue", "RevenueFromContractsWithCustomers",
                "RevenueFromSaleOfGoods", "RevenueFromRenderingOfServices"],
    "net_income": ["ProfitLoss", "ProfitLossAttributableToOwnersOfParent"],
    "eps_diluted": ["DilutedEarningsLossPerShare",
                    "DilutedEarningsLossPerShareFromContinuingOperations"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["ProfitLossFromOperatingActivities",
                         "OperatingProfitLoss"],
}

US_FORMS = ("10-Q", "10-K")
IFRS_FORMS = ("20-F", "40-F", "6-K")


def _pick_unit(units, metric):
    """Choose the unit key: per-share for EPS, otherwise a currency (USD first)."""
    keys = list(units)
    if not keys:
        return None
    if metric == "eps_diluted":
        for k in keys:
            if "shares" in k:
                return k
    if "USD" in units:
        return "USD"
    for k in keys:
        if len(k) == 3 and k.isalpha():
            return k
    return keys[0]


def _to_date(s):
    try:
        return datetime.date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _extract_metrics(facts, metric_tags, forms, quarters, allow_longer):
    out = []
    for metric, tags in metric_tags.items():
        picked = next((t for t in tags if t in facts), None)
        if not picked:
            continue
        units = facts[picked].get("units", {})
        unit_key = _pick_unit(units, metric)
        if not unit_key:
            continue
        series = units.get(unit_key, [])
        quarterly, longer = [], []
        for row in series:
            form = row.get("form")
            if form not in forms:
                continue
            start, end = _to_date(row.get("start")), _to_date(row.get("end"))
            if not end:
                continue
            dur = (end - start).days if start else None
            rec = {
                "period_end": end.isoformat(), "fy": row.get("fy"),
                "fp": row.get("fp"), "form": form, "value": row.get("val"),
                "unit": unit_key, "filed": row.get("filed"),
                "accession": row.get("accn"),
            }
            if dur is None or dur <= 100:          # true quarterly period
                quarterly.append(rec)
            elif allow_longer and dur <= 380:       # annual / semi-annual
                longer.append(rec)
        rows = quarterly or longer
        rows.sort(key=lambda x: (x["period_end"], x["form"] in ("10-Q", "6-K")),
                  reverse=True)
        seen = set()
        for row in rows:
            if row["period_end"] in seen:
                continue
            seen.add(row["period_end"])
            row["metric"] = metric
            out.append(row)
            if len(seen) >= quarters:
                break
    return out


def xbrl_metrics(cik, quarters=8):
    """Return the most recent values for key metrics.

    Tries the US-GAAP taxonomy first; if it yields nothing, falls back to the
    IFRS taxonomy (``ifrs-full``) used by foreign private issuers
    (20-F/40-F/6-K). For IFRS, annual/semi-annual periods are accepted when no
    quarterly period exists.
    """
    r = sec_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                timeout=60, retries=2)
    if r is None or r.status_code != 200:
        return []
    try:
        allfacts = r.json().get("facts", {})
    except ValueError:
        return []
    ug = allfacts.get("us-gaap", {})
    ifrs = allfacts.get("ifrs-full", {})

    us = _extract_metrics(ug, US_METRIC_TAGS, US_FORMS, quarters,
                          allow_longer=False)
    if us:
        return us
    return _extract_metrics(ifrs, IFRS_METRIC_TAGS, IFRS_FORMS, quarters,
                            allow_longer=True)


# --------------------------------------------------------------------------- #
# News / wires
# --------------------------------------------------------------------------- #
GOOGLE_NEWS = ("https://news.google.com/rss/search?q={q}"
               "&hl=en-US&gl=US&ceid=US:en")


def google_news(name, limit=40):
    q = f'"{name}" (press release OR earnings OR dividend OR investor)'
    url = GOOGLE_NEWS.format(q=urllib.parse.quote(q))
    r = plain_get(url)
    if r is None or r.status_code != 200:
        return url, []
    parsed = feedparser.parse(r.content)
    return url, [{
        "source": "google_news", "title": e.get("title"),
        "url": e.get("link"), "published": e.get("published"),
        "text": None,
    } for e in parsed.entries[:limit]]


YAHOO_NEWS = "https://finance.yahoo.com/rss/headline?s={t}"
BING_NEWS = "https://www.bing.com/news/search?q={q}&format=RSS"


def _rss_items(url, source, limit=40, use_cf=False):
    r = cf_get(url) if use_cf else plain_get(url)
    if r is None or r.status_code != 200:
        return []
    parsed = feedparser.parse(r.content)
    return [{
        "source": source, "title": e.get("title"), "url": e.get("link"),
        "published": e.get("published"), "text": None,
    } for e in parsed.entries[:limit]]


def yahoo_finance_rss(ticker, limit=20):
    """Yahoo Finance per-symbol headline RSS (works for the whole universe)."""
    url = YAHOO_NEWS.format(t=urllib.parse.quote(ticker))
    return url, _rss_items(url, "yahoo_news", limit, use_cf=True)


def bing_news(name, limit=20):
    """Bing News per-company RSS (independent secondary news source)."""
    url = BING_NEWS.format(q=urllib.parse.quote(f'"{name}"'))
    return url, _rss_items(url, "bing_news", limit, use_cf=True)


def _cf_page(url, cache=True):
    if cache:
        key = hashlib.md5(url.encode()).hexdigest()
        path = os.path.join(WIRE_CACHE, key + ".html")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except OSError:
            pass
    else:
        path = None
    r = cf_get(url)
    if r is None or r.status_code >= 400:
        return None
    if path:
        os.makedirs(WIRE_CACHE, exist_ok=True)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(r.text)
        except OSError:
            pass
    return r.text


def globe_newswire(name, limit=10):
    """Per-company press releases via GlobeNewswire keyword search."""
    url = "https://www.globenewswire.com/en/search/keyword/" + \
        urllib.parse.quote(name)
    html = _cf_page(url)
    if not html:
        return url, []
    soup = BeautifulSoup(html, "lxml")
    items, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news-release/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        full = urljoin("https://www.globenewswire.com", href)
        if full in seen:
            continue
        seen.add(full)
        items.append({"source": "globenewswire", "title": title, "url": full,
                      "published": None, "text": None})
        if len(items) >= limit:
            break
    return url, items


def pr_newswire(name, limit=10):
    """Per-company press releases via PR Newswire search."""
    url = "https://www.prnewswire.com/search/news/?keyword=" + \
        urllib.parse.quote(name)
    html = _cf_page(url)
    if not html:
        return url, []
    soup = BeautifulSoup(html, "lxml")
    items, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "prnewswire.com/news-releases/" not in href and \
           not href.startswith("/news-releases/"):
            continue
        title = a.get_text(" ", strip=True)
        if len(title) < 15:
            continue
        full = urljoin("https://www.prnewswire.com", href)
        if full in seen:
            continue
        seen.add(full)
        items.append({"source": "prnewswire", "title": title, "url": full,
                      "published": None, "text": None})
        if len(items) >= limit:
            break
    return url, items


def businesswire(name, limit=10):
    """Business Wire blocks automated access; retained for completeness."""
    url = "https://www.businesswire.com/portal/site/home/search/?searchTerm=" + \
        urllib.parse.quote(name)
    html = _cf_page(url)
    if not html:
        return url, []
    soup = BeautifulSoup(html, "lxml")
    items, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news/home/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        full = urljoin("https://www.businesswire.com", href)
        if not title or full in seen:
            continue
        seen.add(full)
        items.append({"source": "businesswire", "title": title, "url": full,
                      "published": None, "text": None})
        if len(items) >= limit:
            break
    return url, items


def wire_news(name):
    """Aggregate wire-service press releases across providers.

    Business Wire is omitted: it hard-blocks automated access (HTTP 403) and
    only wastes a request + retries per company.
    """
    items = []
    for fn in (globe_newswire, pr_newswire):
        try:
            _, got = fn(name)
            items.extend(got)
        except Exception:
            pass
    return items


# --------------------------------------------------------------------------- #
# IR website RSS discovery
# --------------------------------------------------------------------------- #
IR_SUBDOMAINS = ["investor", "investors", "ir"]
IR_PATHS = ["/investors", "/investor-relations", "/investor", "/ir",
            "/about/investors", "/about-us/investors", "/company/investors",
            "/corporate/investors", "/en/investors", "/en/investor-relations",
            "/investors-relations", "/investor-relations/overview",
            "/news-events", "/press-releases", "/media/press-releases"]
COMMON_FEED_PATHS = [
    "/rss", "/feed", "/feed.xml", "/rss.xml", "/atom.xml", "/index.xml",
    "/rss/news", "/rss/press-releases", "/rss/pressreleases",
    "/rss/news-releases", "/news/rss", "/news/feed", "/news/rss.xml",
    "/news/feed.xml", "/press-releases/rss", "/press-releases/feed",
    "/press-releases/rss.xml", "/press-releases/feed.xml",
    "/press/press-releases/rss", "/news-events/press-releases/rss",
    "/news-events/press-releases/rss.xml", "/media/press-releases/rss",
    "/en/rss", "/en/news/rss", "/blog/feed", "/feed/rss", "/?feed=rss2",
    # Q4 Inc IR platform + Nasdaq IR / common CMS endpoints
    "/feed/PressRelease.aspx", "/rss/pressrelease.aspx",
    "/rss/PressRelease.aspx", "/feed/press-releases", "/feed/default.aspx",
    "/rss/News.aspx", "/rss/news.aspx",
]
SITEMAP_PATHS = ["/sitemap.xml", "/sitemap_index.xml", "/news-sitemap.xml",
                 "/sitemap/news.xml", "/press-releases/sitemap.xml"]
NEWS_HINT = re.compile(r"news|press|release|media|announce", re.I)
PRESS_SUB_RE = re.compile(r"press|news|media|releases", re.I)
IR_URL_RE = re.compile(r"(^|\.)(investor|investors|ir)\.|"
                       r"/(investor|investors|ir)(-?relations)?(/|$)", re.I)


def _domain(website):
    if not website:
        return None
    if not website.startswith(("http://", "https://")):
        website = "https://" + website
    host = urlparse(website).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def _extract_feeds(html, base):
    out, seen = [], set()
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("link", href=True):
        rel = link.get("rel") or []
        rel = [r.lower() for r in (rel if isinstance(rel, list) else [rel])]
        typ = (link.get("type") or "").lower()
        href = link["href"]
        if "alternate" in rel and ("rss" in typ or "atom" in typ or
                                   "rss" in href.lower() or "feed" in href.lower()):
            u = urljoin(base, href)
            if u not in seen:
                seen.add(u); out.append(u)
    for a in soup.find_all("a", href=True):
        low = a["href"].lower()
        if ("rss" in low or "feed" in low) and low.endswith(
                (".xml", "/rss", "/feed", "rss/", "feed/")):
            u = urljoin(base, a["href"])
            if u not in seen:
                seen.add(u); out.append(u)
    return out


def _validate_feed(url):
    r = http_text(url, timeout=15)
    if r is None or r.status_code != 200:
        return None
    head = r.content[:400].lstrip().lower()
    if not (b"<rss" in head or b"<feed" in head or b"<?xml" in head):
        return None
    try:
        f = feedparser.parse(r.content)
    except Exception:
        return None
    if f.entries or (f.feed and f.feed.get("title")):
        return {"title": (f.feed.get("title") if f.feed else None),
                "entries": len(f.entries)}
    return None


def _press_sublinks(html, base):
    """Absolute links on a page whose text/href looks like news/press."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(("mailto:", "javascript:", "#")):
            continue
        if PRESS_SUB_RE.search((a.get_text(" ", strip=True) or "") + " " + href):
            u = urljoin(base, href)
            if u.startswith("http") and u not in seen:
                seen.add(u)
                out.append(u)
    return out


def _find_press_sitemap(base):
    """Return a sitemap URL that appears to list news/press pages, else None."""
    u = urlparse(base)
    root = f"{u.scheme}://{u.netloc}"
    for p in SITEMAP_PATHS:
        url = root + p
        r = http_text(url, timeout=10)
        if r is None or r.status_code != 200:
            continue
        text = r.text
        head = text[:1000]
        if "<sitemapindex" in head:
            for loc in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", text):
                if NEWS_HINT.search(loc):
                    return loc
            continue
        if "<urlset" in head or "<url " in head:
            if NEWS_HINT.search(p) or NEWS_HINT.search(text[:10000]):
                return url
    return None


def discover_ir(website, deep=True, budget=15.0, use_cache=True):
    """Return {ir_page, feed_url, feed_title, feed_entries, press_sitemap}.

    Results are cached per-domain in ``.cache/ir/`` so re-runs are instant.
    A wall-clock ``budget`` (seconds) bounds the work per company.
    """
    dom = _domain(website)
    if not dom:
        return {"ir_page": None, "feed_url": None, "feed_title": None,
                "feed_entries": None, "press_sitemap": None}
    cache_path = os.path.join(IR_CACHE, dom.replace(":", "_") + ".json")
    if use_cache:
        cached = _read_json(cache_path)
        if cached is not None:
            return cached
    res = _discover_ir_uncached(website, dom, deep, budget)
    if use_cache:
        _write_json(cache_path, res)
    return res


def _discover_ir_uncached(website, dom, deep, budget):
    out = {"ir_page": None, "feed_url": None, "feed_title": None,
           "feed_entries": None, "press_sitemap": None}
    deadline = time.time() + budget
    root = dom.split(":")[0]
    bases = [f"https://{s}.{root}" for s in IR_SUBDOMAINS] + \
            [f"https://www.{root}{p}" for p in IR_PATHS] + \
            [f"https://{root}{p}" for p in IR_PATHS]
    page_html = None
    for base in bases[:6]:
        if time.time() > deadline:
            break
        r = http_text(base, timeout=8)
        if r is None or r.status_code != 200:
            continue
        ctype = (r.headers.get("Content-Type") or "html").lower()
        if "html" not in ctype:
            continue
        page_html, out["ir_page"] = r.text, r.url
        if IR_URL_RE.search(r.url):
            break
    if not out["ir_page"]:
        return out

    cands = _extract_feeds(page_html, out["ir_page"])
    # Follow press/news subpages that may advertise their own feed.
    if deep and page_html and time.time() < deadline:
        for sub in _press_sublinks(page_html, out["ir_page"])[:2]:
            if time.time() > deadline:
                break
            r = http_text(sub, timeout=8)
            if r is None or r.status_code != 200:
                continue
            cands.extend(_extract_feeds(r.text, r.url))
    for p in COMMON_FEED_PATHS:
        cands.append(out["ir_page"].rstrip("/") + p)

    seen = set()
    for u in cands:
        if u in seen or time.time() > deadline:
            continue
        seen.add(u)
        v = _validate_feed(u)
        if v:
            out.update(feed_url=u, feed_title=v["title"],
                       feed_entries=v["entries"])
            break
    # No native feed: fall back to a press/news sitemap (still enumerates
    # press releases, and is consumable by most feed tooling).
    if not out["feed_url"] and deep and time.time() < deadline:
        out["press_sitemap"] = _find_press_sitemap(out["ir_page"])
    return out
