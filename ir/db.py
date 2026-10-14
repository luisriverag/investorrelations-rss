"""SQLite storage for the IR dataset."""
from __future__ import annotations

import os
import sqlite3
import datetime

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(HERE, "output", "ir.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies(
    ticker       TEXT PRIMARY KEY,
    name         TEXT,
    exchange     TEXT,
    cik          TEXT,
    sector       TEXT,
    industry     TEXT,
    ir_website   TEXT,
    ir_page      TEXT,
    country      TEXT,
    in_sp500     INTEGER DEFAULT 0,
    updated_at   TEXT
);
CREATE TABLE IF NOT EXISTS filings(
    ticker       TEXT,
    form         TEXT,
    filing_date  TEXT,
    report_date  TEXT,
    accession    TEXT,
    url          TEXT,
    items        TEXT,
    is_earnings  INTEGER DEFAULT 0,
    PRIMARY KEY(ticker, accession)
);
CREATE TABLE IF NOT EXISTS press_releases(
    ticker       TEXT,
    source       TEXT,
    title        TEXT,
    url          TEXT,
    published    TEXT,
    text         TEXT,
    PRIMARY KEY(ticker, source, url)
);
CREATE TABLE IF NOT EXISTS metrics(
    ticker       TEXT,
    metric       TEXT,
    period_end   TEXT,
    fy           INTEGER,
    fp           TEXT,
    form         TEXT,
    value        REAL,
    unit         TEXT,
    filed        TEXT,
    accession    TEXT,
    PRIMARY KEY(ticker, metric, period_end, form)
);
CREATE TABLE IF NOT EXISTS feeds(
    ticker       TEXT,
    kind         TEXT,
    url          TEXT,
    title        TEXT,
    entries      INTEGER,
    verified     INTEGER,
    PRIMARY KEY(ticker, kind)
);
CREATE TABLE IF NOT EXISTS sync(
    ticker       TEXT,
    stage        TEXT,
    status       TEXT,
    detail       TEXT,
    updated_at   TEXT,
    PRIMARY KEY(ticker, stage)
);
CREATE INDEX IF NOT EXISTS idx_filings_ticker ON filings(ticker);
CREATE INDEX IF NOT EXISTS idx_pr_ticker ON press_releases(ticker);
CREATE INDEX IF NOT EXISTS idx_metrics_ticker ON metrics(ticker);
"""


def connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def init(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def _now() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="seconds")


def upsert_company(con, ticker, **fields) -> None:
    fields["ticker"] = ticker
    fields["updated_at"] = _now()
    cols = list(fields.keys())
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols if c != "ticker")
    con.execute(
        f"INSERT INTO companies({','.join(cols)}) VALUES({placeholders}) "
        f"ON CONFLICT(ticker) DO UPDATE SET {updates}",
        [fields[c] for c in cols],
    )


def set_sync(con, ticker, stage, status, detail="") -> None:
    con.execute(
        "INSERT INTO sync(ticker,stage,status,detail,updated_at) "
        "VALUES(?,?,?,?,?) ON CONFLICT(ticker,stage) DO UPDATE SET "
        "status=excluded.status,detail=excluded.detail,updated_at=excluded.updated_at",
        (ticker, stage, status, detail[:500], _now()),
    )


def sync_status(con, ticker, stage) -> str | None:
    cur = con.execute("SELECT status FROM sync WHERE ticker=? AND stage=?",
                      (ticker, stage))
    row = cur.fetchone()
    return row["status"] if row else None


def insert_filings(con, ticker, filings: list[dict]) -> None:
    con.executemany(
        "INSERT INTO filings(ticker,form,filing_date,report_date,accession,url,"
        "items,is_earnings) VALUES(:ticker,:form,:filing_date,:report_date,"
        ":accession,:url,:items,:is_earnings) ON CONFLICT(ticker,accession) DO "
        "UPDATE SET form=excluded.form, filing_date=excluded.filing_date, "
        "url=excluded.url, items=excluded.items, is_earnings=excluded.is_earnings",
        [{**f, "ticker": ticker} for f in filings],
    )


def insert_press_releases(con, ticker, items: list[dict]) -> None:
    con.executemany(
        "INSERT INTO press_releases(ticker,source,title,url,published,text) "
        "VALUES(:ticker,:source,:title,:url,:published,:text) "
        "ON CONFLICT(ticker,source,url) DO UPDATE SET title=excluded.title, "
        "published=excluded.published, text=COALESCE(excluded.text, press_releases.text)",
        [{**{"text": None}, **it, "ticker": ticker} for it in items],
    )


def insert_metrics(con, ticker, items: list[dict]) -> None:
    con.executemany(
        "INSERT INTO metrics(ticker,metric,period_end,fy,fp,form,value,unit,filed,accession) "
        "VALUES(:ticker,:metric,:period_end,:fy,:fp,:form,:value,:unit,:filed,:accession) "
        "ON CONFLICT(ticker,metric,period_end,form) DO UPDATE SET value=excluded.value, "
        "filed=excluded.filed, accession=excluded.accession",
        [{**it, "ticker": ticker} for it in items],
    )


def upsert_feed(con, ticker, kind, url, title=None, entries=None, verified=0) -> None:
    con.execute(
        "INSERT INTO feeds(ticker,kind,url,title,entries,verified) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(ticker,kind) DO UPDATE SET url=excluded.url, "
        "title=excluded.title, entries=excluded.entries, verified=excluded.verified",
        (ticker, kind, url, title, entries, verified),
    )


def commit(con) -> None:
    con.commit()
