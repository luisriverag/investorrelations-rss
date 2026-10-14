"""Flask web app to browse the S&P 500 / US-listed IR dataset."""
from __future__ import annotations

import os
import math

from flask import Flask, render_template, request, jsonify, abort

from ir import db

app = Flask(__name__)
PAGE_SIZE = 50


def get_db():
    con = db.connect()
    db.init(con)
    return con


def fmt_num(v):
    if v is None:
        return ""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= div:
            return f"{v/div:.2f}{unit}"
    return f"{v:,.2f}"


app.jinja_env.filters["num"] = fmt_num


@app.route("/")
def index():
    q = (request.args.get("q") or "").strip()
    exchange = (request.args.get("exchange") or "").strip() or None
    only_sp500 = request.args.get("sp500") == "1"
    only_rss = request.args.get("rss") == "1"
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    con = get_db()

    where, params = [], {}
    if q:
        where.append("(c.ticker LIKE :like OR c.name LIKE :like)")
        params["like"] = f"%{q}%"
    if exchange:
        where.append("c.exchange = :ex")
        params["ex"] = exchange
    if only_sp500:
        where.append("c.in_sp500 = 1")
    if only_rss:
        where.append("EXISTS(SELECT 1 FROM feeds f WHERE f.ticker=c.ticker "
                     "AND f.kind='ir_rss' AND f.verified=1)")
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    total = con.execute(f"SELECT COUNT(*) c FROM companies c {clause}",
                        params).fetchone()["c"]
    params["limit"] = PAGE_SIZE
    params["offset"] = (page - 1) * PAGE_SIZE
    rows = con.execute(f"""
        SELECT c.*,
          (SELECT COUNT(*) FROM press_releases p WHERE p.ticker=c.ticker) AS pr_count,
          (SELECT COUNT(*) FROM filings fi WHERE fi.ticker=c.ticker) AS filing_count,
          (SELECT url FROM feeds f WHERE f.ticker=c.ticker AND f.kind='ir_rss'
             AND f.verified=1 LIMIT 1) AS ir_rss
        FROM companies c {clause}
        ORDER BY c.in_sp500 DESC, c.ticker
        LIMIT :limit OFFSET :offset""", params).fetchall()

    exchanges = [r["exchange"] for r in con.execute(
        "SELECT DISTINCT exchange FROM companies WHERE exchange IS NOT NULL "
        "ORDER BY exchange")]
    con.close()

    pages = max(1, math.ceil(total / PAGE_SIZE))
    return render_template("index.html", rows=rows, total=total, page=page,
                           pages=pages, q=q, exchange=exchange or "",
                           only_sp500=only_sp500, only_rss=only_rss,
                           exchanges=exchanges)


@app.route("/company/<ticker>")
def company(ticker):
    con = get_db()
    c = con.execute("SELECT * FROM companies WHERE ticker=?",
                    (ticker.upper(),)).fetchone()
    if not c:
        con.close()
        abort(404)

    feeds = con.execute("SELECT * FROM feeds WHERE ticker=? ORDER BY kind",
                        (ticker.upper(),)).fetchall()

    filings = con.execute(
        "SELECT * FROM filings WHERE ticker=? ORDER BY filing_date DESC LIMIT 40",
        (ticker.upper(),)).fetchall()

    prs = con.execute(
        "SELECT * FROM press_releases WHERE ticker=? "
        "ORDER BY published IS NULL, published DESC LIMIT 300",
        (ticker.upper(),)).fetchall()
    by_source = {}
    for p in prs:
        by_source.setdefault(p["source"], []).append(p)

    metrics_rows = con.execute(
        "SELECT * FROM metrics WHERE ticker=? ORDER BY period_end DESC",
        (ticker.upper(),)).fetchall()
    metrics = {}
    periods = []
    for m in metrics_rows:
        metrics.setdefault(m["metric"], {})[m["period_end"]] = m["value"]
        if m["period_end"] not in periods:
            periods.append(m["period_end"])
    periods = periods[:8]
    con.close()

    return render_template("company.html", c=c, feeds=feeds, filings=filings,
                           by_source=by_source, metrics=metrics,
                           periods=periods)


@app.route("/stats")
def stats():
    con = get_db()
    s = {
        "companies": con.execute("SELECT COUNT(*) c FROM companies").fetchone()["c"],
        "sp500": con.execute("SELECT COUNT(*) c FROM companies WHERE in_sp500=1").fetchone()["c"],
        "companies_with_filings": con.execute("SELECT COUNT(DISTINCT ticker) c FROM filings").fetchone()["c"],
        "press_releases": con.execute("SELECT COUNT(*) c FROM press_releases").fetchone()["c"],
        "earnings_texts": con.execute("SELECT COUNT(*) c FROM press_releases WHERE source='edgar_8k'").fetchone()["c"],
        "metric_rows": con.execute("SELECT COUNT(*) c FROM metrics").fetchone()["c"],
        "ir_rss": con.execute("SELECT COUNT(*) c FROM feeds WHERE kind='ir_rss' AND verified=1").fetchone()["c"],
        "google_news": con.execute("SELECT COUNT(*) c FROM feeds WHERE kind='google_news' AND verified=1").fetchone()["c"],
    }
    sources = [dict(r) for r in con.execute(
        "SELECT source, COUNT(*) c FROM press_releases GROUP BY source ORDER BY c DESC")]
    con.close()
    return render_template("stats.html", s=s, sources=sources)


@app.route("/api/companies")
def api_companies():
    q = (request.args.get("q") or "").strip()
    con = get_db()
    if q:
        rows = con.execute(
            "SELECT ticker,name,exchange,sector,cik FROM companies "
            "WHERE ticker LIKE ? OR name LIKE ? ORDER BY ticker LIMIT 100",
            (f"%{q}%", f"%{q}%")).fetchall()
    else:
        rows = con.execute(
            "SELECT ticker,name,exchange,sector,cik FROM companies "
            "ORDER BY in_sp500 DESC, ticker LIMIT 100").fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/company/<ticker>")
def api_company(ticker):
    con = get_db()
    t = ticker.upper()
    c = con.execute("SELECT * FROM companies WHERE ticker=?", (t,)).fetchone()
    if not c:
        con.close()
        return jsonify({"error": "not found"}), 404
    data = {
        "company": dict(c),
        "feeds": [dict(r) for r in con.execute("SELECT * FROM feeds WHERE ticker=?", (t,))],
        "filings": [dict(r) for r in con.execute(
            "SELECT * FROM filings WHERE ticker=? ORDER BY filing_date DESC LIMIT 40", (t,))],
        "press_releases": [dict(r) for r in con.execute(
            "SELECT ticker,source,title,url,published FROM press_releases "
            "WHERE ticker=? ORDER BY published DESC LIMIT 100", (t,))],
        "metrics": [dict(r) for r in con.execute(
            "SELECT * FROM metrics WHERE ticker=? ORDER BY period_end DESC", (t,))],
    }
    con.close()
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
