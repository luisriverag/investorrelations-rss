"""Export the ticker -> feed mapping from the IR database to CSV."""
from __future__ import annotations

import csv
import os

from . import db

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT = os.path.join(HERE, "output", "ticker_feeds.csv")

# Feed kinds that should always appear as columns (even if empty everywhere),
# in this order. Any other kinds found in the DB are appended automatically.
FEED_KINDS = ["ir_rss", "edgar_atom", "google_news", "bing_news",
              "yahoo_news", "press_sitemap"]


def export_feeds(out_path: str = DEFAULT_OUT) -> tuple[str, int]:
    """Write one row per company with a column per verified feed kind.

    Returns (path, row_count).
    """
    con = db.connect()
    db.init(con)

    kinds = FEED_KINDS + sorted(
        {r["kind"] for r in con.execute("SELECT DISTINCT kind FROM feeds")}
        - set(FEED_KINDS))

    feeds: dict[str, dict[str, str]] = {}
    for r in con.execute(
        "SELECT ticker, kind, url, verified FROM feeds WHERE url IS NOT NULL "
        "AND url != '' ORDER BY ticker, kind"
    ):
        if r["verified"] == 0:
            continue
        feeds.setdefault(r["ticker"], {})[r["kind"]] = r["url"]

    rows = con.execute(
        "SELECT ticker, name, exchange, in_sp500 FROM companies "
        "ORDER BY in_sp500 DESC, ticker"
    ).fetchall()

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    n = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "name", "exchange", "in_sp500", *kinds])
        for r in rows:
            fk = feeds.get(r["ticker"], {})
            w.writerow([r["ticker"], r["name"], r["exchange"] or "",
                        int(r["in_sp500"] or 0), *[fk.get(k, "") for k in kinds]])
            n += 1
    con.close()
    return out_path, n
