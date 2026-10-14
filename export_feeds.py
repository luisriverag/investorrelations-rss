#!/usr/bin/env python3
"""Export the ticker -> feed mapping from the IR database to CSV.

Writes one row per company with a column per feed kind (verified feeds only).
Used as a small, diff-friendly artifact that is committed to the repo.

    python export_feeds.py                 # -> output/ticker_feeds.csv
    python export_feeds.py --out foo.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from ir import db  # noqa: E402

# Feed kinds become columns; order defines CSV column order.
FEED_KINDS = ["ir_rss", "edgar_atom", "google_news"]


def export(out_path: str) -> tuple[str, int]:
    con = db.connect()
    db.init(con)

    kinds = sorted({r["kind"] for r in con.execute("SELECT DISTINCT kind FROM feeds")}
                   | set(FEED_KINDS))
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


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=os.path.join(HERE, "output", "ticker_feeds.csv"))
    args = ap.parse_args()
    path, n = export(args.out)
    print(f"wrote {n} rows -> {path}")


if __name__ == "__main__":
    main()
