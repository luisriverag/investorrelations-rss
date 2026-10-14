#!/usr/bin/env python3
"""Export the ticker -> feed mapping from the IR database to CSV.

Writes one row per company with a column per feed kind (verified feeds only).
Used as a small, diff-friendly artifact that is committed to the repo.

    python export_feeds.py                 # -> output/ticker_feeds.csv
    python export_feeds.py --out foo.csv
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from ir.export import DEFAULT_OUT, export_feeds  # noqa: E402


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()
    path, n = export_feeds(args.out)
    print(f"wrote {n} rows -> {path}")


if __name__ == "__main__":
    main()
