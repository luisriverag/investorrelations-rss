#!/usr/bin/env bash
# Collector + web app helper.
#
#   ./run.sh collect-sp500     Seed the S&P 500 (fast, good demo dataset)
#   ./run.sh collect-all       Collect the full US-listed SEC universe (~10k)
#   ./run.sh refine-sp500      Re-run ir_rss + wires for the S&P 500
#   ./run.sh web               Start the Flask app on :5000
#   ./run.sh feeds             Export output/ticker_feeds.csv (tickers + feeds)
#   ./run.sh stats             Print dataset counts
set -euo pipefail
cd "$(dirname "$0")"
PY="../.venv/bin/python"

case "${1:-web}" in
  collect-sp500)
    $PY -m ir.collect --universe sp500 --workers 6 ;;
  collect-all)
    $PY -m ir.collect --universe all --workers 6 ;;
  refine-sp500)
    $PY -m ir.collect --universe sp500 --stages ir_rss,wires --workers 6 --force ;;
  web)
    exec $PY ir_app.py ;;
  feeds)
    $PY export_feeds.py ;;
  stats)
    $PY - <<'PY'
from ir import db
con = db.connect()
for t in ("companies","filings","press_releases","metrics","feeds"):
    print(t, con.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"])
print("press by source:", [dict(r) for r in con.execute(
    "SELECT source,COUNT(*) c FROM press_releases GROUP BY source ORDER BY c DESC")])
PY
    ;;
  *) echo "usage: $0 {collect-sp500|collect-all|refine-sp500|web|feeds|stats}"; exit 1 ;;
esac
