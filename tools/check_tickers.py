#!/usr/bin/env python3
"""
Validate ticker_master.json against live market data.

Run this on demand -- NOT in the weekly job. It price-checks every entry and
reports three things:

  DARK      live entries with no recent price -- either a bad symbol or a
            corporate action nobody has recorded yet
  STALE     live entries whose last trade is more than --max-age days old,
            the usual signature of a delisting
  OK        priced fine

`assumed` entries (inferred as plain US listings) are called out separately,
since those are the ones most likely to be wrong.

Anything reported DARK or STALE needs a human decision: fix the `yf` symbol in
tools/gen_ticker_master.py OVERRIDES, or record the corporate action in
CORPORATE_ACTIONS, then regenerate.

    python3 tools/check_tickers.py                 # covered names only
    python3 tools/check_tickers.py --all           # every entry
    python3 tools/check_tickers.py --max-age 10
"""
import argparse, json, logging, pathlib, sys, warnings
from datetime import date

warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)

REPO = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(REPO))
import yfinance as yf
import stale_alerts as sa


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age", type=int, default=7,
                    help="days since last trade before a name is called STALE")
    ap.add_argument("--all", action="store_true",
                    help="check every entry, not just analyst-covered names")
    args = ap.parse_args()

    doc = json.loads((REPO / "ticker_master.json").read_text())
    analyst_map = json.loads((REPO / "analyst_map.json").read_text())
    covered = {sa.normalize_ticker(t) for t in analyst_map}

    dark, stale, ok, skipped = [], [], [], []

    for bbg, entry in sorted(doc["tickers"].items()):
        aliases = [sa.normalize_ticker(a) for a in entry.get("aliases", [])]
        if entry.get("status", "live") != "live":
            skipped.append((bbg, entry["status"]))
            continue
        if not args.all and not any(a in covered for a in aliases):
            continue

        sym = entry.get("yf")
        conf = entry.get("confidence", "?")
        if not sym:
            dark.append((bbg, sym, conf, "no yf symbol"))
            continue
        try:
            hist = yf.Ticker(sym).history(period="1mo", auto_adjust=True)
            closes = hist["Close"].dropna() if not hist.empty else None
            if closes is None or closes.empty:
                dark.append((bbg, sym, conf, "no price data"))
                continue
            age = (date.today() - closes.index[-1].date()).days
            if age > args.max_age:
                stale.append((bbg, sym, conf, f"last trade {age}d ago"))
            else:
                ok.append((bbg, sym, conf))
        except Exception as e:
            dark.append((bbg, sym, conf, str(e)[:60]))

    def dump(title, rows):
        if not rows:
            return
        print(f"\n{title} ({len(rows)})")
        for bbg, sym, conf, why in rows:
            print(f"  {bbg:14} yf={str(sym):14} [{conf}]  {why}")

    dump("DARK — no price, needs a decision", dark)
    dump("STALE — likely delisted, needs a decision", stale)

    print(f"\nOK: {len(ok)}   "
          f"(resolved={sum(1 for _b,_s,c in ok if c=='resolved')}, "
          f"assumed={sum(1 for _b,_s,c in ok if c=='assumed')})")
    if skipped:
        print(f"Already recorded as corporate actions: "
              f"{', '.join(f'{b} ({s})' for b, s in skipped)}")
    if dark or stale:
        print("\nFix in tools/gen_ticker_master.py (OVERRIDES or CORPORATE_ACTIONS), "
              "then re-run tools/gen_ticker_master.py.")
    return 1 if (dark or stale) else 0


if __name__ == "__main__":
    sys.exit(main())
