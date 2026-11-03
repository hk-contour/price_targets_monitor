#!/usr/bin/env python3
"""
Build earnings_dates.json -- "who has reported recently" -- from the TMT
Earnings Calendar workbooks.

Source (Dropbox):
    /Hari Kumar/06. Research/Earnings Calendar/TMT Earnings Calendar_<date>.xlsx

The calendar is FORWARD-looking: one row per ticker carrying its NEXT earnings
date. Once a company reports, the next snapshot rolls that row forward to the
following quarter -- so a single file cannot tell you who just reported.

Recovering that needs several weekly snapshots. For each ticker we take the
latest earnings date across all snapshots that is on or before today. Example:
the 10/23 snapshot has SPOT at 10/22 Confirmed; the 10/30 snapshot has already
rolled it to 2/10/2027. Only the older file knows SPOT reported on 10/22.

Keep the last ~6-8 weekly workbooks in the input folder so a full reporting
season stays recoverable.

Usage:
    python3 tools/gen_earnings_dates.py <folder-of-xlsx> [-o earnings_dates.json]

Layout (sheet "TMT Earnings Calendar"): headers on row 7, data from row 8 --
col B Ticker, C Name, D Earnings Date, E Status, F Session.
Skip the "DAILY VIEW" workbooks; those are a derived pivot, not the table.
"""
import argparse, json, pathlib, re, sys
from datetime import date, datetime

import openpyxl

REPO = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(REPO))
import stale_alerts as sa

HEADER_ROW, FIRST_DATA_ROW = 7, 8
COL_TICKER, COL_NAME, COL_DATE, COL_STATUS, COL_SESSION = 2, 3, 4, 5, 6

# A date we trust enough to nudge on. "Estimate" is Bloomberg guesswork and is
# routinely wrong by days, so it must never trigger an analyst email.
TRUSTED_STATUS = {"CONFIRMED", "TENTATIVE"}


def parse_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s or not re.match(r"^\d{1,2}/\d{1,2}/\d{4}", s):
        return None
    try:
        return datetime.strptime(s.split()[0], "%m/%d/%Y").date()
    except ValueError:
        return None


def read_workbook(path):
    """Yields (bbg_ticker_as_written, report_date, status, session)."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        ws = (wb["TMT Earnings Calendar"]
              if "TMT Earnings Calendar" in wb.sheetnames else wb.active)
        # iter_rows, not ws.cell(): in read_only mode max_row can report a huge
        # sparse range and per-cell access re-walks the sheet each time.
        for i, row in enumerate(ws.iter_rows(min_col=1, max_col=COL_SESSION,
                                             values_only=True), start=1):
            if i < FIRST_DATA_ROW:
                continue
            tk = row[COL_TICKER - 1]
            dt = parse_date(row[COL_DATE - 1])
            if not tk or not dt:
                continue
            yield (str(tk).strip().upper(), dt,
                   str(row[COL_STATUS - 1] or "").strip(),
                   str(row[COL_SESSION - 1] or "").strip())
    finally:
        wb.close()


def resolve_bbg(raw: str, master: dict) -> str | None:
    """Calendar ticker -> ticker_master BBG key."""
    if raw in master:                      # already BBG form: "SHOP CN", "5802 JP"
        return raw
    us = f"{raw} US"
    if us in master:                       # plain US listing: "SPOT" -> "SPOT US"
        return us
    entry = sa.TICKER_BY_ALIAS.get(sa.normalize_ticker(raw))
    return entry["bbg"] if entry else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder", help="folder of TMT Earnings Calendar .xlsx snapshots")
    ap.add_argument("-o", "--out", default=str(REPO / "earnings_dates.json"))
    ap.add_argument("--as-of", default=None, help="treat this as today (YYYY-MM-DD)")
    args = ap.parse_args()

    today = date.fromisoformat(args.as_of) if args.as_of else date.today()
    sa.load_ticker_master(str(REPO / "ticker_master.json"))
    master = sa.TICKER_BY_BBG

    files = sorted(p for p in pathlib.Path(args.folder).glob("*.xlsx")
                   if "DAILY VIEW" not in p.name.upper() and not p.name.startswith("~$"))
    if not files:
        sys.exit(f"no calendar workbooks in {args.folder}")

    reported, unmatched = {}, {}
    for path in files:
        for raw, dt, status, session in read_workbook(path):
            if dt > today:
                continue                                  # not yet reported
            if status.upper() not in TRUSTED_STATUS:
                continue                                  # Bloomberg estimate, don't nudge on it
            bbg = resolve_bbg(raw, master)
            if not bbg:
                prev = unmatched.get(raw)
                if not prev or dt > date.fromisoformat(prev["date"]):
                    unmatched[raw] = {"date": dt.isoformat(), "status": status}
                continue
            prev = reported.get(bbg)
            if not prev or dt > date.fromisoformat(prev["date"]):
                reported[bbg] = {"date": dt.isoformat(), "status": status,
                                 "session": session, "source": path.name}

    doc = {
        "_meta": {
            "generated": today.isoformat(),
            "source": "Dropbox /Hari Kumar/06. Research/Earnings Calendar",
            "snapshots": [p.name for p in files],
            "rule": ("latest earnings date on or before the generated date, across all "
                     "snapshots; only Confirmed/Tentative rows -- Bloomberg Estimate "
                     "dates are excluded so an analyst is never nudged on a guess"),
            "key": "Bloomberg ticker, matching ticker_master.json",
            "counts": {"reported": len(reported), "unmatched": len(unmatched)},
        },
        "reported": dict(sorted(reported.items())),
        "unmatched": dict(sorted(unmatched.items())),
    }
    pathlib.Path(args.out).write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote {args.out}")
    print(f"  snapshots : {', '.join(p.name for p in files)}")
    print(f"  reported  : {len(reported)}")
    print(f"  unmatched : {len(unmatched)} (not in ticker_master; mostly non-covered names)")


if __name__ == "__main__":
    main()
