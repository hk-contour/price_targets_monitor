#!/usr/bin/env python3
"""
Build pt_submissions.json -- when each name's target was last SUBMITTED by its
analyst, from the pricetargets@contourasset.com inbox.

Why this exists
---------------
Contour-Price-Targets.csv records what the desk has POSTED to the master sheet.
The inbox records what the analyst has SENT. The sheet always lags, sometimes
by a week or more, and the gap is worst exactly where it hurts: right after a
print, when the analyst has just submitted and the alert is about to nudge
them for it.

Per Hari: every email in that inbox is a target submission. So the submission
date is the authoritative "when was this target last set", and the alert takes
    effective PT date = max(CSV BeginDate, submission date)
for both the age rule and the earnings rule.

Input
-----
tools/inbox/pt_inbox_<date>.json -- [{"d": "YYYY-MM-DD", "s": subject, "f": sender}, ...]
pulled from the shared mailbox (Outlook connector, newest first, paged back
far enough to cover earnings_lookback_days with room to spare).

Ticker extraction
-----------------
Subjects follow two shapes, both seen in the live inbox:
    "ExlService (EXLS US) - Updated Price Targets"   -> parenthetical, preferred
    "RDDT Update"                                     -> bare leading token
Everything resolves through ticker_master.json, so "Sumitomo Electric (5802 JP)"
and "TSMC (2330 TT)" land on the right BBG key.

Unresolved subjects are reported, never dropped silently -- a subject we cannot
parse is a name we might wrongly nudge.
"""
import argparse, json, pathlib, re, sys
from datetime import date

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
sys.path.insert(0, str(REPO))
import stale_alerts as sa

PAREN = re.compile(r"\(([A-Z0-9]{1,7}(?:[/.][A-Z])?(?:\s+[A-Z]{2})?)\)")
LEAD  = re.compile(r"^([A-Z0-9]{1,7}(?:\s+[A-Z]{2})?)\b")

# Subject words that look like tickers but are not.
NOT_TICKERS = {"PT", "PTS", "FW", "RE", "UPDATE", "UPDATED", "NEW", "Q1", "Q2",
               "Q3", "Q4", "FY", "US", "THE"}


def extract(subject: str, master: dict):
    """Returns (bbg_key, raw_token) or (None, None)."""
    cands = PAREN.findall(subject) + LEAD.findall(subject.upper())
    for raw in cands:
        raw = raw.strip().upper()
        if raw in NOT_TICKERS:
            continue
        for probe in (raw, f"{raw} US"):
            if probe in master:
                return probe, raw
        entry = sa.TICKER_BY_ALIAS.get(sa.normalize_ticker(raw))
        if entry:
            return entry["bbg"], raw
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inbox", nargs="?",
                    default=str(HERE / "inbox" / "pt_inbox_2026-11-03.json"))
    ap.add_argument("-o", "--out", default=str(REPO / "pt_submissions.json"))
    args = ap.parse_args()

    sa.load_ticker_master(str(REPO / "ticker_master.json"))
    master = sa.TICKER_BY_BBG
    msgs = json.loads(pathlib.Path(args.inbox).read_text())

    subs, unresolved = {}, []
    for m in msgs:
        bbg, raw = extract(m["s"], master)
        if not bbg:
            unresolved.append({"subject": m["s"], "date": m["d"]})
            continue
        prev = subs.get(bbg)
        if not prev or m["d"] > prev["date"]:
            subs[bbg] = {"date": m["d"], "from": m.get("f", ""), "subject": m["s"]}

    dates = sorted(m["d"] for m in msgs)
    doc = {
        "_meta": {
            "generated": date.today().isoformat(),
            "source": "pricetargets@contourasset.com (every email is a target submission)",
            "messages_read": len(msgs),
            "window": {"from": dates[0], "to": dates[-1]} if dates else {},
            "rule": ("latest submission per name; the alert uses "
                     "max(CSV BeginDate, submission date) as the effective "
                     "price-target date for BOTH the age rule and the earnings rule"),
            "key": "Bloomberg ticker, matching ticker_master.json",
            "counts": {"resolved": len(subs), "unresolved": len(unresolved)},
        },
        "submissions": dict(sorted(subs.items())),
        "unresolved": unresolved,
    }
    pathlib.Path(args.out).write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote {args.out}")
    print(f"  messages   : {len(msgs)}  ({dates[0]} -> {dates[-1]})")
    print(f"  resolved   : {len(subs)}")
    if unresolved:
        print(f"  UNRESOLVED ({len(unresolved)}) — add a ticker_master alias:")
        for u in unresolved:
            print(f"     {u['date']}  {u['subject']}")


if __name__ == "__main__":
    main()
