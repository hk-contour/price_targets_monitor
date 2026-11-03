#!/usr/bin/env python3
"""
Build portfolio_current.json -- what the book holds TODAY.

Companion to portfolio_12mo.json (membership over the trailing 12 months).
Together they let the alert separate:
    currently held  |  held sometime in the last 12 months  |  coverage only

Lightkeeper pull: delta-adjusted-net-exposure, netTimeMethod=end, rollup=Issuer,
return_what=itmValues, single date = the latest valid portfolio date. Save the
response rows as tools/lk_pulls/current_exposure_<date>.json, then run this.

A zero exposure is treated as NOT held -- a flat line is a closed position.

Usage:
    python3 tools/gen_portfolio_current.py [-o portfolio_current.json]
"""
import argparse, json, pathlib, re, sys
from datetime import date

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
sys.path.insert(0, str(REPO))
import stale_alerts as sa

PULL = "current_exposure_2026-11-02.json"
AS_OF = "2026-11-02"

CCY = {"AUD","CAD","CHF","DKK","EUR","GBP","HKD","ILS","JPY","KRW",
       "NOK","NZD","SEK","SGD","TWD","USD"}
JUNK = {"DIVIDENDS"}
JUNK_RE = re.compile(r"^(CFD_|EQSWAP_|MS[A-Z]{2,5}\d{2}[LS]$)")


def is_junk(t: str) -> bool:
    if t in JUNK or JUNK_RE.match(t):
        return True
    if t in CCY or (len(t) == 6 and t[:3] in CCY and t[3:] in CCY):
        return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=str(REPO / "portfolio_current.json"))
    args = ap.parse_args()

    rows = json.loads((HERE / "lk_pulls" / PULL).read_text())
    held, dropped = {}, {"zero exposure": [], "non-equity": []}

    for issuer, exposure in rows:
        t = sa.normalize_ticker(issuer)
        if not t:
            continue
        if is_junk(t):
            dropped["non-equity"].append(issuer)
            continue
        try:
            pct = float(str(exposure).strip().rstrip("%"))
        except ValueError:
            continue
        if pct == 0:
            dropped["zero exposure"].append(issuer)
            continue
        # keep the largest absolute exposure if two rows normalize together
        if abs(pct) > abs(held.get(t, 0.0)):
            held[t] = pct

    doc = {
        "_meta": {
            "source": "Lightkeeper LKP_MASTER__PORT",
            "stat": "delta-adjusted-net-exposure (end), rollup=Issuer, units=%",
            "series": "Trading (non-additive)",
            "as_of": AS_OF,
            "generated": date.today().isoformat(),
            "note": ("Names held in the master portfolio as of as_of. A zero exposure "
                     "counts as NOT held. Currency, swap-basket and CFD rows dropped. "
                     "Pairs with portfolio_12mo.json to separate currently-held names "
                     "from names traded earlier in the trailing 12 months."),
            "dropped_rows": {k: sorted(v) for k, v in dropped.items() if v},
        },
        "tickers": sorted(held),
        "exposure_pct": dict(sorted(held.items())),
    }
    pathlib.Path(args.out).write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote {args.out} — {len(held)} currently held (as of {AS_OF})")
    for k, v in dropped.items():
        if v:
            print(f"  dropped [{k}] ({len(v)}): {', '.join(sorted(v))}")


if __name__ == "__main__":
    main()
