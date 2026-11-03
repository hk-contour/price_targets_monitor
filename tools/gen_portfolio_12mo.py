#!/usr/bin/env python3
"""
Regenerate portfolio_12mo.json from a Lightkeeper days-held-by-rollup pull.

Input : raw_lk.json  — [[issuer, days_held], ...] exactly as returned by
        lightkeeper_get_portfolio_data(stat=days-held-by-rollup, ntm=max,
        rollup=Issuer, itmValues) over the trailing 12 months.
Output: portfolio_12mo.json

Rules:
  - days_held >= 1  (strict: a name that appears in the blotter but was never
    carried across a day is NOT "traded in the past 12 months")
  - drop FX / currency rows, swap baskets, CFD wrappers, housekeeping rows
  - normalize tickers the same way stale_alerts.normalize_ticker does
"""
import json, re, sys, pathlib
from datetime import date

HERE = pathlib.Path(__file__).parent
PULL = "days_held_2026-11-02.json"
RAW = json.loads((HERE / "lk_pulls" / PULL).read_text())

WINDOW_BEGIN = "2025-11-03"
WINDOW_END   = "2026-11-02"
MIN_DAYS_HELD = 1

# ── Non-equity / non-position rows ───────────────────────────────────────────
CURRENCY_CODES = {
    "AUD", "CAD", "CHF", "DKK", "EUR", "GBP", "HKD", "ILS", "JPY", "KRW",
    "NOK", "NZD", "SEK", "SGD", "TWD", "USD",
}
# Explicit non-equity rows seen in this pull
EXPLICIT_DROP = {
    "NOD",                 # Nordea FX/rates line, dropped in the prior snapshot too
    "DIVIDENDS ORPHANED",  # housekeeping
}

def is_currency(t: str) -> bool:
    if t in CURRENCY_CODES:
        return True
    # pair forms: CADUSD, JPYUSD, USDKRW, NOKUSD, TWDUSD, HKDUSD, SEKUSD ...
    if len(t) == 6 and t[:3] in CURRENCY_CODES and t[3:] in CURRENCY_CODES:
        return True
    # bracket form: "AUD [USD]"
    if re.fullmatch(r"[A-Z]{3} \[[A-Z]{3}\]", t):
        return True
    return False

def is_derivative_wrapper(t: str) -> bool:
    if t.startswith("CFD_"):
        return True
    # Morgan Stanley / street swap baskets, e.g. MSLYT26L, MSLYT26S
    if re.fullmatch(r"MS[A-Z]{2,5}\d{2}[LS]", t):
        return True
    # listed option strings, e.g. "DASH 1/15/27 C200 OTC"
    if re.search(r"\d+/\d+/\d+\s+[CP]\d", t):
        return True
    return False

def normalize_ticker(raw: str) -> str:
    return str(raw).strip().upper().split()[0] if raw else ""

kept, dropped = {}, {}

def drop(tkr, days, why):
    dropped.setdefault(why, []).append(f"{tkr} ({days:g}d)")

for issuer, days in RAW:
    if days is None:
        drop(issuer, 0, "null days-held"); continue
    norm = normalize_ticker(issuer)
    if not norm:
        continue
    upper_raw = str(issuer).strip().upper()
    if upper_raw in EXPLICIT_DROP or norm in EXPLICIT_DROP:
        drop(issuer, days, "non-equity / housekeeping row"); continue
    if is_currency(upper_raw) or is_currency(norm):
        drop(issuer, days, "currency / FX row"); continue
    if is_derivative_wrapper(upper_raw):
        drop(issuer, days, "option / swap-basket / CFD wrapper"); continue
    if days < MIN_DAYS_HELD:
        drop(issuer, days, f"days-held < {MIN_DAYS_HELD} (never carried overnight)"); continue
    # keep the max days-held if two raw rows normalize to the same ticker
    kept[norm] = max(kept.get(norm, 0), days)

out = {
    "_meta": {
        "source": "Lightkeeper LKP_MASTER__PORT",
        "stat": "days-held-by-rollup (max), rollup=Issuer",
        "series": "Trading (non-additive)",
        "window_begin": WINDOW_BEGIN,
        "window_end": WINDOW_END,
        "generated": date.today().isoformat(),
        "min_days_held": MIN_DAYS_HELD,
        "note": (
            "Normalized ticker set of every name HELD in the master portfolio during "
            f"the trailing 12 months, requiring days-held >= {MIN_DAYS_HELD} (a name that "
            "appears in the blotter but was never carried across a day does not count as "
            "traded). Regenerate periodically via Lightkeeper (GitHub Actions cannot reach "
            "the connector live). Currency, option, swap-basket and CFD rows are dropped "
            "during normalization."
        ),
        "dropped_rows": {k: sorted(v) for k, v in sorted(dropped.items())},
    },
    "tickers": sorted(kept),
}

dest = pathlib.Path(sys.argv[1])
dest.write_text(json.dumps(out, indent=2) + "\n")

print(f"kept {len(kept)} tickers -> {dest}")
for why, items in sorted(dropped.items()):
    print(f"  dropped [{why}] ({len(items)}): {', '.join(sorted(items))}")
