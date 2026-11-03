#!/usr/bin/env python3
"""
Build ticker_master.json -- the single identifier map for both monitor.py and
stale_alerts.py.

Keyed on BBG ticker (space form: "IFX GY", "DSY FP", "SHOP CN"). Every other
code the firm uses -- the Reuters-ish Issuer codes in Contour-Price-Targets.csv,
the Lightkeeper blotter Issuer, the LK instrument ticker -- is an ALIAS that
resolves to one BBG key. yfinance symbols and corporate-action status hang off
the same entry.

Seeded from the Lightkeeper pulls in tools/lk_pulls/, then overlaid with the
curated OVERRIDES below for anything LK cannot settle (foreign listings,
renames, delistings).

Entries carry `confidence`:
  resolved -- curated below, or verified against a live price fetch
  assumed  -- plain US listing inferred as "<TICKER> US" / yf == ticker

Run tools/check_tickers.py to validate `assumed` entries and catch names that
have gone dark.
"""
import json, re, sys, pathlib
from datetime import date

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
sys.path.insert(0, str(REPO))
import stale_alerts as sa

DAYS_HELD_PULL = "days_held_2026-11-02.json"
ANALYST_PULL   = "analyst_2026-11-02.json"

# ─────────────────────────────────────────────────────────────────────────────
# Curated overrides. Anything not listed here and not junk is treated as a
# plain US listing. Keys are the normalized code as it appears in the PT CSV.
# ─────────────────────────────────────────────────────────────────────────────
OVERRIDES = {
    # ── Foreign listings: mapping only, all live ─────────────────────────────
    "IFXGN": ("IFX GY",    "IFX.DE",     "Infineon Technologies"),
    "SAPG":  ("SAP GY",    "SAP.DE",     "SAP"),
    "AG1G":  ("AG1 GY",    "AG1.F",      "AUTO1 Group"),
    "RHMG":  ("RHM GY",    "RHM.DE",     "Rheinmetall"),
    "PUBP":  ("PUB FP",    "PUB.PA",     "Publicis Groupe"),
    "DAST":  ("DSY FP",    "DSY.PA",     "Dassault Systemes"),
    "WISEA": ("WISE LN",   "WISE.L",     "Wise plc"),
    "REL":   ("REL LN",    "REL.L",      "RELX"),
    "LOOMB": ("LOOM SS",   "LOOMIS.ST",  "Loomis AB"),
    "CSU":   ("CSU CN",    "CSU.TO",     "Constellation Software"),
    "RCIB":  ("RCI/B CN",  "RCI-B.TO",   "Rogers Communications B"),
    "ATRL":  ("ATRL CN",   "ATRL.TO",    "SNC-Lavalin / AtkinsRealis"),
    "STN":   ("STN CN",    "STN.TO",     "Stantec"),
    "WSP":   ("WSP CN",    "WSP.TO",     "WSP Global"),
    "GIBA":  ("GIB/A CN",  "GIB-A.TO",   "CGI Inc A"),
    "WTC":   ("WTC AU",    "WTC.AX",     "WiseTech Global"),
    "REA":   ("REA AU",    "REA.AX",     "REA Group"),
    "NOKIA": ("NOK US",    "NOK",        "Nokia (ADR)"),
    "TSM":   ("TSM US",    "TSM",        "TSMC (ADR)"),
    "FLTRF": ("FLUT US",   "FLUT",       "Flutter Entertainment"),
    "TEPRF": ("TPK LN",    "TPK.L",      "Travis Perkins"),
    "WLSNC": ("WKL NA",    "WKL.AS",     "Wolters Kluwer"),
    "ADYEN": ("ADYEN NA",  "ADYEN.AS",   "Adyen"),
    "ESLT":  ("ESLT IT",   "ESLT",       "Elbit Systems"),
    "NICE":  ("NICE IT",   "NICE",       "NICE Ltd (ADR)"),

    # ── Japan / Korea / Taiwan numeric lines ─────────────────────────────────
    "8136":  ("8136 JP",   "8136.T",     "Sanrio"),
    "6098":  ("6098 JP",   "6098.T",     "Recruit Holdings"),
    "7974":  ("7974 JP",   "7974.T",     "Nintendo"),
    "7751":  ("7751 JP",   "7751.T",     "Canon"),
    "4324":  ("4324 JP",   "4324.T",     "Dentsu"),
    "6981":  ("6981 JP",   "6981.T",     "Murata Manufacturing"),
    "6963":  ("6963 JP",   "6963.T",     "Rohm"),
    "6857":  ("6857 JP",   "6857.T",     "Advantest"),
    "4661":  ("4661 JP",   "4661.T",     "Oriental Land"),
    "6594":  ("6594 JP",   "6594.T",     "Nidec"),
    "6752":  ("6752 JP",   "6752.T",     "Panasonic"),
    "4901":  ("4901 JP",   "4901.T",     "FUJIFILM"),
    "5802":  ("5802 JP",   "5802.T",     "Sumitomo Electric"),
    "6758":  ("6758 JP",   "6758.T",     "Sony"),
    "9992":  ("9992 HK",   "9992.HK",    "Pop Mart"),
    "2330":  ("2330 TT",   "2330.TW",    "TSMC (Taiwan)"),
    "3034":  ("3034 TT",   "3034.TW",    "Novatek Microelectronics"),
    "000660": ("000660 KS", "000660.KS", "SK Hynix"),
    "005930": ("005930 KS", "005930.KS", "Samsung Electronics"),

    # ── Renames ──────────────────────────────────────────────────────────────
    "SATS":  ("ECHO US",   "ECHO",       "EchoStar (was SATS)"),
}

# Renamed tickers that already have an entry under the NEW code. The old code
# becomes an extra alias on that same entry rather than a second entry, so the
# PT CSV's historic rows still resolve.
RENAMED_ONTO = {
    "TPX": "SGI US",   # Tempur Sealy -> Somnigroup International
    "ZI":  "GTM US",   # ZoomInfo -> GTM (Go To Market)
}

# Corporate actions: no longer tradeable. Excluded from alerts, surfaced in the
# summary under "corporate actions" so coverage can be cleaned up.
CORPORATE_ACTIONS = {
    "QRVO": ("QRVO US", "acquired", "2026-10-05",
             "Acquired by Skyworks; delisted from Nasdaq", "SWKS US"),
    "OS":   ("OS US",   "private",  "2026-04-01",
             "Taken private by Hg; ceased trading on Nasdaq", None),
    "WNS":  ("WNS US",  "acquired", "2025-10-17",
             "Acquired by Capgemini; delisted from NYSE", None),
    "ACVA": ("ACVA US", "acquired", "2026-10-16",
             "Acquired by Copart; cash merger 2026-10-20", "CPRT US"),
}

# Rows that are not securities at all.
JUNK_RE = re.compile(r"^(CFD_|EQSWAP_|MS[A-Z]{2,5}\d{2}[LS]$)")
JUNK = {"NOD", "DIVIDENDS", "ARXS", "CBRS", "INIO", "MAIR", "QNT", "YSS", "SPCX", "CVSA"}
CURRENCY = re.compile(r"^[A-Z]{3}(USD)?$")
CCY = {"AUD","CAD","CHF","DKK","EUR","GBP","HKD","ILS","JPY","KRW","NOK","NZD","SEK","SGD","TWD","USD"}

def is_junk(t):
    if t in JUNK or JUNK_RE.match(t):
        return True
    if t in CCY or (len(t) == 6 and t[:3] in CCY and t[3:] in CCY):
        return True
    return False


def build():
    days  = json.loads((HERE / "lk_pulls" / DAYS_HELD_PULL).read_text())
    lkan  = json.loads((HERE / "lk_pulls" / ANALYST_PULL).read_text())
    hand  = json.loads((REPO / "analyst_map.json").read_text())

    # universe = everything LK has held + everything an analyst covers
    universe = set()
    for issuer, _d in days:
        universe.add(sa.normalize_ticker(issuer))
    for _code, names in lkan.items():
        for raw in names:
            universe.add(sa.normalize_ticker(raw))
    universe |= {sa.normalize_ticker(t) for t in hand}
    universe = {t for t in universe if t and not is_junk(t)}

    out, counts = {}, {"resolved": 0, "assumed": 0, "corporate_action": 0}
    for code in sorted(universe):
        if code in RENAMED_ONTO:
            continue  # folded in as an alias below
        if code in CORPORATE_ACTIONS:
            bbg, status, when, note, succ = CORPORATE_ACTIONS[code]
            entry = {"aliases": [code], "yf": None, "status": status,
                     "status_date": when, "note": note, "confidence": "resolved"}
            if succ:
                entry["successor"] = succ
            counts["corporate_action"] += 1
        elif code in OVERRIDES:
            bbg, yf, name = OVERRIDES[code]
            entry = {"name": name, "aliases": [code], "yf": yf,
                     "status": "live", "confidence": "resolved"}
            counts["resolved"] += 1
        else:
            bbg = f"{code} US"
            entry = {"aliases": [code], "yf": code,
                     "status": "live", "confidence": "assumed"}
            counts["assumed"] += 1
        out.setdefault(bbg, entry)

    # fold renamed predecessors in as aliases on the surviving entry
    for old, new_bbg in sorted(RENAMED_ONTO.items()):
        if new_bbg in out:
            out[new_bbg].setdefault("aliases", []).append(old)
            out[new_bbg]["aliases"] = sorted(set(out[new_bbg]["aliases"]))
            out[new_bbg]["former_codes"] = sorted(
                set(out[new_bbg].get("former_codes", []) + [old]))
            out[new_bbg]["confidence"] = "resolved"
        else:
            print(f"  WARNING: rename target {new_bbg} for {old} not in universe")

    doc = {
        "_meta": {
            "generated": date.today().isoformat(),
            "key": "Bloomberg ticker, space form (e.g. 'IFX GY')",
            "aliases": "Codes used by Contour-Price-Targets.csv and the Lightkeeper blotter",
            "seeded_from": [f"tools/lk_pulls/{DAYS_HELD_PULL}",
                            f"tools/lk_pulls/{ANALYST_PULL}",
                            "analyst_map.json"],
            "confidence": {
                "resolved": "curated in tools/gen_ticker_master.py OVERRIDES",
                "assumed":  "inferred as a plain US listing; validate with tools/check_tickers.py",
            },
            "status_values": ["live", "acquired", "private", "renamed"],
            "counts": counts,
        },
        "tickers": out,
    }
    dest = REPO / "ticker_master.json"
    dest.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    print(f"wrote {dest} -- {len(out)} entries  {counts}")


if __name__ == "__main__":
    build()
