#!/usr/bin/env python3
"""
Merge the Lightkeeper Analyst blotter tag into analyst_map.json.

Lightkeeper's Analyst attribute is a TRADING attribution, not a research
coverage map:
  - BK/BT/JC/JEC/MS/SS are real research owners.
  - DM is the PM's catch-all bucket; RF is excluded per Hari.
  - Some covered names are never held, so LK has no row for them at all.

So LK is authoritative only where it carries a real research code, and the
hand-maintained map wins everywhere else.

Rules:
  1. Start from the existing analyst_map.json (coverage universe).
  2. Fill blanks with LK's real-coverage tag (max days-held wins ties).
  3. Never let DM/RF overwrite or create an entry.
  4. Leave existing hand entries alone, including the two that disagree
     with LK -- those are flagged for Hari, not auto-changed.
  5. Drop non-equity / housekeeping rows.
"""
import json, re, sys, pathlib

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
PULL = "analyst_2026-11-02.json"
sys.path.insert(0, str(REPO))
import stale_alerts as sa

COVERAGE_CODES = {"BK", "BT", "JC", "JEC", "MS", "SS"}
GENERIC_CODES  = {"DM", "RF", "Cross Rate"}

JUNK = {"CFD_DAST", "DIVIDENDS", "NOD"}
def is_junk(t):
    return (t in JUNK
            or t.startswith("CFD_")
            or re.fullmatch(r"MS[A-Z]{2,5}\d{2}[LS]", t) is not None)

lk   = json.loads((HERE / "lk_pulls" / PULL).read_text())
hand = json.loads((REPO / "analyst_map.json").read_text())

# best real-coverage tag per normalized ticker
best = {}
for code, names in lk.items():
    if code not in COVERAGE_CODES:
        continue
    for raw, days in names.items():
        t = sa.normalize_ticker(raw)
        if is_junk(t):
            continue
        if days > best.get(t, (None, -1))[1]:
            best[t] = (code, days)

merged = dict(hand)
added, conflicts = [], []
for t, (code, days) in sorted(best.items()):
    if t not in merged:
        merged[t] = code
        added.append((t, code, days))
    elif merged[t] != code:
        conflicts.append((t, merged[t], code, days))

merged = {t: c for t, c in sorted(merged.items()) if not is_junk(t)}

(REPO / "analyst_map.json").write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n")

print(f"hand map in : {len(hand)}")
print(f"LK coverage : {len(best)}")
print(f"added       : {len(added)}")
print(f"merged out  : {len(merged)}")
print(f"\nconflicts left as-is for Hari ({len(conflicts)}):")
for t, h, c, d in conflicts:
    print(f"   {t:8} hand={h}  LK={c} ({d:g}d)")
