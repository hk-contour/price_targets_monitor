#!/usr/bin/env python3
"""
Contour Stale Price Target Alert

Two independent triggers. Either one alone is enough to nudge the analyst.

  1. AGE + DRIFT  (unchanged)
     Name was in the MASTER portfolio in the past 12 months (verified against
     the Lightkeeper membership snapshot, portfolio_12mo.json) AND
       - the price target has not been updated in >= stale_age_days (135), AND
       - price is >= price_drift_pct (30%) OUTSIDE the target band -- that is
         30%+ ABOVE the upside PT, or 30%+ BELOW the downside PT.
     A price inside the band never flags, however old the target. A breakout on
     a target newer than 135 days never flags. Prices are split-adjusted first,
     so a stock split cannot masquerade as a breakout.

  2. REPORTED SINCE THE TARGET WAS SET
     The company has reported (per the TMT Earnings Calendar) and its price
     target predates the print. Fires regardless of PT age or where price sits
     in the band -- a 60-day-old target inside the band still needs refreshing
     once the company has reported against it.
       - waits earnings_grace_days (3) after the print: analysts know to update,
         so same-day and next-day nudges are noise
       - stops after earnings_lookback_days (45), so last quarter's print does
         not nudge forever
       - follows the COVERAGE universe (analyst_map.json), not portfolio
         membership, because a covered name can report while the book holds
         none of it
       - only Confirmed/Tentative dates; Bloomberg "Estimate" dates are
         excluded so nobody is nudged on a guessed date

Action:
  - Email the responsible analyst (or hari only in TEST_MODE)
  - 1st nudge on first detection
  - 2nd nudge after SECOND_NUDGE_DAYS if still stale and no acknowledgment
  - Quiet after the 2nd nudge until a new TRIGGER re-arms the cycle: either a
    refreshed price target (new BeginDate) or a fresh earnings print. Each
    quarter's print therefore gets its own two nudges.

Accuracy safeguards:
  - Every ticker that fails a price fetch is reported (never silently dropped).
  - Delisted / acquired names are reported once under "corporate actions"
    rather than recurring as price-fetch failures.
  - Every stale name excluded ONLY because it's not in the 12-month portfolio
    is listed in the summary email to hari, so nothing goes undetected.

Identifiers all resolve through ticker_master.json (BBG-keyed). Regenerate the
derived data files with the scripts in tools/.
"""

import os
import json
import logging
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — edit this section
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # ── Flag rule (BOTH conditions required) ──────────────────────────────────
    # A name flags only if it is stale by age AND price has broken out of the band.
    "stale_age_days":      135,    # Condition 1: PT not updated in >= this many days
    "price_drift_pct":     0.30,   # Condition 2: price >= this % above upside OR below downside

    # Split-adjustment guard: if price is this far beyond a band edge, check for a
    # stock split before flagging (a split can look like a huge breakout).
    "split_adjust_trigger_pct": 0.20,

    # ── Earnings trigger (independent of the age/drift rule above) ───────────
    # A name that has just reported with a price target predating the print
    # needs a refresh regardless of how old the PT is or where price sits in
    # the band. Deliberately NOT same-day: analysts know to update, so the
    # nudge only makes sense once a few days have passed.
    "earnings_grace_days":    3,   # wait this long after the print before nudging
    "earnings_lookback_days": 45,  # stop nudging on a print older than this
    # Earnings alerts follow the coverage universe (analyst_map), not portfolio
    # membership: a covered name can report while the book holds none of it.
    "earnings_requires_portfolio": False,
    "earnings_snapshot_max_age_days": 14,

    # ── Alert cadence ─────────────────────────────────────────────────────────
    "second_nudge_days":   5,      # Days after 1st alert before sending 2nd nudge (if no ack)

    # ── TEST MODE — flip to False to send to real analysts ────────────────────
    "test_mode":           True,
    "test_email":          "hari.kumar@contourasset.com",

    # ── Analyst emails ────────────────────────────────────────────────────────
    "analyst_emails": {
        "JC": "james.collins@contourasset.com",
        "SS": "saaheb.sidana@contourasset.com",
        "BT": "brian.thackray@contourasset.com",
        "MS": "mit.shah@contourasset.com",
        "BK": "bronson.kussin@contourasset.com",
    },

    # ── Email delivery: Power Automate webhook (non-premium Teams trigger) ────
    # This posts {body, subject, to} to a Power Automate flow whose trigger is
    # "When a Teams webhook request is received" (a STANDARD, non-premium
    # connector — the same kind monitor.py uses). The flow reads the three
    # fields and sends the email, routing To dynamically per analyst.
    # Paste the new flow's HTTP POST URL here after you build it.
    "power_automate_url":  "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/18/workflows/33f7e228caf94286bb2e010b73657138/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EKQ7c4Pr8kktKEPpGX90Us9e9Kit0tECPvz_89TDDts",

    # ── Analyst action: email the price-targets inbox ─────────────────────────
    # The button in each alert opens a pre-filled email to this inbox. The
    # analyst sends the updated target (or "no change"); ops updates the master
    # spreadsheet from there. Once the sheet's BeginDate is refreshed, the
    # script auto-detects the new target and stops nudging — no manual reset.
    "pricetargets_inbox":  "pricetargets@contourasset.com",

    # ── File paths (relative to repo root) ───────────────────────────────────
    "csv_path":            "Contour-Price-Targets.csv",
    "portfolio_12mo_path": "portfolio_12mo.json",   # Lightkeeper 12-month membership snapshot
    "analyst_map_path":    "analyst_map.json",
    "ticker_master_path":  "ticker_master.json",    # BBG-keyed identifier + status map
    "earnings_dates_path": "earnings_dates.json",   # who has reported recently
    "state_path":          "stale_alerts_state.json",

    # Warn in the summary email if the membership snapshot is older than this.
    "portfolio_snapshot_max_age_days": 45,
}

# ── Identifier map ────────────────────────────────────────────────────────────
# ticker_master.json is the single source of truth for symbol resolution and
# corporate-action status. It is keyed on BBG ticker (space form, "IFX GY");
# the Reuters-ish codes in Contour-Price-Targets.csv and the Lightkeeper
# blotter codes are ALIASES onto that key.
#
# This replaces the old NUMERIC_TICKERS / TICKER_MAP / SKIP dicts, which were
# matched against already-normalized (uppercased, space-split) tickers -- so
# every lowercase or spaced key in them ("IFXGn", "WISEa", "RCIb", "LOOMb",
# "PRSM LN", "JET LN") silently never matched.
#
# Regenerate with tools/gen_ticker_master.py; validate with tools/check_tickers.py.

TICKER_BY_BBG:   dict = {}
TICKER_BY_ALIAS: dict = {}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def normalize_ticker(raw: str) -> str:
    return str(raw).strip().upper().split()[0] if raw else ""


def load_ticker_master(path: str):
    """
    Loads ticker_master.json into TICKER_BY_BBG / TICKER_BY_ALIAS.

    Fatal if missing: without it we cannot tell a delisted name from a
    mapping bug, and would start guessing symbols.
    """
    global TICKER_BY_BBG, TICKER_BY_ALIAS
    if not os.path.exists(path):
        log.error(f"Ticker master not found: {path} — aborting rather than "
                  f"guessing symbols. Run tools/gen_ticker_master.py.")
        raise SystemExit(1)
    with open(path) as f:
        doc = json.load(f)

    TICKER_BY_BBG = doc.get("tickers", {})
    TICKER_BY_ALIAS = {}
    for bbg, raw_entry in TICKER_BY_BBG.items():
        entry = {**raw_entry, "bbg": bbg}
        for alias in entry.get("aliases", []):
            TICKER_BY_ALIAS[normalize_ticker(alias)] = entry

    meta = doc.get("_meta", {})
    log.info(f"Loaded {len(TICKER_BY_BBG)} ticker-master entries "
             f"({len(TICKER_BY_ALIAS)} aliases), generated {meta.get('generated', '?')}.")
    return meta


def resolve_ticker(ticker: str) -> dict | None:
    """Returns the ticker_master entry for any alias, or None if unmapped."""
    return TICKER_BY_ALIAS.get(normalize_ticker(ticker))

def yf_symbol(ticker: str) -> str:
    e = resolve_ticker(ticker)
    return e["yf"] if e and e.get("yf") else ticker

def display_ticker(ticker: str) -> str:
    """BBG ticker for display — the firm's canonical identifier."""
    e = resolve_ticker(ticker)
    return e["bbg"] if e else ticker

def ticker_status(ticker: str) -> str:
    """'live', a corporate-action status ('acquired'/'private'/...), or 'unmapped'."""
    e = resolve_ticker(ticker)
    return e.get("status", "live") if e else "unmapped"

def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None

def fetch_price(ticker: str):
    """Returns (price, error_reason) — price is None on failure."""
    sym = yf_symbol(ticker)
    try:
        hist = yf.Ticker(sym).history(period="1mo", auto_adjust=True)
        if hist.empty:
            return None, f"no history returned for {sym}"
        # Some venues (Korea, Taiwan) return a trailing bar with a NaN close.
        # A NaN price fails every comparison silently, so the name would never
        # flag and never be reported — drop to the last VALID close instead.
        closes = hist["Close"].dropna()
        if closes.empty:
            return None, f"no valid close in last month for {sym}"
        price = round(float(closes.iloc[-1]), 2)
        if price <= 0:
            return None, f"non-positive close {price} for {sym}"
        data_age = (date.today() - closes.index[-1].date()).days
        if data_age > 5:
            return None, f"data is {data_age} days stale ({sym})"
        return price, None
    except Exception as e:
        return None, str(e)


def get_split_factor(ticker: str, target_date_iso: str) -> float:
    """
    Cumulative split factor for splits AFTER the target date.
    Used to rebase an old PT onto today's split-adjusted price so a split
    doesn't masquerade as a stale target. Ported from monitor.py.
    Returns 1.0 if no split (or on any error — fail safe, no rebase).
    """
    sym = yf_symbol(ticker)
    try:
        tgt = date.fromisoformat(target_date_iso)
        splits = yf.Ticker(sym).splits
        if splits is None or splits.empty:
            return 1.0
        after = splits[splits.index.date > tgt]
        if after.empty:
            return 1.0
        factor = 1.0
        for ratio in after.values:
            factor *= float(ratio)
        return factor if factor > 0 else 1.0
    except Exception as e:
        log.warning(f"{ticker}: split check failed — {e}")
        return 1.0

# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────

def load_targets(path: str) -> dict:
    """
    Returns {ticker: {upside, downside, date, age_days}} for the most
    recent price target entry per ticker.
    """
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["BeginDate"] = pd.to_datetime(df["BeginDate"], format="mixed", dayfirst=False)
    df["Issuer"]    = df["Issuer"].astype(str).map(normalize_ticker)
    df = df.sort_values("BeginDate", ascending=False).drop_duplicates("Issuer", keep="first")

    targets = {}
    for _, row in df.iterrows():
        ticker   = row["Issuer"]
        tgt_date = row["BeginDate"].date()
        upside   = _to_float(row.get("Upside Price Target"))
        downside = _to_float(row.get("Downside Price Target"))
        if upside is None and downside is None:
            continue
        targets[ticker] = {
            "upside":    upside,
            "downside":  downside,
            "date":      tgt_date.isoformat(),
            "age_days":  (date.today() - tgt_date).days,
        }
    log.info(f"Loaded {len(targets)} price target entries.")
    return targets


def load_portfolio_12mo(path: str) -> tuple[set, dict]:
    """
    Loads the Lightkeeper 12-month master-portfolio membership snapshot.
    Returns (set_of_tickers, meta_dict).

    This snapshot is generated from Lightkeeper (LKP_MASTER__PORT,
    days-held-by-rollup over the trailing 12 months). GitHub Actions cannot
    reach the Lightkeeper connector live, so the file is committed to the repo
    and must be refreshed periodically.
    """
    if not os.path.exists(path):
        log.error(f"Portfolio membership file not found: {path} — "
                  f"CANNOT verify 12-month membership. Aborting to avoid mis-targeting analysts.")
        raise SystemExit(1)
    with open(path) as f:
        data = json.load(f)
    tickers = {normalize_ticker(t) for t in data.get("tickers", [])}
    meta = data.get("_meta", {})
    log.info(f"Loaded {len(tickers)} tickers from 12-month portfolio snapshot "
             f"(generated {meta.get('generated', '?')}).")
    return tickers, meta


def load_analyst_map(path: str) -> dict:
    """Returns {ticker: analyst_code}."""
    if not os.path.exists(path):
        log.warning(f"Analyst map not found: {path}")
        return {}
    with open(path) as f:
        return json.load(f)


def load_earnings_dates(path: str) -> tuple[dict, dict]:
    """
    Loads earnings_dates.json -> ({bbg: {date, status, session}}, meta).

    Optional: a missing file disables the earnings trigger but leaves the
    age/drift rule untouched, so the alert degrades rather than breaking.
    Regenerate with tools/gen_earnings_dates.py.
    """
    if not os.path.exists(path):
        log.warning(f"Earnings dates not found: {path} — earnings trigger disabled "
                    f"for this run. Run tools/gen_earnings_dates.py.")
        return {}, {}
    with open(path) as f:
        doc = json.load(f)
    reported = doc.get("reported", {})
    meta = doc.get("_meta", {})
    log.info(f"Loaded {len(reported)} recent earnings dates "
             f"(generated {meta.get('generated', '?')}).")
    return reported, meta


def earnings_reason(ticker: str, info: dict, reported: dict, today: date):
    """
    Returns (reason, report_date) when a name has reported since its price
    target was set, or (None, None).

    Fires when ALL hold:
      - we have a trusted (Confirmed/Tentative) report date for the name
      - the print is at least earnings_grace_days old   (not the morning after)
      - the print is at most earnings_lookback_days old (not last quarter's)
      - the price target predates the print

    Independent of the age/drift rule: a 60-day-old PT sitting inside the band
    still needs a refresh once the company has reported against it.
    """
    entry = resolve_ticker(ticker)
    if not entry:
        return None, None
    rec = reported.get(entry["bbg"])
    if not rec:
        return None, None

    report_date = date.fromisoformat(rec["date"])
    days_since  = (today - report_date).days
    if days_since < CONFIG["earnings_grace_days"]:
        return None, None
    if days_since > CONFIG["earnings_lookback_days"]:
        return None, None

    pt_date = date.fromisoformat(info["date"])
    if pt_date >= report_date:
        return None, None       # target already refreshed since the print

    return (f"Reported {report_date:%b %d} ({days_since}d ago) — price target "
            f"dates from {pt_date:%b %d, %Y} and has not been updated since",
            report_date)


def load_state(path: str) -> dict:
    """Loads alert state. Creates fresh state if file missing."""
    empty = {"first_alert": {}, "second_alert": {}, "acknowledged": {}}
    if not os.path.exists(path):
        return empty
    try:
        with open(path) as f:
            data = json.load(f)
        # Ensure all keys exist
        for k in empty:
            data.setdefault(k, {})
        return data
    except Exception as e:
        log.warning(f"Failed to load state: {e}. Starting fresh.")
        return empty


def save_state(state: dict, path: str):
    with open(path, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    log.info(f"State saved to {path}")

# ─────────────────────────────────────────────────────────────────────────────
# STALENESS CHECK
# ─────────────────────────────────────────────────────────────────────────────

def staleness_reasons(ticker: str, info: dict, price: float) -> tuple[list[str], dict]:
    """
    Returns (reasons, adjusted_targets).
      reasons          — list of human-readable reason strings; empty = not stale
      adjusted_targets — {'upside', 'downside'} after any split rebasing (for display)

    Flag rule — BOTH must be true (portfolio membership is enforced upstream):
      1. PT is stale by age:  age_days >= stale_age_days (135d), AND
      2. Price is >= price_drift_pct (30%) OUTSIDE the target band:
           price >= 30% ABOVE the upside PT, OR
           price >= 30% BELOW the downside PT.

    A price inside the [downside, upside] band never flags, no matter how old.
    An old-but-in-range target never flags. A big breakout on a fresh (<135d)
    target never flags. Only stale-AND-broken-out fires.
    """
    upside   = info["upside"]
    downside = info["downside"]
    age_days = info["age_days"]

    extreme  = CONFIG["price_drift_pct"]       # 0.30
    trig     = CONFIG["split_adjust_trigger_pct"]

    # ── Condition 1: must be stale by age. If not, stop — nothing flags. ───────
    if age_days < CONFIG["stale_age_days"]:
        return [], {"upside": upside, "downside": downside}

    # ── Split adjustment guard ────────────────────────────────────────────────
    # A split makes the price land wildly OUTSIDE the target band. If the price
    # is beyond an edge by more than the trigger, check for a post-target split
    # and rebase the targets before judging the breakout.
    beyond_band = (
        (upside   is not None and price > upside   * (1 + trig)) or
        (downside is not None and price < downside * (1 - trig))
    )
    if beyond_band:
        factor = get_split_factor(ticker, info["date"])
        if factor != 1.0:
            if upside:   upside   = round(upside   / factor, 2)
            if downside: downside = round(downside / factor, 2)
            log.info(f"  {ticker}: applied split factor {factor:.4f} → "
                     f"upside={upside}, downside={downside}")

    # ── Condition 2: price must be >=30% outside the band ─────────────────────
    reasons = []
    age_note = f"PT not updated in {age_days} days"
    if upside is not None and price > upside and (price / upside - 1) >= extreme:
        pct = (price / upside - 1) * 100
        reasons.append(f"{age_note} and price ${price:.2f} is {pct:.0f}% above upside PT ${upside:.2f}")
    elif downside is not None and price < downside and (1 - price / downside) >= extreme:
        pct = (1 - price / downside) * 100
        reasons.append(f"{age_note} and price ${price:.2f} is {pct:.0f}% below downside PT ${downside:.2f}")

    return reasons, {"upside": upside, "downside": downside}

# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def is_acknowledged(ticker: str, state: dict, info: dict) -> bool:
    """
    Returns True if analyst acknowledged AND has NOT since posted a new PT.
    Acknowledgment auto-expires once a new PT is posted (BeginDate > ack date).
    """
    ack = state["acknowledged"].get(ticker)
    if not ack:
        return False
    ack_date = date.fromisoformat(ack["date"])
    pt_date  = date.fromisoformat(info["date"])
    # If a new PT was posted AFTER acknowledgment, the ack is stale → alert again
    if pt_date > ack_date:
        log.info(f"  {ticker}: new PT posted after ack ({pt_date} > {ack_date}) — clearing ack")
        return False
    return True


def alert_status(ticker: str, state: dict, info: dict, report_date: date = None) -> str:
    """
    Returns:
      'send_first'   — never alerted, or state was cleared by a new PT / new print
      'send_second'  — first alert sent 5+ days ago, no ack, no second sent yet
      'wait'         — first alert sent < 5 days ago
      'done'         — second alert already sent
      'acknowledged' — analyst acknowledged

    The two-nudge cadence is unchanged. What re-arms it is a new TRIGGER: either
    a refreshed price target (as before) or, now, a fresh earnings print. That
    gives each quarter's print its own two nudges instead of a name going
    permanently silent after its second reminder.
    """
    if is_acknowledged(ticker, state, info):
        return "acknowledged"

    first  = state["first_alert"].get(ticker)
    second = state["second_alert"].get(ticker)

    if not first:
        return "send_first"

    first_date = date.fromisoformat(first["date"])
    pt_date    = date.fromisoformat(info["date"])
    # Most recent thing that justifies restarting the cycle.
    trigger_date = max(pt_date, report_date) if report_date else pt_date

    # If a new PT was posted, or the company reported, after our first alert → reset
    if trigger_date > first_date:
        what = "PT updated" if trigger_date == pt_date else "reported"
        log.info(f"  {ticker}: {what} after first alert — resetting state")
        return "send_first"

    if second:
        second_date = date.fromisoformat(second["date"])
        if trigger_date > second_date:
            return "send_first"   # new trigger after the second alert too
        return "done"

    days_since_first = (date.today() - first_date).days
    if days_since_first >= CONFIG["second_nudge_days"]:
        return "send_second"
    return "wait"

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def ack_link(ticker: str, analyst: str, price=None, upside=None, downside=None) -> str:
    """
    Builds a pre-filled mailto to the price-targets inbox for this ticker.
    The analyst fills in the new levels (or writes 'no change') and sends;
    ops updates the master spreadsheet from the inbox.
    """
    from urllib.parse import quote
    inbox = CONFIG["pricetargets_inbox"]
    subject = f"Price Target Update: {ticker}"
    body = (
        f"Ticker: {ticker}\n"
        f"Current price: {f'${price:.2f}' if price else ''}\n"
        f"Current target range: {f'${downside:.2f}' if downside else '-'} (downside) / "
        f"{f'${upside:.2f}' if upside else '-'} (upside)\n\n"
        f"New Upside PT: \n"
        f"New Downside PT: \n\n"
        f"(Or reply 'no change' to confirm the current target still stands and pause reminders.)"
    )
    return f"mailto:{inbox}?subject={quote(subject)}&body={quote(body)}"


def build_analyst_email(analyst_code: str, alerts: list, is_second: bool) -> str:
    analyst_name_map = {
        "JC": "James", "SS": "Saaheb",
        "BT": "Brian",  "MS": "Mit", "BK": "Bronson",
    }
    first_name = analyst_name_map.get(analyst_code, analyst_code)
    nudge_label = "Second reminder" if is_second else "Action required"
    subject_flag = "⚠️ " if is_second else ""
    today_str = date.today().strftime("%B %d, %Y")

    th = "padding:8px 12px;text-align:left;font-size:12px;font-weight:600;white-space:nowrap"
    td = "padding:7px 12px;font-size:13px;white-space:nowrap;border-bottom:1px solid #f0f0f0"

    rows = ""
    for a in alerts:
        reason_html = "<br>".join(a["reasons"])
        ack_url = ack_link(a["ticker"], analyst_code,
                           price=a["price"], upside=a["upside"], downside=a["downside"])
        upside_str   = f"${a['upside']:.2f}"   if a["upside"]   else "—"
        downside_str = f"${a['downside']:.2f}" if a["downside"] else "—"
        rows += (
            f"<tr>"
            f"<td style='{td}'><b>{display_ticker(a['ticker'])}</b></td>"
            f"<td style='{td};text-align:right'>${a['price']:.2f}</td>"
            f"<td style='{td};text-align:right'>{upside_str}</td>"
            f"<td style='{td};text-align:right'>{downside_str}</td>"
            f"<td style='{td};text-align:right;color:#c0392b'>{a['age_days']}d</td>"
            f"<td style='{td};color:#555;font-size:12px'>{reason_html}</td>"
            f"<td style='{td};text-align:center'>"
            f"<a href='{ack_url}' style='background:#1a3c6e;color:white;padding:4px 10px;"
            f"border-radius:4px;font-size:11px;text-decoration:none;font-weight:600'>"
            f"Email Update</a></td>"
            f"</tr>"
        )

    intro_color = "#9a3412" if is_second else "#1a3c6e"
    intro_bg    = "#fff7ed" if is_second else "#f0f4ff"

    return f"""
<div style="font-family:Arial,sans-serif;max-width:900px">

  <p style="font-size:15px;font-weight:600;color:{intro_color};margin-bottom:6px">
    {subject_flag}Contour Price Targets — {nudge_label} ({today_str})
  </p>

  <div style="background:{intro_bg};border-left:4px solid {intro_color};
              padding:10px 14px;margin-bottom:16px;font-size:13px;color:#333">
    Hi {first_name},<br><br>
    {"This is a <b>second reminder</b> — the following" if is_second else "The following"}
    price target{'s' if len(alerts) != 1 else ''} in your coverage
    {'need' if len(alerts) != 1 else 'needs'} a refresh &mdash; the company has
    reported since the target was set, or the target is stale and price has
    moved outside the range. The reason is shown per name.
    For each name, click <b>Email Update</b> to send your revised upside/downside
    (or "no change") to
    <a href="mailto:{CONFIG['pricetargets_inbox']}" style="color:{intro_color};font-weight:600">{CONFIG['pricetargets_inbox']}</a>.
    The desk updates the master price-targets file from there, and reminders stop
    automatically once the target is refreshed.
  </div>

  <table style="border-collapse:collapse;font-family:Arial,sans-serif;width:100%">
    <thead>
      <tr style="background:#1a3c6e;color:white">
        <th style="{th}">Ticker</th>
        <th style="{th};text-align:right">Price</th>
        <th style="{th};text-align:right">Upside PT</th>
        <th style="{th};text-align:right">Downside PT</th>
        <th style="{th};text-align:right">PT Age</th>
        <th style="{th}">Reason</th>
        <th style="{th};text-align:center">Action</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>

  <p style="font-size:11px;color:#aaa;margin-top:12px">
    Sent by Contour Price Target Monitor · stale_alerts.py ·
    {'TEST MODE — would normally go to ' + analyst_code if True else ''} ·
    Alerts stop automatically once a new price target entry is posted.
  </p>

</div>
"""


ANALYST_NAMES = {
    "JC": "James Collins", "SS": "Saaheb Sidana",
    "BT": "Brian Thackray", "MS": "Mit Shah", "BK": "Bronson Kussin",
}

def _mini_table(title: str, subtitle: str, colour: str, headers: list, rows: str) -> str:
    """Compact section used for the summary email's housekeeping blocks."""
    ths = "".join(
        f"<th style='padding:6px 10px;font-size:12px;text-align:"
        f"{'right' if h.startswith('>') else 'left'}'>{h.lstrip('>')}</th>"
        for h in headers
    )
    return (
        f"<p style='font-family:Arial,sans-serif;font-size:13px;font-weight:600;"
        f"color:{colour};margin-top:20px'>{title}</p>"
        f"<p style='font-family:Arial,sans-serif;font-size:12px;color:#777;margin:2px 0 8px'>"
        f"{subtitle}</p>"
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%'>"
        f"<thead><tr style='background:{colour};color:white'>{ths}</tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def build_summary_email(all_analyst_alerts: dict, skipped_no_port: list,
                        needs_analyst: list, price_errors: list,
                        corporate_actions: list, unmapped: list,
                        snapshot_warning: str, pf_meta: dict) -> str:
    """Audit-trail summary to hari: what was sent, what was excluded, what failed."""
    today_str = date.today().strftime("%B %d, %Y")
    td = "padding:6px 10px;font-size:13px;border-bottom:1px solid #f0f0f0"

    # ── Alerts sent ────────────────────────────────────────────────────────────
    rows = ""
    for analyst_code, entries in sorted(all_analyst_alerts.items()):
        for e in entries:
            rows += (
                f"<tr>"
                f"<td style='{td}'>{ANALYST_NAMES.get(analyst_code, analyst_code)}</td>"
                f"<td style='{td}'><b>{display_ticker(e['ticker'])}</b></td>"
                f"<td style='{td};text-align:right'>${e['price']:.2f}</td>"
                f"<td style='{td};color:#c0392b'>{e['age_days']}d old</td>"
                f"<td style='{td};color:#555;font-size:12px'>{'; '.join(e['reasons'])}</td>"
                f"<td style='{td}'>{e['nudge']}</td>"
                f"</tr>"
            )
    total = sum(len(v) for v in all_analyst_alerts.values())
    alerts_table = (
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%'>"
        f"<thead><tr style='background:#1a3c6e;color:white'>"
        f"<th style='padding:8px 10px;font-size:12px;text-align:left'>Analyst</th>"
        f"<th style='padding:8px 10px;font-size:12px;text-align:left'>Ticker</th>"
        f"<th style='padding:8px 10px;font-size:12px;text-align:right'>Price</th>"
        f"<th style='padding:8px 10px;font-size:12px'>PT Age</th>"
        f"<th style='padding:8px 10px;font-size:12px'>Reason</th>"
        f"<th style='padding:8px 10px;font-size:12px'>Nudge #</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
        if rows else
        "<p style='font-family:Arial,sans-serif;font-size:13px;color:#777'>"
        "<i>No stale alerts sent this run.</i></p>"
    )

    # ── SAFEGUARD 1: stale names EXCLUDED because not in 12-month portfolio ─────
    excl_html = ""
    if skipped_no_port:
        excl_rows = "".join(
            f"<tr>"
            f"<td style='{td}'><b>{display_ticker(s['ticker'])}</b></td>"
            f"<td style='{td}'>{ANALYST_NAMES.get(s['analyst'], s['analyst'])}</td>"
            f"<td style='{td};color:#c0392b'>{s['age_days']}d old</td>"
            f"<td style='{td};color:#888'>{s['pt_date']}</td>"
            f"</tr>"
            for s in sorted(skipped_no_port, key=lambda x: -x["age_days"])
        )
        excl_html = (
            f"<p style='font-family:Arial,sans-serif;font-size:13px;font-weight:600;"
            f"color:#9a3412;margin-top:20px'>Excluded — age-stale but NOT in 12-month "
            f"master portfolio ({len(skipped_no_port)})</p>"
            f"<p style='font-family:Arial,sans-serif;font-size:12px;color:#777;margin:2px 0 8px'>"
            f"These have old targets but no master-portfolio position in the trailing 12 months, "
            f"so no analyst was nudged. Listed here so nothing goes undetected — review if any "
            f"should be covered.</p>"
            f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%'>"
            f"<thead><tr style='background:#9a3412;color:white'>"
            f"<th style='padding:6px 10px;font-size:12px;text-align:left'>Ticker</th>"
            f"<th style='padding:6px 10px;font-size:12px;text-align:left'>Analyst</th>"
            f"<th style='padding:6px 10px;font-size:12px'>PT Age</th>"
            f"<th style='padding:6px 10px;font-size:12px'>PT Date</th>"
            f"</tr></thead><tbody>{excl_rows}</tbody></table>"
        )

    # ── SAFEGUARD 2: in-portfolio + stale but NO analyst assigned ──────────────
    gap_html = ""
    if needs_analyst:
        gap_rows = "".join(
            f"<tr>"
            f"<td style='{td}'><b>{display_ticker(s['ticker'])}</b></td>"
            f"<td style='{td};color:#c0392b'>{s['age_days']}d old</td>"
            f"<td style='{td};color:#888'>{s['pt_date']}</td>"
            f"</tr>"
            for s in sorted(needs_analyst, key=lambda x: -x["age_days"])
        )
        gap_html = (
            f"<p style='font-family:Arial,sans-serif;font-size:13px;font-weight:600;"
            f"color:#b45309;margin-top:20px'>Action needed — in portfolio & stale but "
            f"NO analyst assigned ({len(needs_analyst)})</p>"
            f"<p style='font-family:Arial,sans-serif;font-size:12px;color:#777;margin:2px 0 8px'>"
            f"These ARE in the 12-month master portfolio and have stale targets, but no analyst "
            f"is mapped — so no nudge was sent. Add them to analyst_map.json to start alerting.</p>"
            f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%'>"
            f"<thead><tr style='background:#b45309;color:white'>"
            f"<th style='padding:6px 10px;font-size:12px;text-align:left'>Ticker</th>"
            f"<th style='padding:6px 10px;font-size:12px'>PT Age</th>"
            f"<th style='padding:6px 10px;font-size:12px'>PT Date</th>"
            f"</tr></thead><tbody>{gap_rows}</tbody></table>"
        )

    # ── Corporate actions: covered but no longer tradeable ─────────────────────
    ca_html = ""
    if corporate_actions:
        ca_rows = "".join(
            f"<tr>"
            f"<td style='{td}'><b>{display_ticker(c['ticker'])}</b></td>"
            f"<td style='{td}'>{ANALYST_NAMES.get(c['analyst'], c['analyst'])}</td>"
            f"<td style='{td};color:#555'>{c['status']}</td>"
            f"<td style='{td};color:#888'>{c['date']}</td>"
            f"<td style='{td};color:#555;font-size:12px'>{c['note']}"
            f"{' &rarr; ' + c['successor'] if c['successor'] else ''}</td>"
            f"</tr>"
            for c in sorted(corporate_actions, key=lambda x: x["date"], reverse=True)
        )
        ca_html = _mini_table(
            f"Corporate actions — no longer tradeable ({len(corporate_actions)})",
            "These are still mapped to an analyst but cannot be priced. Remove them "
            "from analyst_map.json, or point coverage at the successor.",
            "#6b21a8",
            ["Ticker", "Analyst", "Status", "Date", "Detail"],
            ca_rows,
        )

    # ── Unmapped: covered but absent from ticker_master.json ───────────────────
    unmapped_html = ""
    if unmapped:
        um_rows = "".join(
            f"<tr><td style='{td}'><b>{u['ticker']}</b></td>"
            f"<td style='{td}'>{ANALYST_NAMES.get(u['analyst'], u['analyst'])}</td>"
            f"<td style='{td};color:#c0392b'>{u['age_days']}d</td></tr>"
            for u in sorted(unmapped, key=lambda x: -x["age_days"])
        )
        unmapped_html = _mini_table(
            f"Not in ticker_master.json ({len(unmapped)})",
            "Covered by an analyst but absent from the identifier map, so not priced. "
            "Add them via tools/gen_ticker_master.py.",
            "#0e7490",
            ["Ticker", "Analyst", ">PT Age"],
            um_rows,
        )

    # ── SAFEGUARD 3: price fetch failures ──────────────────────────────────────
    err_html = ""
    if price_errors:
        err_rows = "".join(
            f"<tr><td style='{td}'><b>{display_ticker(t)}</b></td>"
            f"<td style='{td};color:#c0392b;font-size:12px'>{err}</td></tr>"
            for t, err in price_errors
        )
        err_html = (
            f"<p style='font-family:Arial,sans-serif;font-size:13px;font-weight:600;"
            f"color:#c0392b;margin-top:20px'>Price fetch failures ({len(price_errors)})</p>"
            f"<p style='font-family:Arial,sans-serif;font-size:12px;color:#777;margin:2px 0 8px'>"
            f"Could not verify drift for these — <b>not checked, not dropped silently.</b> "
            f"Age-based staleness still applied. Review ticker mappings if persistent.</p>"
            f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%'>"
            f"<thead><tr style='background:#c0392b;color:white'>"
            f"<th style='padding:6px 10px;font-size:12px;text-align:left'>Ticker</th>"
            f"<th style='padding:6px 10px;font-size:12px;text-align:left'>Reason</th>"
            f"</tr></thead><tbody>{err_rows}</tbody></table>"
        )

    # ── Membership snapshot banner ─────────────────────────────────────────────
    snap_banner = ""
    if snapshot_warning:
        snap_banner = (
            f"<p style='font-family:Arial,sans-serif;font-size:12px;background:#fff7ed;"
            f"color:#9a3412;padding:8px 12px;border-left:4px solid #f97316;margin:8px 0'>"
            f"⚠️ {snapshot_warning}</p>"
        )

    return f"""
<p style="font-family:Arial,sans-serif;font-size:15px;font-weight:600;color:#1a3c6e">
  Stale PT Alert Run — {today_str}
  {'<span style="background:#f97316;color:white;font-size:11px;padding:2px 8px;border-radius:3px;margin-left:8px">TEST MODE</span>' if CONFIG['test_mode'] else ''}
</p>
<p style="font-family:Arial,sans-serif;font-size:13px;color:#555">
  {total} stale alert(s) across {len(all_analyst_alerts)} analyst(s).
  {'In test mode — all analyst emails routed to <b>' + CONFIG['test_email'] + '</b>.' if CONFIG['test_mode'] else ''}
  Portfolio membership: 12-month master snapshot generated {pf_meta.get('generated', '?')}.
</p>
{snap_banner}
{alerts_table}
{gap_html}
{err_html}
{ca_html}
{unmapped_html}
{excl_html}
<p style="font-family:Arial,sans-serif;font-size:11px;color:#aaa;margin-top:16px">
  Flag rule (both required): PT not updated in ≥ {CONFIG['stale_age_days']}d AND price {int(CONFIG['price_drift_pct']*100)}%+
  outside the target range (above the upside PT or below the downside PT). A price within the band, or a
  breakout on a target newer than {CONFIG['stale_age_days']}d, is not flagged. Prices are split-adjusted.
  Flip test_mode=False in CONFIG to send directly to analysts.
</p>
"""

# ─────────────────────────────────────────────────────────────────────────────
# SEND
# ─────────────────────────────────────────────────────────────────────────────

# Map non-ASCII glyphs to safe HTML entities so Outlook renders them correctly
# regardless of the charset Power Automate applies (fixes the "â€”" mojibake).
_HTML_ENTITIES = {
    "—": "&mdash;", "–": "&ndash;", "·": "&middot;",
    "≥": "&ge;",    "≤": "&le;",    "→": "&rarr;",
    "✓": "&#10003;", "⚠": "&#9888;", "️": "",
}
# For plain-text subject lines (entities won't render there) — use ASCII.
_SUBJECT_ASCII = {
    "—": " - ", "–": "-", "→": "->",
    "⚠": "[!]", "️": "", "≥": ">=",
}

def _to_entities(s: str) -> str:
    return "".join(_HTML_ENTITIES.get(c, c) if ord(c) > 127 else c for c in s)

def _ascii_subject(s: str) -> str:
    for k, v in _SUBJECT_ASCII.items():
        s = s.replace(k, v)
    return s.encode("ascii", "ignore").decode()


def send_email(html: str, subject: str, to_email: str):
    """
    Posts {body, subject, to} to the Power Automate flow whose trigger is
    "When a Teams webhook request is received" (standard/non-premium connector).
    The flow sends the email, routing To to the address passed here.
    """
    html    = _to_entities(html).strip()  # HTML-safe body, no stray whitespace
    subject = _ascii_subject(subject)     # ASCII-safe subject
    payload = {"body": html, "subject": subject, "to": to_email}

    url = CONFIG["power_automate_url"]
    if not url or url.startswith("PASTE_"):
        log.error("  power_automate_url not set — cannot send. "
                  "Build the flow and paste its URL into CONFIG.")
        return False
    try:
        resp = requests.post(
            url, json=payload,
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        if resp.status_code in (200, 202):
            log.info(f"  Email sent -> {to_email} (subject: {subject[:60]})")
            return True
        log.error(f"  Webhook failed: HTTP {resp.status_code} — {resp.text[:200]}")
        return False
    except Exception as e:
        log.error(f"  Send error for {to_email}: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info(f"Stale PT alert run — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"Test mode: {CONFIG['test_mode']} → emails to: {CONFIG['test_email'] if CONFIG['test_mode'] else 'analysts'}")

    tm_meta            = load_ticker_master(CONFIG["ticker_master_path"])
    targets            = load_targets(CONFIG["csv_path"])
    portfolio, pf_meta = load_portfolio_12mo(CONFIG["portfolio_12mo_path"])
    analyst_map        = load_analyst_map(CONFIG["analyst_map_path"])
    earnings, earn_meta = load_earnings_dates(CONFIG["earnings_dates_path"])
    state              = load_state(CONFIG["state_path"])

    today = date.today()

    # Warn if the Lightkeeper membership snapshot is stale
    snapshot_warning = None
    gen = pf_meta.get("generated")
    if gen:
        snap_age = (today - date.fromisoformat(gen)).days
        if snap_age > CONFIG["portfolio_snapshot_max_age_days"]:
            snapshot_warning = (f"Portfolio membership snapshot is {snap_age} days old "
                                f"(generated {gen}). Consider regenerating from Lightkeeper.")
            log.warning(snapshot_warning)

    earn_gen = earn_meta.get("generated")
    if earn_gen:
        earn_age = (today - date.fromisoformat(earn_gen)).days
        if earn_age > CONFIG["earnings_snapshot_max_age_days"]:
            w = (f"Earnings calendar snapshot is {earn_age} days old (generated "
                 f"{earn_gen}). Recent prints will be missed until it is refreshed.")
            snapshot_warning = f"{snapshot_warning} {w}" if snapshot_warning else w
            log.warning(w)

    # ── Identify stale tickers ────────────────────────────────────────────────
    price_errors      = []
    skipped_no_port   = []    # stale names dropped ONLY for not being in portfolio
    needs_analyst     = []    # IN portfolio + age-stale but no analyst assigned (gap to fill)
    corporate_actions = []    # covered names that are no longer tradeable
    unmapped          = []    # covered names with no ticker_master entry
    skipped_acked     = []
    skipped_done      = []
    skipped_wait      = []

    # Collect alerts per analyst: {analyst_code: [alert_dict, ...]}
    analyst_alerts: dict[str, list] = {}

    for ticker, info in sorted(targets.items()):
        age_stale = info["age_days"] >= CONFIG["stale_age_days"]

        # Earnings trigger is evaluated first: it follows the COVERAGE universe,
        # not portfolio membership, because a covered name can report while the
        # book holds none of it (HOOD is the standing example). When it fires it
        # overrides the portfolio filter below.
        earn_reason, report_date = earnings_reason(ticker, info, earnings, today)
        earnings_override = bool(earn_reason) and not CONFIG["earnings_requires_portfolio"]

        # ── Filter 1: Must have been in the MASTER portfolio in past 12 months ─
        in_portfolio = ticker in portfolio
        if not in_portfolio and not earnings_override:
            # SAFEGUARD: surface names that an analyst actively covers (has an
            # assignment) yet aren't in the portfolio and are age-stale — these
            # are the cases where a wrong exclusion would actually matter.
            # (Unassigned + not-in-portfolio = old watchlist noise, not shown.)
            if age_stale and analyst_map.get(ticker):
                skipped_no_port.append({
                    "ticker":   ticker,
                    "analyst":  analyst_map.get(ticker),
                    "age_days": info["age_days"],
                    "pt_date":  info["date"],
                })
            continue

        # ── Filter 2: Must have an assigned analyst ───────────────────────────
        analyst_code = analyst_map.get(ticker)
        if not analyst_code:
            # SAFEGUARD: in-portfolio + stale but unassigned → surface, don't drop.
            if age_stale:
                needs_analyst.append({
                    "ticker":   ticker,
                    "age_days": info["age_days"],
                    "pt_date":  info["date"],
                })
            continue

        # ── Filter 3: must still be tradeable ─────────────────────────────────
        # A delisted name cannot be priced, so it would otherwise reappear as a
        # price-fetch failure every week and bury the real mapping bugs. Report
        # it once, in its own section, so coverage can actually be cleaned up.
        status = ticker_status(ticker)
        if status == "unmapped":
            unmapped.append({"ticker": ticker, "analyst": analyst_code,
                             "age_days": info["age_days"]})
            log.warning(f"  {ticker}: no ticker_master entry — not priced")
            continue
        if status != "live":
            entry = resolve_ticker(ticker) or {}
            corporate_actions.append({
                "ticker":   ticker,
                "analyst":  analyst_code,
                "status":   status,
                "date":     entry.get("status_date", ""),
                "note":     entry.get("note", ""),
                "successor": entry.get("successor", ""),
            })
            log.info(f"  {ticker}: {status} ({entry.get('status_date','?')}) — excluded")
            continue

        # ── Fetch live price ──────────────────────────────────────────────────
        price, err = fetch_price(ticker)
        if price is None:
            price_errors.append((ticker, err))
            log.warning(f"  {ticker}: price fetch failed — {err}")
            continue

        # ── Check staleness (split-adjusted) ──────────────────────────────────
        reasons, adj = staleness_reasons(ticker, info, price)
        # The earnings trigger is additive: either rule alone is enough to nudge.
        if earn_reason:
            reasons = [earn_reason] + reasons
        if not reasons:
            continue   # Neither rule fired

        # ── Check alert state ─────────────────────────────────────────────────
        status = alert_status(ticker, state, info, report_date)
        log.info(f"  STALE {ticker}: status={status}, reasons={reasons}")

        if status == "acknowledged":
            skipped_acked.append(ticker)
            continue
        if status == "done":
            skipped_done.append(ticker)
            continue
        if status == "wait":
            skipped_wait.append(ticker)
            continue

        is_second = (status == "send_second")
        nudge_num = "2nd nudge" if is_second else "1st nudge"

        alert_entry = {
            "ticker":    ticker,
            "price":     price,
            "upside":    adj["upside"],
            "downside":  adj["downside"],
            "age_days":  info["age_days"],
            "reasons":   reasons,
            "nudge":     nudge_num,
            "is_second": is_second,
            "earnings":  bool(earn_reason),
        }

        analyst_alerts.setdefault(analyst_code, []).append(alert_entry)
        # NOTE: state is updated AFTER a successful send (see send section below),
        # so a failed delivery never suppresses a future nudge.

    # ── Log summary counts ────────────────────────────────────────────────────
    total_alerts = sum(len(v) for v in analyst_alerts.values())
    log.info(f"\nResults: {total_alerts} alerts | "
             f"excluded_not_in_portfolio={len(skipped_no_port)} | "
             f"in_portfolio_needs_analyst={len(needs_analyst)} | "
             f"corporate_actions={len(corporate_actions)} | "
             f"unmapped={len(unmapped)} | "
             f"acked={len(skipped_acked)} | "
             f"done={len(skipped_done)} | "
             f"waiting={len(skipped_wait)} | "
             f"price_errors={len(price_errors)}")
    if price_errors:
        log.warning(f"Price fetch errors: {price_errors}")

    # ── Send per-analyst emails (routed to test_email if test_mode) ────────────
    # State is recorded ONLY for tickers whose email actually sent, so a failed
    # delivery leaves them eligible to re-fire on the next run.
    today_str = today.isoformat()

    def record_state(alerts_batch):
        for a in alerts_batch:
            bucket = "second_alert" if a["is_second"] else "first_alert"
            state[bucket][a["ticker"]] = {"date": today_str, "analyst": a["_analyst"]}

    for analyst_code, alerts in sorted(analyst_alerts.items()):
        for a in alerts:
            a["_analyst"] = analyst_code
        first_alerts  = [a for a in alerts if not a["is_second"]]
        second_alerts = [a for a in alerts if a["is_second"]]

        to_email = CONFIG["test_email"] if CONFIG["test_mode"] else CONFIG["analyst_emails"].get(analyst_code, CONFIG["test_email"])

        if first_alerts:
            html    = build_analyst_email(analyst_code, first_alerts, is_second=False)
            subject = f"Action Required: {len(first_alerts)} Price Target(s) Need Update"
            if CONFIG["test_mode"]:
                subject = f"[TEST -> {analyst_code}] {subject}"
            if send_email(html, subject, to_email):
                record_state(first_alerts)
            else:
                log.error(f"  {analyst_code}: first-nudge email failed — state NOT updated, "
                          f"will retry next run.")

        if second_alerts:
            html    = build_analyst_email(analyst_code, second_alerts, is_second=True)
            subject = f"Second Reminder: {len(second_alerts)} Price Target(s) Still Unupdated"
            if CONFIG["test_mode"]:
                subject = f"[TEST -> {analyst_code}] {subject}"
            if send_email(html, subject, to_email):
                record_state(second_alerts)
            else:
                log.error(f"  {analyst_code}: second-nudge email failed — state NOT updated, "
                          f"will retry next run.")

    # ── Always send a run-summary to hari (audit trail + safeguards) ───────────
    summary_html = build_summary_email(
        analyst_alerts, skipped_no_port, needs_analyst,
        price_errors, corporate_actions, unmapped, snapshot_warning, pf_meta
    )
    summary_subject = f"Stale PT Alert Summary - {today.strftime('%b %d')} - {total_alerts} alert(s)"
    send_email(summary_html, summary_subject, CONFIG["test_email"])

    save_state(state, CONFIG["state_path"])
    log.info("Done.")


if __name__ == "__main__":
    run()
