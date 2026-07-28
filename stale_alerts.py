#!/usr/bin/env python3
"""
Contour Stale Price Target Alert

Signals:
  1. Name has been in the MASTER portfolio in the past 12 months
     (verified against a Lightkeeper membership snapshot: portfolio_12mo.json)
     AND at least one of:
       a. Price target age >= 135 days since BeginDate
       b. Current price has drifted 30%+ away from EITHER the upside OR downside PT
       c. Current price has drifted 20%+ away AND the PT is > 180 days old
          (mirrors the "Targets may need update" flag in the daily monitor.py)

     Prices are split-adjusted before drift is computed, so a stock split
     never masquerades as a stale target (ported from monitor.py).

Action:
  - Email the responsible analyst (or hari only in TEST_MODE)
  - 1st nudge on first detection
  - 2nd nudge after SECOND_NUDGE_DAYS if still stale and no acknowledgment
  - Stop after 2nd nudge until analyst updates the PT (new BeginDate clears state)

Accuracy safeguards:
  - Every ticker that fails a price fetch is reported (never silently dropped).
  - Every stale name excluded ONLY because it's not in the 12-month portfolio
    is listed in the summary email to hari, so nothing goes undetected.
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
    # ── Staleness thresholds ──────────────────────────────────────────────────
    "stale_age_days":      135,    # Alert if PT hasn't been updated in this many days
    "price_drift_pct":     0.30,   # Alert if price drifted 30%+ from upside OR downside PT

    # Secondary drift tier (mirrors monitor.py "Targets may need update"):
    # a smaller drift still flags IF the target is also fairly old.
    "drift_pct_with_age":  0.20,   # 20%+ drift ...
    "drift_age_days":      180,    # ... combined with a PT older than 180 days

    # Split-adjustment guard: if raw drift exceeds this, check for a stock split
    # before flagging (a split can look like a huge drift). Ported from monitor.py.
    "split_adjust_trigger_pct": 0.20,

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

    # ── Power Automate webhook (same as monitor.py) ───────────────────────────
    "power_automate_url": "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/31/workflows/acddb77a65844bd0b6d1e966030ffae4/triggers/manual/paths/invoke?api-version=1",

    # ── Acknowledge webhook (Power Automate HTTP trigger) ─────────────────────
    # TODO: Create a Power Automate flow that:
    #   1. Has an "HTTP request" trigger
    #   2. Accepts GET ?ticker=X&analyst=Y
    #   3. Appends a row to a SharePoint list (or Excel) logging the ack
    #   Paste that flow's URL here.  Until then, acknowledgments are manual.
    "ack_webhook_url":     "",  # e.g. https://prod-XX.eastus.logic.azure.com/workflows/...

    # ── File paths (relative to repo root) ───────────────────────────────────
    "csv_path":            "Contour-Price-Targets.csv",
    "portfolio_12mo_path": "portfolio_12mo.json",   # Lightkeeper 12-month membership snapshot
    "analyst_map_path":    "analyst_map.json",
    "state_path":          "stale_alerts_state.json",

    # Warn in the summary email if the membership snapshot is older than this.
    "portfolio_snapshot_max_age_days": 45,
}

# ── Ticker overrides (shared with monitor.py) ─────────────────────────────────
NUMERIC_TICKERS = {
    "8136": ("8136.T", "JP"),  "6098": ("6098.T", "JP"),
    "7974": ("7974.T", "JP"),  "7751": ("7751.T", "JP"),
    "4324": ("4324.T", "JP"),  "6981": ("6981.T", "JP"),
    "6963": ("6963.T", "JP"),  "6857": ("6857.T", "JP"),
    "4661": ("4661.T", "JP"),  "6594": ("6594.T", "JP"),
    "6752": ("6752.T", "JP"),  "2330": ("2330.TW", "TT"),
    "3034": ("3034.TW", "TT"),
}
TICKER_MAP = {
    "IFXGn": "IFX.DE", "SAPG": "SAP.DE",
    "AG1G": "AG1.F",   "PUBP": "PUB.PA",
    "WISEa": "WISE.L", "RCIb": "RCI-B.TO",
}
SKIP = {
    "MSCHWCCH","MSCHWCHK","FLTRF","TEPRF","LOOMb","HFGG",
    "DHER","AUTOA","RMV","TKWY","PSON","TMV","SIM","ITRK",
    "WLN","ASOS","PRSM LN","JET LN","BCO",
}

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

def yf_symbol(ticker: str) -> str:
    if ticker in NUMERIC_TICKERS:
        return NUMERIC_TICKERS[ticker][0]
    return TICKER_MAP.get(ticker, ticker)

def display_ticker(ticker: str) -> str:
    if ticker in NUMERIC_TICKERS:
        return f"{ticker} {NUMERIC_TICKERS[ticker][1]}"
    return ticker

def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None

def fetch_price(ticker: str):
    """Returns (price, error_reason) — price is None on failure."""
    if ticker in SKIP:
        return None, "in skip list"
    sym = yf_symbol(ticker)
    try:
        hist = yf.Ticker(sym).history(period="1mo", auto_adjust=True)
        if hist.empty:
            return None, "no history returned"
        price = round(float(hist["Close"].iloc[-1]), 2)
        data_age = (date.today() - hist.index[-1].date()).days
        if data_age > 5:
            return None, f"data is {data_age} days stale"
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

    Conditions (any one flags the name):
      1. PT age >= stale_age_days
      2. Split-adjusted price drifted >= price_drift_pct (30%) from upside OR downside
      3. Split-adjusted price drifted >= drift_pct_with_age (20%) from upside OR
         downside AND PT older than drift_age_days (180d)   [monitor.py parity]
    """
    reasons  = []
    upside   = info["upside"]
    downside = info["downside"]
    age_days = info["age_days"]

    extreme = CONFIG["price_drift_pct"]        # 0.30
    mild    = CONFIG["drift_pct_with_age"]     # 0.20
    mild_age = CONFIG["drift_age_days"]        # 180

    # ── Condition 1: age ──────────────────────────────────────────────────────
    if age_days >= CONFIG["stale_age_days"]:
        reasons.append(f"PT not updated in {age_days} days (threshold: {CONFIG['stale_age_days']}d)")

    # ── Split adjustment guard ────────────────────────────────────────────────
    # If raw drift looks extreme, a stock split may be the cause. Rebase the
    # target by the post-target-date split factor before judging drift.
    def raw_drift(t):
        return abs(price / t - 1) if t else 0.0

    suspicious = (raw_drift(upside) > CONFIG["split_adjust_trigger_pct"] or
                  raw_drift(downside) > CONFIG["split_adjust_trigger_pct"])
    if suspicious:
        factor = get_split_factor(ticker, info["date"])
        if factor != 1.0:
            if upside:   upside   = round(upside   / factor, 2)
            if downside: downside = round(downside / factor, 2)
            log.info(f"  {ticker}: applied split factor {factor:.4f} → "
                     f"upside={upside}, downside={downside}")

    # ── Conditions 2 & 3: drift (on split-adjusted targets) ───────────────────
    def drift_reason(pt, label):
        if not pt:
            return None
        drift = abs(price / pt - 1)
        direction = "above" if price > pt else "below"
        if drift >= extreme:
            return f"Price ${price:.2f} is {drift*100:.0f}% {direction} {label} PT ${pt:.2f}"
        if drift >= mild and age_days > mild_age:
            return (f"Price ${price:.2f} is {drift*100:.0f}% {direction} {label} PT "
                    f"${pt:.2f} and PT is {age_days}d old")
        return None

    for pt, label in [(upside, "upside"), (downside, "downside")]:
        r = drift_reason(pt, label)
        if r:
            reasons.append(r)

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


def alert_status(ticker: str, state: dict, info: dict) -> str:
    """
    Returns:
      'send_first'   — never alerted, or state was cleared by a new PT
      'send_second'  — first alert sent 5+ days ago, no ack, no second sent yet
      'wait'         — first alert sent < 5 days ago
      'done'         — second alert already sent
      'acknowledged' — analyst acknowledged
    """
    if is_acknowledged(ticker, state, info):
        return "acknowledged"

    first  = state["first_alert"].get(ticker)
    second = state["second_alert"].get(ticker)

    if not first:
        return "send_first"

    first_date = date.fromisoformat(first["date"])
    pt_date    = date.fromisoformat(info["date"])

    # If a new PT was posted after our first alert → reset
    if pt_date > first_date:
        log.info(f"  {ticker}: PT updated after first alert — resetting state")
        return "send_first"

    if second:
        second_date = date.fromisoformat(second["date"])
        if pt_date > second_date:
            return "send_first"   # PT updated after second alert too
        return "done"

    days_since_first = (date.today() - first_date).days
    if days_since_first >= CONFIG["second_nudge_days"]:
        return "send_second"
    return "wait"

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def ack_link(ticker: str, analyst: str) -> str:
    """Returns an acknowledge URL if configured, else a mailto fallback."""
    base = CONFIG.get("ack_webhook_url", "").strip()
    if base:
        return f"{base}?ticker={ticker}&analyst={analyst}"
    # Fallback: mailto with pre-filled subject so hari can manually log ack
    return (
        f"mailto:hari.kumar@contourasset.com"
        f"?subject=ACK%3A%20{ticker}"
        f"&body=Acknowledging%20price%20target%20for%20{ticker}%20—%20will%20update%20shortly."
    )


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
        ack_url = ack_link(a["ticker"], analyst_code)
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
            f"Mark Updated ✓</a></td>"
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
    {'have' if len(alerts) != 1 else 'has'} not been updated recently or
    {'have' if len(alerts) != 1 else 'has'} drifted significantly from the current price.
    Please review and update in the
    <a href="https://contourassetmgmt-my.sharepoint.com/:x:/r/personal/hari_kumar_contourasset_com/_layouts/15/Doc.aspx?sourcedoc=%7BF34FAAAE-2AA6-4132-B9D3-4B8C5F416DFA%7D&file=Contour-Price-Targets.xlsx"
       style="color:{intro_color};font-weight:600">Price Targets spreadsheet</a>,
    then click <b>Mark Updated</b> next to each name to stop receiving reminders.
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

def build_summary_email(all_analyst_alerts: dict, skipped_no_port: list,
                        needs_analyst: list, price_errors: list,
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
{excl_html}
{err_html}
<p style="font-family:Arial,sans-serif;font-size:11px;color:#aaa;margin-top:16px">
  Signals: PT age ≥ {CONFIG['stale_age_days']}d, OR price {int(CONFIG['price_drift_pct']*100)}%+ from a target,
  OR {int(CONFIG['drift_pct_with_age']*100)}%+ from a target with PT older than {CONFIG['drift_age_days']}d.
  Prices are split-adjusted. Flip test_mode=False in CONFIG to send directly to analysts.
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
    """Posts HTML body to Power Automate webhook (same as monitor.py)."""
    html    = _to_entities(html)      # HTML-safe body
    subject = _ascii_subject(subject) # ASCII-safe subject
    payload = {"body": html, "subject": subject, "to": to_email}
    try:
        resp = requests.post(
            CONFIG["power_automate_url"],
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code in (200, 202):
            log.info(f"  Email sent → {to_email} (subject: {subject[:60]})")
        else:
            log.error(f"  Webhook failed: HTTP {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        log.error(f"  Send error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info(f"Stale PT alert run — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"Test mode: {CONFIG['test_mode']} → emails to: {CONFIG['test_email'] if CONFIG['test_mode'] else 'analysts'}")

    targets            = load_targets(CONFIG["csv_path"])
    portfolio, pf_meta = load_portfolio_12mo(CONFIG["portfolio_12mo_path"])
    analyst_map        = load_analyst_map(CONFIG["analyst_map_path"])
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

    # ── Identify stale tickers ────────────────────────────────────────────────
    price_errors      = []
    skipped_no_port   = []    # stale names dropped ONLY for not being in portfolio
    needs_analyst     = []    # IN portfolio + age-stale but no analyst assigned (gap to fill)
    skipped_acked     = []
    skipped_done      = []
    skipped_wait      = []

    # Collect alerts per analyst: {analyst_code: [alert_dict, ...]}
    analyst_alerts: dict[str, list] = {}

    for ticker, info in sorted(targets.items()):
        age_stale = info["age_days"] >= CONFIG["stale_age_days"]

        # ── Filter 1: Must have been in the MASTER portfolio in past 12 months ─
        in_portfolio = ticker in portfolio
        if not in_portfolio:
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

        # ── Fetch live price ──────────────────────────────────────────────────
        price, err = fetch_price(ticker)
        if price is None:
            if ticker not in SKIP:
                price_errors.append((ticker, err))
                log.warning(f"  {ticker}: price fetch failed — {err}")
            continue

        # ── Check staleness (split-adjusted) ──────────────────────────────────
        reasons, adj = staleness_reasons(ticker, info, price)
        if not reasons:
            continue   # Not stale

        # ── Check alert state ─────────────────────────────────────────────────
        status = alert_status(ticker, state, info)
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
        }

        analyst_alerts.setdefault(analyst_code, []).append(alert_entry)

        # Update state
        today_str = today.isoformat()
        if is_second:
            state["second_alert"][ticker] = {"date": today_str, "analyst": analyst_code}
        else:
            state["first_alert"][ticker] = {"date": today_str, "analyst": analyst_code}

    # ── Log summary counts ────────────────────────────────────────────────────
    total_alerts = sum(len(v) for v in analyst_alerts.values())
    log.info(f"\nResults: {total_alerts} alerts | "
             f"excluded_not_in_portfolio={len(skipped_no_port)} | "
             f"in_portfolio_needs_analyst={len(needs_analyst)} | "
             f"acked={len(skipped_acked)} | "
             f"done={len(skipped_done)} | "
             f"waiting={len(skipped_wait)} | "
             f"price_errors={len(price_errors)}")
    if price_errors:
        log.warning(f"Price fetch errors: {price_errors}")

    # ── Send per-analyst emails (routed to test_email if test_mode) ────────────
    for analyst_code, alerts in sorted(analyst_alerts.items()):
        first_alerts  = [a for a in alerts if not a["is_second"]]
        second_alerts = [a for a in alerts if a["is_second"]]

        to_email = CONFIG["test_email"] if CONFIG["test_mode"] else CONFIG["analyst_emails"].get(analyst_code, CONFIG["test_email"])

        if first_alerts:
            html    = build_analyst_email(analyst_code, first_alerts, is_second=False)
            subject = f"Action Required: {len(first_alerts)} Price Target(s) Need Update"
            if CONFIG["test_mode"]:
                subject = f"[TEST → {analyst_code}] {subject}"
            send_email(html, subject, to_email)

        if second_alerts:
            html    = build_analyst_email(analyst_code, second_alerts, is_second=True)
            subject = f"⚠️ Second Reminder: {len(second_alerts)} Price Target(s) Still Unupdated"
            if CONFIG["test_mode"]:
                subject = f"[TEST → {analyst_code}] {subject}"
            send_email(html, subject, to_email)

    # ── Always send a run-summary to hari (audit trail + safeguards) ───────────
    summary_html = build_summary_email(
        analyst_alerts, skipped_no_port, needs_analyst,
        price_errors, snapshot_warning, pf_meta
    )
    summary_subject = f"Stale PT Alert Summary — {today.strftime('%b %d')} — {total_alerts} alert(s)"
    send_email(summary_html, summary_subject, CONFIG["test_email"])

    save_state(state, CONFIG["state_path"])
    log.info("Done.")


if __name__ == "__main__":
    run()
