#!/usr/bin/env python3
"""
Contour Price Target Monitor
- Reads Contour-Price-Targets.csv (targets set within past 2 years)
- Alerts if price is within 10% of OR has crossed upside/downside target
- 14-day RSI per ticker
- Signed % difference (not absolute value)
- One alert per ticker per calendar day
- Sends single HTML payload to Power Automate webhook at 10am ET weekdays
"""

import os
import json
import logging
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
import requests

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────

POWER_AUTOMATE_URL = "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ec83745336c243eda45b7aec12638d18/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=K-X9_sEQSPeYMwz1zq8y1wb5Fyb28bFvcicYB61F5Uo"

CSV_PATH       = "Contour-Price-Targets.csv"
ALERT_LOG      = "alerts_sent.json"
THRESHOLD      = 0.10
MAX_TARGET_AGE = 365      # 12 months

TICKER_MAP = {
    # Germany (Xetra)
    "IFXGn": "IFX.DE",   "SAPG": "SAP.DE",
    # Frankfurt
    "AG1G":  "AG1.F",
    # France (Euronext Paris)
    "PUBP":  "PUB.PA",
    # UK
    "WISEa": "WISE.L",
    # Canada
    "RCIb":  "RCI-B.TO",
    # Japan (Tokyo Stock Exchange)
    "8136":  "8136.T",  "6098": "6098.T",   "7974": "7974.T",
    "7751":  "7751.T",  "4324": "4324.T",   "6981": "6981.T",
    "6963":  "6963.T",  "6857": "6857.T",   "4661": "4661.T",
    "6594":  "6594.T",
    # Taiwan (Taiwan Stock Exchange)
    "2330":  "2330.TW",
}

SKIP = {
    "MSCHWCCH","MSCHWCHK","FLTRF","TEPRF","LOOMb","HFGG",
    "DHER","AUTOA","RMV","TKWY","PSON","TMV","SIM","ITRK",
    "WLN","ASOS","PRSM LN","JET LN","BCO",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# STEP 1: LOAD TARGETS (2-year filter)
# ─────────────────────────────────────────────────────

def load_targets(path: str) -> dict:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["BeginDate"] = pd.to_datetime(df["BeginDate"], format="mixed", dayfirst=False)
    df["Issuer"]    = df["Issuer"].astype(str).str.strip().str.upper()
    df = df.sort_values("BeginDate", ascending=False).drop_duplicates("Issuer", keep="first")

    cutoff  = date.today() - timedelta(days=MAX_TARGET_AGE)
    targets = {}
    skipped = []

    for _, row in df.iterrows():
        ticker   = row["Issuer"]
        tgt_date = row["BeginDate"].date()
        if tgt_date < cutoff:
            skipped.append(ticker)
            continue
        upside   = _to_float(row.get("Upside Price Target"))
        downside = _to_float(row.get("Downside Price Target"))
        if upside is None and downside is None:
            continue
        targets[ticker] = {
            "upside":   upside,
            "downside": downside,
            "date":     tgt_date.strftime("%m/%d/%Y"),
        }

    log.info(f"Loaded {len(targets)} tickers (skipped {len(skipped)} older than 2 years).")
    return targets


def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────
# STEP 2: FETCH PRICE + RSI
# ─────────────────────────────────────────────────────

def compute_rsi(closes: pd.Series, period: int = 14) -> float:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, float("inf"))
    rsi      = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 1)


def get_split_adjustment(ticker: str, target_date_str: str) -> float:
    """
    Returns cumulative split factor for splits that occurred after target_date.
    e.g. a 10:1 split returns 10.0, meaning targets should be divided by 10.
    Only called when % away is suspiciously large (>20%).
    """
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        from datetime import date as date_
        tgt_date = datetime.strptime(target_date_str, "%m/%d/%Y").date()
        splits   = yf.Ticker(sym).splits
        if splits is None or splits.empty:
            return 1.0
        # Only splits after the target date
        after = splits[splits.index.date > tgt_date]
        if after.empty:
            return 1.0
        factor = 1.0
        for ratio in after.values:
            factor *= ratio
        log.info(f"  {ticker}: split adjustment factor={factor:.4f} (splits after {tgt_date})")
        return factor
    except Exception as e:
        log.warning(f"{ticker}: split check failed - {e}")
        return 1.0


def fetch_data(ticker: str):
    """Returns (price, rsi, hist) or (None, None, None)."""
    if ticker in SKIP:
        return None, None, None
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        t    = yf.Ticker(sym)
        hist = t.history(period="60d", auto_adjust=True)
        if hist.empty or len(hist) < 15:
            log.warning(f"{ticker}: insufficient history.")
            return None, None, None
        price = round(float(hist["Close"].iloc[-1]), 2)
        rsi   = compute_rsi(hist["Close"])
        return price, rsi, hist
    except Exception as e:
        log.warning(f"{ticker}: fetch error - {e}")
        return None, None, None


def crossed_into_zone_this_month(hist, upside, downside, threshold) -> bool:
    """
    Returns True if the stock crossed INTO the alert zone within the past month.
    Logic: price ~1 month ago was OUTSIDE the zone, but has since entered it.
    Uses the 60-day history already fetched — no extra API call needed.

    "In zone" for upside  = price >= upside  * (1 - threshold)
    "In zone" for downside = price <= downside * (1 + threshold)
    """
    if len(hist) < 22:
        return True  # not enough history, default to showing

    try:
        # Price from ~1 month ago (22 trading days back)
        month_ago_close = float(hist["Close"].iloc[-22])

        if upside is not None:
            zone_floor = upside * (1 - threshold)
            was_outside = month_ago_close < zone_floor
            is_inside   = float(hist["Close"].iloc[-1]) >= zone_floor
            if was_outside and is_inside:
                return True

        if downside is not None:
            zone_ceil   = downside * (1 + threshold)
            was_outside = month_ago_close > zone_ceil
            is_inside   = float(hist["Close"].iloc[-1]) <= zone_ceil
            if was_outside and is_inside:
                return True

        return False
    except Exception as e:
        log.warning(f"Zone cross check failed: {e}")
        return True


# ─────────────────────────────────────────────────────
# STEP 3: DAILY DEDUP
# ─────────────────────────────────────────────────────

LOOKBACK_DAYS = 30   # Alert if ticker entered zone at any point in past 30 days


# ─────────────────────────────────────────────────────
# STEP 4: BUILD HTML TABLE
# ─────────────────────────────────────────────────────

def fmt_pct(pct: float, crossed: bool) -> str:
    if pct is None:
        return "<span style='color:#ccc'>—</span>"
    color = "#c0392b" if crossed else "#333"
    weight = "600" if crossed else "400"
    sign = "+" if pct > 0 else ""
    return f"<span style='color:{color};font-weight:{weight}'>{sign}{pct:.1f}%</span>"


def fmt_rsi(rsi) -> str:
    if rsi is None:
        return "<span style='color:#ccc'>—</span>"
    if rsi >= 70:
        return f"<span style='color:#c0392b;font-weight:400'>{rsi}</span>"
    if rsi <= 30:
        return f"<span style='color:#27ae60;font-weight:400'>{rsi}</span>"
    return str(rsi)


def fmt_pt(val) -> str:
    return f"{val:.2f}" if val is not None else "<span style='color:#ccc'>—</span>"


def build_html(alerts: list, today: str) -> str:
    if not alerts:
        return (
            f"<p style='font-family:Arial,sans-serif;font-size:14px'>"
            f"<b>Contour Price Target Alert — {today}</b><br><br>"
            f"No tickers within 10% of their upside/downside price target today.</p>"
        )

    th = "padding:7px 10px;text-align:left;font-weight:500;font-size:12px;white-space:nowrap;letter-spacing:0.3px"
    td = "padding:6px 10px;font-size:13px;white-space:nowrap;border-bottom:1px solid #f0f0f0"

    rows = ""
    for a in alerts:
        px         = a["price"]
        up_pt      = a["upside_pt"]
        dn_pt      = a["downside_pt"]
        alert_side = a["alert_side"]
        up_dist    = abs(px - up_pt) if up_pt else float("inf")
        dn_dist    = abs(px - dn_pt) if dn_pt else float("inf")
        bold_up    = up_dist <= dn_dist
        bold_dn    = dn_dist <  up_dist

        def _pt(val, bold):
            if val is None:
                return "<span style='color:#ccc'>—</span>"
            s = f"{val:.2f}"
            return f"<b>{s}</b>" if bold else s

        # Only show the relevant % column; show "--" for the other
        pct_dn_str = fmt_pct(a['pct_downside'], a['crossed'] and alert_side == 'downside')                      if alert_side == 'downside' else "<span style='color:#bbb'>--</span>"
        pct_up_str = fmt_pct(a['pct_upside'], a['crossed'] and alert_side == 'upside')                      if alert_side == 'upside' else "<span style='color:#bbb'>--</span>"

        rows += (
            f"<tr>"
            f"<td style='{td}'><b>{a['ticker']}</b></td>"
            f"<td style='{td};color:#555'>{alert_side.capitalize()}</td>"
            f"<td style='{td};text-align:right'><b>{a['price']:.2f}</b></td>"
            f"<td style='{td};text-align:right'>{_pt(dn_pt, bold_dn)}</td>"
            f"<td style='{td};text-align:right'>{pct_dn_str}</td>"
            f"<td style='{td};text-align:right'>{_pt(up_pt, bold_up)}</td>"
            f"<td style='{td};text-align:right'>{pct_up_str}</td>"
            f"<td style='{td};text-align:right'>{fmt_rsi(a['rsi'])}</td>"
            f"<td style='{td};color:#888;font-size:12px;text-align:center'>{a['target_date']}</td>"
            f"</tr>"
        )

    return (
        f"<p style='font-family:Arial,sans-serif;font-size:15px;font-weight:600;margin-bottom:10px'>"
        f"Contour Price Target Alert — {today}</p>"
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;width:100%;table-layout:auto'>"
        f"<thead>"
        f"<tr style='background:#1a3c6e;color:white'>"
        f"<th style='{th}'>Ticker</th>"
        f"<th style='{th}'>Alert</th>"
        f"<th style='{th};text-align:right'>Price</th>"
        f"<th style='{th};text-align:right'>Downside PT</th>"
        f"<th style='{th};text-align:right'>% Downside</th>"
        f"<th style='{th};text-align:right'>Upside PT</th>"
        f"<th style='{th};text-align:right'>% Upside</th>"
        f"<th style='{th};text-align:right'>RSI</th>"
        f"<th style='{th};text-align:center'>PT Date</th>"
        f"</tr>"
        f"</thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>"
        f"<p style='font-family:Arial,sans-serif;font-size:11px;color:#aaa;margin-top:8px'>"
        f"Red = crossed target &nbsp;|&nbsp; RSI &gt;70 red, &lt;30 green &nbsp;|&nbsp; "
        f"Source: Contour-Price-Targets.csv</p>"
    )


# ─────────────────────────────────────────────────────
# STEP 5: SEND VIA POWER AUTOMATE
# ─────────────────────────────────────────────────────

def send_alert(html: str):
    payload = {"body": html}
    resp = requests.post(
        POWER_AUTOMATE_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    if resp.status_code in (200, 202):
        log.info("Payload sent to Power Automate successfully.")
    else:
        log.error(f"Webhook failed: HTTP {resp.status_code} - {resp.text}")
        raise RuntimeError(f"Webhook error: {resp.status_code}")


# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────

def run():
    log.info("=" * 55)
    log.info(f"Check starting - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    targets = load_targets(CSV_PATH)

    alerts_to_send = []
    price_errors   = []

    for ticker, info in sorted(targets.items()):
        upside   = info["upside"]
        downside = info["downside"]
        tgt_date = info["date"]

        price, rsi, hist = fetch_data(ticker)

        if price is None or hist is None:
            if ticker not in SKIP:
                price_errors.append(ticker)
            continue

        # Signed % difference
        # Upside:   positive = above target (crossed), negative = approaching from below
        # Downside: negative = below target (crossed), positive = approaching from above
        pct_upside   = round((price - upside)   / upside   * 100, 1) if upside   else None
        pct_downside = round((price - downside) / downside * 100, 1) if downside else None

        # If either % is suspiciously large (>20%), check for post-target splits
        # and adjust targets accordingly
        suspicious = (
            (pct_upside   is not None and abs(pct_upside)   > 20) or
            (pct_downside is not None and abs(pct_downside) > 20)
        )
        if suspicious:
            factor = get_split_adjustment(ticker, tgt_date)
            if factor != 1.0:
                if upside:
                    upside   = round(upside   / factor, 2)
                if downside:
                    downside = round(downside / factor, 2)
                pct_upside   = round((price - upside)   / upside   * 100, 1) if upside   else None
                pct_downside = round((price - downside) / downside * 100, 1) if downside else None
                log.info(f"  {ticker}: targets adjusted for splits — up={upside} dn={downside}")

        triggered  = False
        crossed    = False
        alert_side = None

        if upside is not None:
            if price >= upside:
                triggered = True; crossed = True; alert_side = "upside"
            elif abs(pct_upside) <= THRESHOLD * 100:
                triggered = True; alert_side = "upside"

        if downside is not None:
            if price <= downside:
                triggered = True; crossed = True; alert_side = "downside"
            elif abs(pct_downside) <= THRESHOLD * 100:
                triggered = True; alert_side = alert_side or "downside"

        if triggered:
            # Only include if price crossed INTO the zone within the past month
            if not crossed_into_zone_this_month(hist, upside, downside, THRESHOLD):
                log.debug(f"  {ticker}: in zone today but crossed in over a month ago, skipping.")
                continue

            log.info(f"  ALERT {ticker}: px={price}  up={upside}  dn={downside}  RSI={rsi}")
            alerts_to_send.append({
                "ticker":       ticker,
                "price":        price,
                "upside_pt":    upside,
                "downside_pt":  downside,
                "pct_upside":   pct_upside,
                "pct_downside": pct_downside,
                "rsi":          rsi,
                "target_date":  tgt_date,
                "alert_side":   alert_side,
                "crossed":      crossed,
            })

    # Sort: crossed first by highest absolute % breach (most extreme first),
    # then approaching by closest to target
    def sort_key(a):
        pct = a["pct_upside"] if a["alert_side"] == "upside" else a["pct_downside"]
        pct = pct if pct is not None else 0
        if a["crossed"]:
            return (0, -abs(pct))   # highest absolute breach first
        else:
            return (1, abs(pct))    # closest to target first

    alerts_to_send.sort(key=sort_key)

    today = date.today().strftime("%B %d, %Y")
    html  = build_html(alerts_to_send, today)
    send_alert(html)

    log.info(
        f"Done. {len(targets)} tickers checked | "
        f"{len(alerts_to_send)} alerts | "
        f"Price errors: {price_errors if price_errors else 'none'}"
    )


if __name__ == "__main__":
    run()
