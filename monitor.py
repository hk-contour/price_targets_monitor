#!/usr/bin/env python3
"""
Contour Price Target Monitor
- Reads Contour-Price-Targets.csv (targets set within past 12 months)
- Reads Contour-Portfolio-Delta-Adjusted.xlsx (current Longs/Shorts)
- Alerts if price is within 10% of OR has crossed upside/downside target
- 14-day RSI per ticker (integer)
- Only includes names that crossed INTO the zone within the past month
- Output is split into Portfolio Alerts (Shorts then Longs) and Non-portfolio Alerts
- Sends single HTML email via Power Automate webhook at 10am ET weekdays
"""

import os
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
PORTFOLIO_PATH = "Contour-Portfolio-Delta-Adjusted.xlsx"
THRESHOLD      = 0.10
MAX_TARGET_AGE = 365      # 12 months
STALE_PCT      = 30       # if abs(% away) > this, mark "Targets may need update"

TICKER_MAP = {
    "IFXGn": "IFX.DE",   "SAPG": "SAP.DE",
    "AG1G":  "AG1.F",
    "PUBP":  "PUB.PA",
    "WISEa": "WISE.L",
    "RCIb":  "RCI-B.TO",
    "8136":  "8136.T",  "6098": "6098.T",   "7974": "7974.T",
    "7751":  "7751.T",  "4324": "4324.T",   "6981": "6981.T",
    "6963":  "6963.T",  "6857": "6857.T",   "4661": "4661.T",
    "6594":  "6594.T",
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
# STEP 1: LOAD TARGETS (12 month filter)
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

    log.info(f"Loaded {len(targets)} tickers (skipped {len(skipped)} older than 12 months).")
    return targets


def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────
# STEP 2: LOAD PORTFOLIO HOLDINGS
# ─────────────────────────────────────────────────────

def load_portfolio(path: str) -> dict:
    """
    Returns { ticker: 'Long' | 'Short' } from the Lightkeeper-format Excel.
    Longs are in column 1 (issuer) and 2 (weight).
    Shorts are in column 5 (issuer) and 6 (weight).
    """
    if not os.path.exists(path):
        log.warning(f"Portfolio file not found: {path}. Continuing without portfolio data.")
        return {}

    try:
        df = pd.read_excel(path, header=None)
        portfolio = {}

        # Longs: col 1 is Issuer, starting from row index 10 (skip header rows)
        for _, row in df.iloc[10:].iterrows():
            t = row[1]
            if pd.notna(t) and str(t).strip() and str(t).strip().lower() != "issuer":
                portfolio[str(t).strip().upper()] = "Long"

        # Shorts: col 5 is Issuer
        for _, row in df.iloc[10:].iterrows():
            t = row[5]
            if pd.notna(t) and str(t).strip() and str(t).strip().lower() != "issuer":
                portfolio[str(t).strip().upper()] = "Short"

        longs  = sum(1 for v in portfolio.values() if v == "Long")
        shorts = sum(1 for v in portfolio.values() if v == "Short")
        log.info(f"Loaded portfolio: {longs} longs, {shorts} shorts.")
        return portfolio
    except Exception as e:
        log.warning(f"Failed to load portfolio: {e}")
        return {}


# ─────────────────────────────────────────────────────
# STEP 3: FETCH PRICE + RSI
# ─────────────────────────────────────────────────────

def compute_rsi(closes: pd.Series, period: int = 14) -> int:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, float("inf"))
    rsi      = 100 - (100 / (1 + rs))
    return int(round(float(rsi.iloc[-1])))


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
    """Returns True if price 22 trading days ago was outside the zone but now is inside."""
    if len(hist) < 22:
        return True
    try:
        month_ago_close = float(hist["Close"].iloc[-22])
        today_close     = float(hist["Close"].iloc[-1])
        if upside is not None:
            zone_floor = upside * (1 - threshold)
            if month_ago_close < zone_floor and today_close >= zone_floor:
                return True
        if downside is not None:
            zone_ceil = downside * (1 + threshold)
            if month_ago_close > zone_ceil and today_close <= zone_ceil:
                return True
        return False
    except Exception as e:
        log.warning(f"Zone cross check failed: {e}")
        return True


def get_split_adjustment(ticker: str, target_date_str: str) -> float:
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        tgt_date = datetime.strptime(target_date_str, "%m/%d/%Y").date()
        splits   = yf.Ticker(sym).splits
        if splits is None or splits.empty:
            return 1.0
        after = splits[splits.index.date > tgt_date]
        if after.empty:
            return 1.0
        factor = 1.0
        for ratio in after.values:
            factor *= ratio
        log.info(f"  {ticker}: split factor={factor:.4f}")
        return factor
    except Exception as e:
        log.warning(f"{ticker}: split check failed - {e}")
        return 1.0


# ─────────────────────────────────────────────────────
# STEP 4: REASON FOR FLAG
# ─────────────────────────────────────────────────────

def reason_for_flag(alert: dict) -> str:
    """Auto-generate the 'Reason for flag' text."""
    p = alert.get("portfolio")  # 'Long' / 'Short' / None
    side    = alert["alert_side"]
    crossed = alert["crossed"]
    pct     = alert["pct_upside"] if side == "upside" else alert["pct_downside"]
    pct_abs = abs(pct) if pct is not None else 0

    # Stale targets — extreme % away
    if pct_abs > STALE_PCT:
        return "*Targets may need update"

    # Portfolio names
    if p == "Short":
        if side == "upside" and crossed:
            return "Short is above upside"
        if side == "upside" and pct_abs <= 7:
            return "Short is near upside"

    if p == "Long":
        if side == "upside" and crossed:
            return "Long above upside"
        if side == "downside" and pct_abs <= 7:
            return "Long near downside"
        if side == "downside" and crossed:
            return "Long below downside"

    # Non-portfolio
    if not p:
        if side == "upside" and crossed:
            return "Above upside"
        if side == "downside" and crossed:
            return "Below downside"

    return ""


# ─────────────────────────────────────────────────────
# STEP 5: BUILD HTML
# ─────────────────────────────────────────────────────

def fmt_pct(pct, crossed):
    if pct is None:
        return "<span style='color:#ccc'>—</span>"
    color = "#c0392b" if crossed else "#333"
    weight = "600" if crossed else "400"
    sign = "+" if pct > 0 else ""
    return f"<span style='color:{color};font-weight:{weight}'>{sign}{pct:.1f}%</span>"


def fmt_rsi(rsi):
    if rsi is None:
        return "<span style='color:#ccc'>—</span>"
    if rsi >= 70:
        return f"<span style='color:#c0392b'>{rsi}</span>"
    if rsi <= 30:
        return f"<span style='color:#27ae60'>{rsi}</span>"
    return str(rsi)


def render_row(a, td):
    """Render a single alert row."""
    px      = a["price"]
    up_pt   = a["upside_pt"]
    dn_pt   = a["downside_pt"]
    side    = a["alert_side"]
    up_dist = abs(px - up_pt) if up_pt else float("inf")
    dn_dist = abs(px - dn_pt) if dn_pt else float("inf")
    bold_up = up_dist <= dn_dist
    bold_dn = dn_dist <  up_dist

    def _pt(val, bold):
        if val is None:
            return "<span style='color:#ccc'>—</span>"
        s = f"{val:.2f}"
        return f"<b>{s}</b>" if bold else s

    pct_dn_str = fmt_pct(a["pct_downside"], a["crossed"] and side == "downside") \
                 if side == "downside" else "<span style='color:#bbb'>--</span>"
    pct_up_str = fmt_pct(a["pct_upside"],   a["crossed"] and side == "upside") \
                 if side == "upside" else "<span style='color:#bbb'>--</span>"

    portfolio_str = a.get("portfolio") or ""
    reason_str    = a.get("reason") or ""

    return (
        f"<tr>"
        f"<td style='{td}'><b>{a['ticker']}</b></td>"
        f"<td style='{td};color:#555'>{portfolio_str}</td>"
        f"<td style='{td};color:#555'>{side.capitalize()}</td>"
        f"<td style='{td};text-align:right'><b>{px:.2f}</b></td>"
        f"<td style='{td};text-align:right'>{_pt(dn_pt, bold_dn)}</td>"
        f"<td style='{td};text-align:right'>{pct_dn_str}</td>"
        f"<td style='{td};text-align:right'>{_pt(up_pt, bold_up)}</td>"
        f"<td style='{td};text-align:right'>{pct_up_str}</td>"
        f"<td style='{td};text-align:right'>{fmt_rsi(a['rsi'])}</td>"
        f"<td style='{td};color:#888;font-size:12px;text-align:center'>{a['target_date']}</td>"
        f"<td style='{td};color:#555;font-size:12px'>{reason_str}</td>"
        f"</tr>"
    )


def render_section_header(title, td):
    return (
        f"<tr><td colspan='11' style='padding:14px 6px 6px 0;"
        f"font-family:Arial,sans-serif;font-size:14px;font-weight:600;color:#1a3c6e'>"
        f"{title}</td></tr>"
    )


def render_blank_row(td):
    return f"<tr><td colspan='11' style='padding:4px 0'></td></tr>"


def build_html(portfolio_alerts: list, non_portfolio_alerts: list, today: str) -> str:
    if not portfolio_alerts and not non_portfolio_alerts:
        return (
            f"<p style='font-family:Arial,sans-serif;font-size:14px'>"
            f"<b>Contour Price Target Alert — {today}</b><br><br>"
            f"No tickers within 10% of their upside/downside price target today.</p>"
        )

    th = "padding:6px 10px;text-align:left;font-weight:500;font-size:12px;white-space:nowrap"
    td = "padding:5px 10px;font-size:13px;white-space:nowrap;border-bottom:1px solid #f0f0f0"
    col_widths = [55, 65, 60, 60, 80, 75, 75, 70, 40, 75, 180]
    colgroup = "".join(f"<col style='min-width:{w}px;width:{w}px'>" for w in col_widths)

    body_rows = ""

    # Portfolio Alerts section
    if portfolio_alerts:
        body_rows += render_section_header("Portfolio Alerts", td)
        # Shorts first, then Longs; within each, sort by % upside descending (crossed first)
        shorts = [a for a in portfolio_alerts if a.get("portfolio") == "Short"]
        longs  = [a for a in portfolio_alerts if a.get("portfolio") == "Long"]

        def port_sort(a):
            pct = a["pct_upside"] if a["alert_side"] == "upside" else a["pct_downside"]
            pct = pct if pct is not None else 0
            return -pct  # most positive first

        for a in sorted(shorts, key=port_sort):
            body_rows += render_row(a, td)
        if shorts and longs:
            body_rows += render_blank_row(td)
        for a in sorted(longs, key=port_sort):
            body_rows += render_row(a, td)

    # Non-portfolio Alerts section
    if non_portfolio_alerts:
        if portfolio_alerts:
            body_rows += render_blank_row(td)
        body_rows += render_section_header("Non-portfolio Alerts", td)

        # Sub-groups
        stale       = [a for a in non_portfolio_alerts if a.get("reason") == "*Targets may need update"]
        upside_x    = [a for a in non_portfolio_alerts if a not in stale and a["alert_side"] == "upside"  and a["crossed"]]
        upside_app  = [a for a in non_portfolio_alerts if a not in stale and a["alert_side"] == "upside"  and not a["crossed"]]
        downside_x  = [a for a in non_portfolio_alerts if a not in stale and a["alert_side"] == "downside" and a["crossed"]]
        downside_app= [a for a in non_portfolio_alerts if a not in stale and a["alert_side"] == "downside" and not a["crossed"]]

        # Stale first, then crossed by extremity, then approaching by closest
        groups = []
        if stale:
            groups.append(sorted(stale,
                                 key=lambda a: -abs(a["pct_upside"] if a["alert_side"]=="upside" else a["pct_downside"])))
        if upside_x:
            groups.append(sorted(upside_x, key=lambda a: -a["pct_upside"]))
        if upside_app:
            groups.append(sorted(upside_app, key=lambda a: abs(a["pct_upside"])))
        if downside_x:
            groups.append(sorted(downside_x, key=lambda a: a["pct_downside"]))
        if downside_app:
            groups.append(sorted(downside_app, key=lambda a: abs(a["pct_downside"])))

        for i, g in enumerate(groups):
            if i > 0:
                body_rows += render_blank_row(td)
            for a in g:
                body_rows += render_row(a, td)

    return (
        f"<p style='font-family:Arial,sans-serif;font-size:15px;font-weight:600;margin-bottom:10px'>"
        f"Contour Price Target Alert — {today}</p>"
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;table-layout:fixed'>"
        f"<colgroup>{colgroup}</colgroup>"
        f"<thead>"
        f"<tr style='background:#1a3c6e;color:white'>"
        f"<th style='{th}'>Ticker</th>"
        f"<th style='{th}'>Portfolio</th>"
        f"<th style='{th}'>Alert</th>"
        f"<th style='{th};text-align:right'>Price</th>"
        f"<th style='{th};text-align:right'>Downside PT</th>"
        f"<th style='{th};text-align:right'>% Downside</th>"
        f"<th style='{th};text-align:right'>Upside PT</th>"
        f"<th style='{th};text-align:right'>% Upside</th>"
        f"<th style='{th};text-align:right'>RSI</th>"
        f"<th style='{th};text-align:center'>PT Date</th>"
        f"<th style='{th}'>Reason for flag</th>"
        f"</tr>"
        f"</thead>"
        f"<tbody>{body_rows}</tbody>"
        f"</table>"
        f"<p style='font-family:Arial,sans-serif;font-size:11px;color:#aaa;margin-top:8px'>"
        f"Red = crossed target | RSI &gt;70 red, &lt;30 green | "
        f"Source: Contour-Price-Targets.csv + Contour-Portfolio-Delta-Adjusted.xlsx</p>"
    )


# ─────────────────────────────────────────────────────
# STEP 6: SEND VIA POWER AUTOMATE
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

    targets   = load_targets(CSV_PATH)
    portfolio = load_portfolio(PORTFOLIO_PATH)

    portfolio_alerts     = []
    non_portfolio_alerts = []
    price_errors         = []

    for ticker, info in sorted(targets.items()):
        upside   = info["upside"]
        downside = info["downside"]
        tgt_date = info["date"]

        price, rsi, hist = fetch_data(ticker)

        if price is None or hist is None:
            if ticker not in SKIP:
                price_errors.append(ticker)
            continue

        # Signed % differences
        pct_upside   = round((price - upside)   / upside   * 100, 1) if upside   else None
        pct_downside = round((price - downside) / downside * 100, 1) if downside else None

        # Split adjustment if extreme
        suspicious = (
            (pct_upside   is not None and abs(pct_upside)   > 20) or
            (pct_downside is not None and abs(pct_downside) > 20)
        )
        if suspicious:
            factor = get_split_adjustment(ticker, tgt_date)
            if factor != 1.0:
                if upside:   upside   = round(upside   / factor, 2)
                if downside: downside = round(downside / factor, 2)
                pct_upside   = round((price - upside)   / upside   * 100, 1) if upside   else None
                pct_downside = round((price - downside) / downside * 100, 1) if downside else None
                log.info(f"  {ticker}: targets adjusted up={upside} dn={downside}")

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
            # Only include if crossed INTO zone in past month
            if not crossed_into_zone_this_month(hist, upside, downside, THRESHOLD):
                log.debug(f"  {ticker}: in zone but crossed in over a month ago, skipping.")
                continue

            port = portfolio.get(ticker)  # 'Long', 'Short', or None
            alert = {
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
                "portfolio":    port,
            }
            alert["reason"] = reason_for_flag(alert)

            if port:
                portfolio_alerts.append(alert)
            else:
                non_portfolio_alerts.append(alert)

            log.info(f"  ALERT {ticker}: px={price} side={alert_side} crossed={crossed} "
                     f"port={port or '-'} reason='{alert['reason']}'")

    today = date.today().strftime("%B %d, %Y")
    html  = build_html(portfolio_alerts, non_portfolio_alerts, today)
    send_alert(html)

    log.info(
        f"Done. {len(targets)} tickers checked | "
        f"{len(portfolio_alerts)} portfolio + {len(non_portfolio_alerts)} non-portfolio alerts | "
        f"Price errors: {price_errors if price_errors else 'none'}"
    )


if __name__ == "__main__":
    run()
