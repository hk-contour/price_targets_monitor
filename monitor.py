#!/usr/bin/env python3
"""
Contour Price Target Monitor

% Convention (trader's view):
  % Upside   = (Upside - Price) / Price * 100
    → positive while price is BELOW upside (room to go up / risk to a short)
    → negative once price has CROSSED above upside (target hit / thesis broken / stale)
  % Downside = (Price - Downside) / Downside * 100
    → positive while price is ABOVE downside (cushion for a long)
    → negative once price has CROSSED below downside (downside hit / thesis broken / stale)

Both % columns are always shown.
Reason for flag conveys what's happening directionally.
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

CONFIG = {
    "csv_path":           "Contour-Price-Targets.csv",
    "portfolio_path":     "Contour_Portfolio_Delta_Adjusted.xlsx",
    "power_automate_url": "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ec83745336c243eda45b7aec12638d18/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=K-X9_sEQSPeYMwz1zq8y1wb5Fyb28bFvcicYB61F5Uo",

    "threshold_pct":              0.10,
    "max_target_age_days":        365,
    "lookback_trading_days":      22,
    "split_adjust_trigger_pct":   20,

    "stale_pct_extreme":          30,
    "stale_pct_with_age":         20,
    "stale_pt_age_days":          180,

    "near_pct":                   7,

    "rsi_overbought":             70,
    "rsi_oversold":               30,
}

# Numeric ticker -> (yfinance symbol, display suffix)
NUMERIC_TICKERS = {
    "8136": ("8136.T", "JP"),
    "6098": ("6098.T", "JP"),
    "7974": ("7974.T", "JP"),
    "7751": ("7751.T", "JP"),
    "4324": ("4324.T", "JP"),
    "6981": ("6981.T", "JP"),
    "6963": ("6963.T", "JP"),
    "6857": ("6857.T", "JP"),
    "4661": ("4661.T", "JP"),
    "6594": ("6594.T", "JP"),
    "6752": ("6752.T", "JP"),
    "2330": ("2330.TW", "TT"),
    "3034": ("3034.TW", "TT"),   # Novatek Microelectronics — Taiwan, NOT 3034.T (Japan pharmacy)
}

# Non-numeric tickers needing yfinance symbol mapping
TICKER_MAP = {
    "IFXGn": "IFX.DE",
    "SAPG":  "SAP.DE",
    "AG1G":  "AG1.F",
    "PUBP":  "PUB.PA",
    "WISEa": "WISE.L",
    "RCIb":  "RCI-B.TO",
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
# HELPERS
# ─────────────────────────────────────────────────────

def normalize_ticker(raw: str) -> str:
    """Strip any space-separated suffix so '6857 JT', '6857 JP', '6857' all become '6857'."""
    if not raw:
        return ""
    return str(raw).strip().upper().split()[0]


def display_ticker(ticker: str) -> str:
    """Format for display. Numeric tickers get region suffix: '6857 JP'."""
    if ticker in NUMERIC_TICKERS:
        return f"{ticker} {NUMERIC_TICKERS[ticker][1]}"
    return ticker


def yf_symbol(ticker: str) -> str:
    """Resolve to the yfinance symbol."""
    if ticker in NUMERIC_TICKERS:
        return NUMERIC_TICKERS[ticker][0]
    return TICKER_MAP.get(ticker, ticker)


def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────
# LOAD TARGETS
# ─────────────────────────────────────────────────────

def load_targets(path: str) -> dict:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["BeginDate"] = pd.to_datetime(df["BeginDate"], format="mixed", dayfirst=False)
    df["Issuer"]    = df["Issuer"].astype(str).map(normalize_ticker)
    df = df.sort_values("BeginDate", ascending=False).drop_duplicates("Issuer", keep="first")

    cutoff  = date.today() - timedelta(days=CONFIG["max_target_age_days"])
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


# ─────────────────────────────────────────────────────
# LOAD PORTFOLIO
# ─────────────────────────────────────────────────────

def load_portfolio(path: str) -> dict:
    if not os.path.exists(path):
        log.warning(f"Portfolio file not found: {path}.")
        return {}
    try:
        df = pd.read_excel(path, header=None)
        portfolio = {}
        for _, row in df.iloc[10:].iterrows():
            t = row[1]
            if pd.notna(t) and str(t).strip() and str(t).strip().lower() != "issuer":
                portfolio[normalize_ticker(t)] = "Long"
        for _, row in df.iloc[10:].iterrows():
            t = row[5]
            if pd.notna(t) and str(t).strip() and str(t).strip().lower() != "issuer":
                portfolio[normalize_ticker(t)] = "Short"
        longs  = sum(1 for v in portfolio.values() if v == "Long")
        shorts = sum(1 for v in portfolio.values() if v == "Short")
        log.info(f"Loaded portfolio: {longs} longs, {shorts} shorts.")
        return portfolio
    except Exception as e:
        log.warning(f"Failed to load portfolio: {e}")
        return {}


# ─────────────────────────────────────────────────────
# FETCH DATA
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
    if ticker in SKIP:
        return None, None, None
    sym = yf_symbol(ticker)
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
    if len(hist) < CONFIG["lookback_trading_days"]:
        return True
    try:
        month_ago_close = float(hist["Close"].iloc[-CONFIG["lookback_trading_days"]])
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
    sym = yf_symbol(ticker)
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
# REASON FOR FLAG
# ─────────────────────────────────────────────────────

def reason_for_flag(alert: dict) -> str:
    p           = alert.get("portfolio")
    side        = alert["alert_side"]
    crossed     = alert["crossed"]
    pct         = alert["pct_upside_signed"] if side == "upside" else alert["pct_downside_signed"]
    pct_abs     = abs(pct) if pct is not None else 0
    pt_age_days = alert.get("pt_age_days", 0)

    if pct_abs > CONFIG["stale_pct_extreme"] or (pct_abs > CONFIG["stale_pct_with_age"] and pt_age_days > CONFIG["stale_pt_age_days"]):
        return "Targets may need update"

    if p == "Short" and side == "upside":
        if crossed:
            return "Short above upside"
        if pct_abs <= CONFIG["near_pct"]:
            return "Short near upside"

    if p == "Long":
        if side == "upside" and crossed:
            return "Above upside"
        if side == "downside":
            if crossed:
                return "Long below downside"
            if pct_abs <= CONFIG["near_pct"]:
                return "Long near downside"

    if not p:
        if side == "upside" and crossed:
            return "Above upside"
        if side == "downside" and crossed:
            return "Below downside"

    return ""


# ─────────────────────────────────────────────────────
# HTML BUILDERS
# ─────────────────────────────────────────────────────

def fmt_pct_trader(pct, is_active_side):
    """
    Trader's view sign: positive = room to go (long-friendly cushion / short-side risk remaining),
    negative = already crossed (target hit / stale).
    Bold only when this is the active alert side.
    """
    if pct is None:
        return "<span style='color:#ccc'>—</span>"
    color  = "#c0392b" if pct < 0 else "#333"
    weight = "700"     if is_active_side else "400"
    sign   = "+" if pct > 0 else ""
    return f"<span style='color:{color};font-weight:{weight}'>{sign}{pct:.1f}%</span>"


def fmt_rsi(rsi):
    if rsi is None:
        return "<span style='color:#ccc'>—</span>"
    if rsi >= CONFIG["rsi_overbought"]:
        return f"<span style='color:#c0392b'>{rsi}</span>"
    if rsi <= CONFIG["rsi_oversold"]:
        return f"<span style='color:#27ae60'>{rsi}</span>"
    return str(rsi)


def fmt_pt(val):
    if val is None:
        return "<span style='color:#ccc'>—</span>"
    return f"{val:.2f}"


def render_row(a, td):
    side = a["alert_side"]
    pct_up = fmt_pct_trader(a["pct_upside_trader"],  side == "upside")
    pct_dn = fmt_pct_trader(a["pct_downside_trader"], side == "downside")

    portfolio_str = a.get("portfolio") or ""
    reason_str    = a.get("reason") or ""
    age_days      = a.get("pt_age_days", 0)

    return (
        f"<tr>"
        f"<td style='{td}'><b>{display_ticker(a['ticker'])}</b></td>"
        f"<td style='{td};color:#555'>{portfolio_str}</td>"
        f"<td style='{td};color:#555'>{side.capitalize()}</td>"
        f"<td style='{td};text-align:right'>{a['price']:.2f}</td>"
        f"<td style='{td};text-align:right'>{fmt_pt(a['downside_pt'])}</td>"
        f"<td style='{td};text-align:right'>{pct_dn}</td>"
        f"<td style='{td};text-align:right'>{fmt_pt(a['upside_pt'])}</td>"
        f"<td style='{td};text-align:right'>{pct_up}</td>"
        f"<td style='{td};text-align:right'>{fmt_rsi(a['rsi'])}</td>"
        f"<td style='{td};text-align:right;color:#555;font-size:12px'>{age_days}d</td>"
        f"<td style='{td};color:#888;font-size:12px;text-align:center'>{a['target_date']}</td>"
        f"<td style='{td};color:#555;font-size:12px'>{reason_str}</td>"
        f"</tr>"
    )


def render_section_header(title, td):
    return (
        f"<tr><td colspan='12' style='padding:18px 6px 8px 0;"
        f"font-family:Arial,sans-serif;font-size:18px;font-weight:700;color:#1a3c6e'>"
        f"{title}</td></tr>"
    )


def render_subheader(title, td):
    return (
        f"<tr><td colspan='12' style='padding:10px 6px 4px 0;"
        f"font-family:Arial,sans-serif;font-size:13px;font-weight:600;color:#555'>"
        f"{title}</td></tr>"
    )


def render_blank_row(td):
    return f"<tr><td colspan='12' style='padding:4px 0'></td></tr>"


def sort_within_group(alerts, prefer_upside_first=True):
    """
    Sort: most actionable (most broken/crossed = most negative %) at top,
    least actionable (most cushion = most positive %) at bottom.
    Within Upside-side group then Downside-side group.
    """
    upside_alerts   = [a for a in alerts if a["alert_side"] == "upside"]
    downside_alerts = [a for a in alerts if a["alert_side"] == "downside"]

    # Most negative (crossed/broken) at top, most positive (cushion) at bottom
    upside_alerts.sort(key=lambda a: a["pct_upside_trader"] if a["pct_upside_trader"] is not None else 999)
    downside_alerts.sort(key=lambda a: a["pct_downside_trader"] if a["pct_downside_trader"] is not None else 999)

    if prefer_upside_first:
        return upside_alerts + downside_alerts
    return downside_alerts + upside_alerts


def build_html(portfolio_alerts, non_portfolio_alerts, today):
    if not portfolio_alerts and not non_portfolio_alerts:
        return (
            f"<p style='font-family:Arial,sans-serif;font-size:14px'>"
            f"<b>Contour Price Target Alert — {today}</b><br><br>"
            f"No tickers within 10% of their upside/downside price target today.</p>"
        )

    no_port_banner = ""
    if not portfolio_alerts:
        no_port_banner = (
            f"<p style='font-family:Arial,sans-serif;font-size:13px;color:#777;"
            f"background:#f5f5f5;padding:8px 12px;border-left:3px solid #1a3c6e;margin:8px 0'>"
            f"<i>No portfolio names hit the alert threshold today.</i></p>"
        )

    th = "padding:6px 10px;text-align:left;font-weight:500;font-size:12px;white-space:nowrap"
    td = "padding:5px 10px;font-size:13px;white-space:nowrap;border-bottom:1px solid #f0f0f0"
    col_widths = [70, 65, 60, 60, 80, 75, 75, 75, 50, 50, 75, 180]
    colgroup = "".join(f"<col style='min-width:{w}px;width:{w}px'>" for w in col_widths)

    body_rows = ""

    # Portfolio Alerts
    if portfolio_alerts:
        body_rows += render_section_header("Portfolio Alerts", td)
        shorts = [a for a in portfolio_alerts if a.get("portfolio") == "Short"]
        longs  = [a for a in portfolio_alerts if a.get("portfolio") == "Long"]

        if shorts:
            body_rows += render_subheader("Shorts", td)
            for a in sort_within_group(shorts, prefer_upside_first=True):
                body_rows += render_row(a, td)

        if shorts and longs:
            body_rows += render_blank_row(td)

        if longs:
            body_rows += render_subheader("Longs", td)
            for a in sort_within_group(longs, prefer_upside_first=True):
                body_rows += render_row(a, td)

    # Non-portfolio Alerts
    if non_portfolio_alerts:
        if portfolio_alerts:
            body_rows += render_blank_row(td)
        body_rows += render_section_header("Non-portfolio Alerts", td)

        stale     = [a for a in non_portfolio_alerts if a.get("reason") == "Targets may need update"]
        rest      = [a for a in non_portfolio_alerts if a not in stale]

        if rest:
            for a in sort_within_group(rest, prefer_upside_first=True):
                body_rows += render_row(a, td)
        if stale:
            if rest:
                body_rows += render_blank_row(td)
            body_rows += render_subheader("Targets may need update", td)
            for a in sorted(stale,
                            key=lambda a: -abs(a["pct_upside_trader"] if a["alert_side"]=="upside" else a["pct_downside_trader"] or 0)):
                body_rows += render_row(a, td)

    return (
        f"<p style='font-family:Arial,sans-serif;font-size:15px;font-weight:600;margin-bottom:10px'>"
        f"Contour Price Target Alert — {today}</p>"
        f"{no_port_banner}"
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
        f"<th style='{th};text-align:right'>Age</th>"
        f"<th style='{th};text-align:center'>PT Date</th>"
        f"<th style='{th}'>Reason for flag</th>"
        f"</tr>"
        f"</thead>"
        f"<tbody>{body_rows}</tbody>"
        f"</table>"
        f"<p style='font-family:Arial,sans-serif;font-size:11px;color:#aaa;margin-top:8px'>"
        f"% Upside positive = price below upside (room to go); negative = crossed above. "
        f"% Downside positive = cushion above downside; negative = crossed below. "
        f"RSI &gt;70 red, &lt;30 green.</p>"
    )


# ─────────────────────────────────────────────────────
# SEND
# ─────────────────────────────────────────────────────

def send_alert(html):
    payload = {"body": html}
    resp = requests.post(
        CONFIG["power_automate_url"],
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

    targets   = load_targets(CONFIG["csv_path"])
    portfolio = load_portfolio(CONFIG["portfolio_path"])

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

        # Mathematical % (price-vs-target gap, used for crossed detection)
        pct_upside_math   = round((price - upside)   / upside   * 100, 1) if upside   else None
        pct_downside_math = round((price - downside) / downside * 100, 1) if downside else None

        # Split adjustment
        suspicious = (
            (pct_upside_math   is not None and abs(pct_upside_math)   > CONFIG["split_adjust_trigger_pct"]) or
            (pct_downside_math is not None and abs(pct_downside_math) > CONFIG["split_adjust_trigger_pct"])
        )
        if suspicious:
            factor = get_split_adjustment(ticker, tgt_date)
            if factor != 1.0:
                if upside:   upside   = round(upside   / factor, 2)
                if downside: downside = round(downside / factor, 2)
                pct_upside_math   = round((price - upside)   / upside   * 100, 1) if upside   else None
                pct_downside_math = round((price - downside) / downside * 100, 1) if downside else None

        # Trader's view % (sign-flipped):
        # Upside:   positive while price below upside (room to go); negative once crossed
        # Downside: positive while price above downside (cushion); negative once crossed
        pct_upside_trader   = round((upside   - price) / price    * 100, 1) if upside   else None
        pct_downside_trader = round((price - downside) / downside * 100, 1) if downside else None

        triggered  = False
        crossed    = False
        alert_side = None

        if upside is not None:
            if price >= upside:
                triggered = True; crossed = True; alert_side = "upside"
            elif abs(pct_upside_math) <= CONFIG["threshold_pct"] * 100:
                triggered = True; alert_side = "upside"

        if downside is not None:
            if price <= downside:
                triggered = True; crossed = True; alert_side = "downside"
            elif abs(pct_downside_math) <= CONFIG["threshold_pct"] * 100:
                triggered = True; alert_side = alert_side or "downside"

        if not triggered:
            continue

        port = portfolio.get(ticker)

        if not port:
            if not crossed_into_zone_this_month(hist, upside, downside, CONFIG["threshold_pct"]):
                log.debug(f"  {ticker}: in zone but crossed >1mo ago, skipping (non-portfolio).")
                continue

        tgt_date_obj = datetime.strptime(tgt_date, "%m/%d/%Y").date()
        pt_age_days  = (date.today() - tgt_date_obj).days

        alert = {
            "ticker":              ticker,
            "price":               price,
            "upside_pt":           upside,
            "downside_pt":         downside,
            "pct_upside_signed":   pct_upside_math,
            "pct_downside_signed": pct_downside_math,
            "pct_upside_trader":   pct_upside_trader,
            "pct_downside_trader": pct_downside_trader,
            "rsi":                 rsi,
            "target_date":         tgt_date,
            "alert_side":          alert_side,
            "crossed":             crossed,
            "portfolio":           port,
            "pt_age_days":         pt_age_days,
        }
        alert["reason"] = reason_for_flag(alert)

        if port:
            portfolio_alerts.append(alert)
        else:
            non_portfolio_alerts.append(alert)

        log.info(f"  ALERT {display_ticker(ticker)}: px={price} side={alert_side} "
                 f"crossed={crossed} port={port or '-'} reason='{alert['reason']}'")

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
