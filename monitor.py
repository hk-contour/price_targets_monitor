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
MAX_TARGET_AGE = 365 * 2

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


def fetch_price_and_rsi(ticker: str):
    if ticker in SKIP:
        return None, None
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        t    = yf.Ticker(sym)
        hist = t.history(period="60d", auto_adjust=True)
        if hist.empty or len(hist) < 15:
            log.warning(f"{ticker}: insufficient history.")
            return None, None
        price = round(float(hist["Close"].iloc[-1]), 2)
        rsi   = compute_rsi(hist["Close"])
        return price, rsi
    except Exception as e:
        log.warning(f"{ticker}: fetch error - {e}")
        return None, None


# ─────────────────────────────────────────────────────
# STEP 3: DAILY DEDUP
# ─────────────────────────────────────────────────────

def load_log() -> dict:
    if os.path.exists(ALERT_LOG):
        with open(ALERT_LOG) as f:
            return json.load(f)
    return {}


def save_log(data: dict):
    cutoff = date.today().toordinal() - 14
    data   = {k: v for k, v in data.items()
              if date.fromisoformat(k).toordinal() >= cutoff}
    with open(ALERT_LOG, "w") as f:
        json.dump(data, f, indent=2)


def already_alerted(data: dict, ticker: str) -> bool:
    return data.get(date.today().isoformat(), {}).get(ticker, False)


def mark_alerted(data: dict, ticker: str):
    data.setdefault(date.today().isoformat(), {})[ticker] = True


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
        return f"<span style='color:#c0392b;font-weight:600'>{rsi}</span>"
    if rsi <= 30:
        return f"<span style='color:#27ae60;font-weight:600'>{rsi}</span>"
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
        rows += (
            f"<tr>"
            f"<td style='{td}'><b>{a['ticker']}</b></td>"
            f"<td style='{td};color:#555'>{a['alert_side'].capitalize()}</td>"
            f"<td style='{td}'>{a['price']:.2f}</td>"
            f"<td style='{td}'>{fmt_pt(a['downside_pt'])}</td>"
            f"<td style='{td}'>{fmt_pct(a['pct_downside'], a['crossed'] and a['alert_side'] == 'downside')}</td>"
            f"<td style='{td}'>{fmt_pt(a['upside_pt'])}</td>"
            f"<td style='{td}'>{fmt_pct(a['pct_upside'],   a['crossed'] and a['alert_side'] == 'upside')}</td>"
            f"<td style='{td}'>{fmt_rsi(a['rsi'])}</td>"
            f"<td style='{td};color:#888;font-size:12px'>{a['target_date']}</td>"
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
        f"<th style='{th}'>Price</th>"
        f"<th style='{th}'>Downside PT</th>"
        f"<th style='{th}'>% Downside</th>"
        f"<th style='{th}'>Upside PT</th>"
        f"<th style='{th}'>% Upside</th>"
        f"<th style='{th}'>RSI</th>"
        f"<th style='{th}'>PT Date</th>"
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

    targets   = load_targets(CSV_PATH)
    alert_log = load_log()

    alerts_to_send = []
    price_errors   = []

    for ticker, info in sorted(targets.items()):
        upside   = info["upside"]
        downside = info["downside"]
        tgt_date = info["date"]

        if already_alerted(alert_log, ticker):
            continue

        price, rsi = fetch_price_and_rsi(ticker)

        if price is None:
            if ticker not in SKIP:
                price_errors.append(ticker)
            continue

        # Signed % difference
        # Upside:   positive = above target (crossed), negative = approaching from below
        # Downside: negative = below target (crossed), positive = approaching from above
        pct_upside   = round((price - upside)   / upside   * 100, 1) if upside   else None
        pct_downside = round((price - downside) / downside * 100, 1) if downside else None

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
            log.info(f"  ALERT {ticker}: px={price}  up={upside}  dn={downside}  RSI={rsi}  crossed={crossed}")
            alerts_to_send.append({
                "ticker":      ticker,
                "price":       price,
                "upside_pt":   upside,
                "downside_pt": downside,
                "pct_upside":  pct_upside,
                "pct_downside": pct_downside,
                "rsi":         rsi,
                "target_date": tgt_date,
                "alert_side":  alert_side,
                "crossed":     crossed,
            })
            mark_alerted(alert_log, ticker)

    save_log(alert_log)

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
