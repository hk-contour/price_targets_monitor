#!/usr/bin/env python3
"""
Contour Price Target Monitor
- Reads Contour-Price-Targets.csv from the repo
- Only includes tickers where the target was set within the past 2 years
- Alerts if live price is within 10% of upside or downside target
- One alert per ticker per calendar day
- Calculates 14-day RSI per ticker
- Sends a single HTML email+Teams post via Power Automate at 10am ET weekdays
- Sends "no alerts" message if nothing qualifies
"""

import os
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
import requests

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────

TEAMS_WEBHOOK_URL = "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ec83745336c243eda45b7aec12638d18/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=K-X9_sEQSPeYMwz1zq8y1wb5Fyb28bFvcicYB61F5Uo"

CSV_PATH        = "Contour-Price-Targets.csv"
ALERT_LOG       = "alerts_sent.json"
THRESHOLD       = 0.10    # 10%
MAX_TARGET_AGE  = 365 * 2 # Only targets set within 2 years

TICKER_MAP = {
    "IFXGn": "IFX.DE", "AG1G":  "AG1.DE",    "SAPG": "SAP.DE",
    "WISEa": "WISE.L",  "PUBP":  "PUB.L",     "RCIb": "RCI-B.TO",
    "8136":  "8136.T",  "6098":  "6098.T",    "7974": "7974.T",
    "7751":  "7751.T",  "4324":  "4324.T",    "2330": "2330.T",
    "6981":  "6981.T",  "6963":  "6963.T",    "6857": "6857.T",
    "4661":  "4661.T",  "6594":  "6594.T",
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

    cutoff      = date.today() - timedelta(days=MAX_TARGET_AGE)
    targets     = {}
    skipped_old = []

    for _, row in df.iterrows():
        ticker   = row["Issuer"]
        tgt_date = row["BeginDate"].date()

        if tgt_date < cutoff:
            skipped_old.append(ticker)
            continue

        upside   = _to_float(row.get("Upside Price Target"))
        downside = _to_float(row.get("Downside Price Target"))
        if upside is None and downside is None:
            continue

        targets[ticker] = {
            "upside":   upside,
            "downside": downside,
            "date":     tgt_date.strftime("%Y-%m-%d"),
        }

    log.info(f"Loaded {len(targets)} tickers (skipped {len(skipped_old)} older than 2 years).")
    return targets


def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────
# STEP 2: FETCH LIVE PRICE + RSI
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
        return None, None, None
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        t    = yf.Ticker(sym)
        hist = t.history(period="60d", auto_adjust=True)
        if hist.empty or len(hist) < 15:
            log.warning(f"{ticker}: insufficient history.")
            return None, None, None
        price    = float(hist["Close"].iloc[-1])
        rsi      = compute_rsi(hist["Close"])
        fi       = t.fast_info
        currency = str(getattr(fi, "currency", "USD") or "USD")
        return price, currency, rsi
    except Exception as e:
        log.warning(f"{ticker}: fetch error - {e}")
        return None, None, None


# ─────────────────────────────────────────────────────
# STEP 3: DAILY DEDUP (once per ticker per calendar day)
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
# STEP 4: BUILD HTML + POST TO TEAMS
# ─────────────────────────────────────────────────────

def rsi_color(rsi):
    if rsi is None:
        return "#888"
    if rsi >= 70:
        return "#c0392b"   # red — overbought
    if rsi <= 30:
        return "#27ae60"   # green — oversold
    return "#333"


def build_html(alerts: list, today: str) -> str:
    """Build a single HTML table for all alerts, or a no-alert message."""
    if not alerts:
        return (
            f"<p style='font-family:Arial,sans-serif'>"
            f"<b style='font-size:16px'>Contour Price Target Alert - {today}</b></p>"
            f"<p style='font-family:Arial,sans-serif;font-size:14px'>"
            f"No tickers within 10% of their upside/downside price target today.</p>"
        )

    rows = ""
    for a in alerts:
        direction = "Upside" if a["target_type"] == "upside" else "Downside"
        rsi       = a.get("rsi")
        rsi_str   = str(rsi) if rsi is not None else "N/A"
        rows += (
            f"<tr>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0'><b>{a['ticker']}</b></td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0'>{a['currency']} {a['price']:.2f}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0'>{direction}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0'>{a['currency']} {a['target_price']:.2f}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0'><b>{a['pct_away']:.1f}%</b></td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;color:{rsi_color(rsi)}'><b>{rsi_str}</b></td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;color:#888'>{a['target_date']}</td>"
            f"</tr>"
        )

    return (
        f"<p style='font-family:Arial,sans-serif'>"
        f"<b style='font-size:16px'>Contour Price Target Alert - {today}</b></p>"
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px;width:100%'>"
        f"<tr style='background:#1a3c6e;color:white'>"
        f"<th style='padding:8px 12px;text-align:left'>Ticker</th>"
        f"<th style='padding:8px 12px;text-align:left'>Live Price</th>"
        f"<th style='padding:8px 12px;text-align:left'>Target</th>"
        f"<th style='padding:8px 12px;text-align:left'>Target Price</th>"
        f"<th style='padding:8px 12px;text-align:left'>Distance</th>"
        f"<th style='padding:8px 12px;text-align:left'>RSI (14)</th>"
        f"<th style='padding:8px 12px;text-align:left'>Set On</th>"
        f"</tr>"
        f"{rows}"
        f"</table>"
        f"<p style='font-family:Arial,sans-serif;font-size:11px;color:#aaa'>"
        f"RSI &gt;70 = red | RSI &lt;30 = green | "
        f"Source: Contour-Price-Targets.csv</p>"
    )


def post_to_teams(html: str):
    payload = {"body": html}
    resp = requests.post(
        TEAMS_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    if resp.status_code in (200, 202):
        log.info("Posted to Teams successfully.")
    else:
        log.error(f"Teams webhook failed: HTTP {resp.status_code} - {resp.text}")
        raise RuntimeError(f"Teams webhook error: {resp.status_code}")


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

        price, currency, rsi = fetch_price_and_rsi(ticker)

        if price is None:
            if ticker not in SKIP:
                price_errors.append(ticker)
            continue

        triggered = False

        if upside is not None:
            pct = abs(price - upside) / upside * 100
            if pct <= THRESHOLD * 100:
                log.info(f"  ALERT {ticker}: ${price:.2f}  upside=${upside}  {pct:.1f}% away  RSI={rsi}")
                alerts_to_send.append({
                    "ticker": ticker, "price": price, "currency": currency,
                    "target_type": "upside", "target_price": upside,
                    "pct_away": pct, "target_date": tgt_date, "rsi": rsi,
                })
                triggered = True

        if downside is not None:
            pct = abs(price - downside) / downside * 100
            if pct <= THRESHOLD * 100:
                log.info(f"  ALERT {ticker}: ${price:.2f}  downside=${downside}  {pct:.1f}% away  RSI={rsi}")
                alerts_to_send.append({
                    "ticker": ticker, "price": price, "currency": currency,
                    "target_type": "downside", "target_price": downside,
                    "pct_away": pct, "target_date": tgt_date, "rsi": rsi,
                })
                triggered = True

        if triggered:
            mark_alerted(alert_log, ticker)

    save_log(alert_log)

    alerts_to_send = sorted(alerts_to_send, key=lambda x: x["pct_away"])
    today = date.today().strftime("%B %d, %Y")
    html  = build_html(alerts_to_send, today)
    post_to_teams(html)

    log.info(
        f"Done. {len(targets)} tickers checked | "
        f"{len(alerts_to_send)} alerts | "
        f"Price errors: {price_errors if price_errors else 'none'}"
    )


if __name__ == "__main__":
    run()
