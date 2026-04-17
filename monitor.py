#!/usr/bin/env python3
"""
Contour Price Target Monitor
- Reads Contour-Price-Targets.csv from the repo
- For each ticker: uses the most recent BeginDate row
- Checks live price via yfinance
- Posts to Microsoft Teams if price is within 10% of upside OR downside
- One alert per ticker per calendar day
- Runs every 4 hours via GitHub Actions
"""

import os
import json
import logging
import sys
from datetime import date, datetime

import pandas as pd
import yfinance as yf
import requests

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────

TEAMS_WEBHOOK_URL = "https://defaultc3c9ee10042749379437645c69c5e5.3a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ec83745336c243eda45b7aec12638d18/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=K-X9_sEQSPeYMwz1zq8y1wb5Fyb28bFvcicYB61F5Uo"

CSV_PATH      = "Contour-Price-Targets.csv"
ALERT_LOG     = "alerts_sent.json"
THRESHOLD     = 0.10   # 10%

# Tickers that need yfinance exchange suffixes
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
# STEP 1: LOAD TARGETS
# ─────────────────────────────────────────────────────

def load_targets(path: str) -> dict:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["BeginDate"] = pd.to_datetime(df["BeginDate"], format="mixed", dayfirst=False)
    df["Issuer"]    = df["Issuer"].astype(str).str.strip().str.upper()
    df = df.sort_values("BeginDate", ascending=False).drop_duplicates("Issuer", keep="first")

    targets = {}
    for _, row in df.iterrows():
        ticker   = row["Issuer"]
        upside   = _to_float(row.get("Upside Price Target"))
        downside = _to_float(row.get("Downside Price Target"))
        if upside is None and downside is None:
            continue
        targets[ticker] = {
            "upside":   upside,
            "downside": downside,
            "date":     row["BeginDate"].strftime("%Y-%m-%d"),
        }

    log.info(f"Loaded {len(targets)} tickers.")
    return targets


def _to_float(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────
# STEP 2: FETCH LIVE PRICES
# ─────────────────────────────────────────────────────

def fetch_price(ticker: str):
    if ticker in SKIP:
        return None, None
    sym = TICKER_MAP.get(ticker, ticker)
    try:
        t     = yf.Ticker(sym)
        fi    = t.fast_info
        price = getattr(fi, "last_price", None)
        if not price or price <= 0:
            hist = t.history(period="2d", auto_adjust=True)
            if hist.empty:
                log.warning(f"{ticker}: no price data.")
                return None, None
            price = float(hist["Close"].iloc[-1])
        return float(price), str(getattr(fi, "currency", "USD") or "USD")
    except Exception as e:
        log.warning(f"{ticker}: fetch error — {e}")
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
# STEP 4: POST TO TEAMS
# ─────────────────────────────────────────────────────

def post_to_teams(alerts: list):
    alerts = sorted(alerts, key=lambda x: x["pct_away"])
    today  = date.today().strftime("%B %d, %Y")
    n      = len(alerts)

    # Build Adaptive Card body rows
    fact_rows = []
    for a in alerts:
        urgency   = "🚨" if a["pct_away"] < 3 else "⚠️" if a["pct_away"] < 7 else "📌"
        direction = "▲ Upside" if a["target_type"] == "upside" else "▼ Downside"
        fact_rows.append({
            "type": "ColumnSet",
            "columns": [
                {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": f"{urgency} **{a['ticker']}**", "wrap": True}]},
                {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": f"Live: {a['currency']} {a['price']:.2f}  |  {direction}: {a['currency']} {a['target_price']:.2f}  |  **{a['pct_away']:.1f}% away**", "wrap": True}]},
            ]
        })

    # Adaptive Card JSON — this is what Power Automate's "Post card" action expects
    adaptive_card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": f"📊 Contour Price Target Alert — {today}",
                "size": "Large",
                "weight": "Bolder",
                "wrap": True
            },
            {
                "type": "TextBlock",
                "text": f"{n} ticker{'s are' if n > 1 else ' is'} within {int(THRESHOLD*100)}% of a price target:",
                "wrap": True,
                "spacing": "Small"
            },
            {"type": "Separator"},
            *fact_rows,
            {"type": "Separator"},
            {
                "type": "TextBlock",
                "text": "One alert per ticker per day  ·  Source: Contour-Price-Targets.csv",
                "size": "Small",
                "isSubtle": True,
                "wrap": True
            }
        ]
    }

    # Power Automate flow uses string(variables('Body')) for the Adaptive Card
    # Send the Adaptive Card JSON string in a 'body' field
    payload = {
        "body": json.dumps(adaptive_card)
    }

    resp = requests.post(
        TEAMS_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )

    if resp.status_code in (200, 202):
        log.info(f"Teams message posted for {[a['ticker'] for a in alerts]}")
    else:
        log.error(f"Teams webhook failed: HTTP {resp.status_code} — {resp.text}")
        raise RuntimeError(f"Teams webhook error: {resp.status_code}")


# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────

def run():
    log.info("=" * 55)
    log.info(f"Check starting — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

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

        price, currency = fetch_price(ticker)

        if price is None:
            if ticker not in SKIP:
                price_errors.append(ticker)
            continue

        triggered = False

        if upside is not None:
            pct = abs(price - upside) / upside * 100
            if pct <= THRESHOLD * 100:
                log.info(f"  ALERT {ticker}: ${price:.2f}  upside=${upside}  {pct:.1f}% away")
                alerts_to_send.append({
                    "ticker": ticker, "price": price, "currency": currency,
                    "target_type": "upside", "target_price": upside,
                    "pct_away": pct, "target_date": tgt_date,
                })
                triggered = True

        if downside is not None:
            pct = abs(price - downside) / downside * 100
            if pct <= THRESHOLD * 100:
                log.info(f"  ALERT {ticker}: ${price:.2f}  downside=${downside}  {pct:.1f}% away")
                alerts_to_send.append({
                    "ticker": ticker, "price": price, "currency": currency,
                    "target_type": "downside", "target_price": downside,
                    "pct_away": pct, "target_date": tgt_date,
                })
                triggered = True

        if triggered:
            mark_alerted(alert_log, ticker)

    save_log(alert_log)

    if alerts_to_send:
        post_to_teams(alerts_to_send)
    else:
        log.info("No alerts this run.")

    log.info(
        f"Done. {len(targets)} tickers checked | "
        f"{len(alerts_to_send)} alerts | "
        f"Price errors: {price_errors if price_errors else 'none'}"
    )


if __name__ == "__main__":
    run()
