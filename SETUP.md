# Contour Price Target Monitor — Setup Guide

Total setup time: ~15 minutes.  
No servers, no Azure, no app registrations.

---

## How it works

```
Every 4 hours (GitHub Actions, free, cloud-hosted):
  1. Download Contour-Price-Targets.xlsx from OneDrive share link
  2. Find most-recent target row per ticker
  3. Pull live prices via yfinance
  4. If any price is within 10% of a target → email all 3 recipients
  5. Never email the same ticker twice in one calendar day
```

---

## Step 1 — Make the SharePoint file publicly shareable

> This lets the script download the file without any login.

1. Open the file in SharePoint/OneDrive
2. Click **Share** (top right)
3. Click the gear icon or **"People with existing access"** → change to **"Anyone with the link"** → set to **View only**
4. Click **Copy link**
5. Save that URL — you'll need it in Step 3

---

## Step 2 — Get an Outlook App Password

> Required because MFA is likely enabled on your account.

1. Go to **https://mysignins.microsoft.com/security-info**  
   (or: Microsoft 365 → your profile → Security info)
2. Click **+ Add method** → choose **App password**
3. Name it (e.g. `ContourPriceMonitor`) → click **Next**
4. Copy the generated password immediately (it's shown only once)

---

## Step 3 — Set up GitHub (free hosting)

### 3a. Create a GitHub account
Go to https://github.com and sign up (free). You only need a personal account.

### 3b. Create a new private repository
1. Click **+** → **New repository**
2. Name: `contour-price-monitor`
3. Set to **Private**
4. Click **Create repository**

### 3c. Upload the files
In the new repo, click **uploading an existing file** and upload:
- `monitor.py`
- `requirements.txt`
- `.github/workflows/monitor.yml`  ← make sure the folder structure is preserved

The `.github/workflows/` folder is the key part — GitHub Actions looks for `.yml` files there.

### 3d. Add your secrets
In GitHub repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these three secrets:

| Secret name | Value |
|---|---|
| `SHAREPOINT_SHARE_URL` | The share link from Step 1 |
| `SMTP_USER` | `hari.kumar@contourasset.com` |
| `SMTP_PASS` | The App Password from Step 2 |

---

## Step 4 — Test it

1. In GitHub repo → **Actions** tab
2. Click **Price Target Monitor** (left sidebar)
3. Click **Run workflow** → **Run workflow** (green button)
4. Watch the run complete — it should show green ✓
5. Check your inbox for an alert (or check the run logs if nothing arrives)

---

## Ongoing maintenance

**When you update price targets in the Excel file:**  
Nothing to do — the script always downloads fresh from SharePoint on every run.

**If a ticker starts failing (wrong symbol, delisted):**  
Check `monitor.log` in the GitHub Actions run output. Failed price fetches are logged clearly.

**To add/remove email recipients:**  
Edit the `ALERT_EMAILS` list in `monitor.py` and re-upload.

**To change the alert threshold (default 10%):**  
Change `THRESHOLD_PCT = 0.10` in `monitor.py`.

**To change check frequency:**  
Edit the cron line in `.github/workflows/monitor.yml`.  
Format: `'0 */4 * * *'` = every 4 hours.  
`'0 */2 * * *'` = every 2 hours, etc.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `401 Unauthorized` on Excel download | Share link not set to "Anyone with the link" |
| `SMTPAuthenticationError` | App Password wrong or expired — generate a new one |
| Ticker shows "price fetch failed" | Check yfinance symbol — may need suffix (e.g. `IFX.DE`) |
| No email received | Check spam folder; check GitHub Actions logs for errors |
| GitHub Actions not running | Repo may be inactive — GitHub pauses scheduled workflows on repos with no activity for 60 days; just trigger manually to re-activate |
