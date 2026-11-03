# tools/ — Lightkeeper-sourced data generators

`portfolio_12mo.json` and `analyst_map.json` are both derived from Lightkeeper.
GitHub Actions cannot reach the Lightkeeper connector, so these files are
generated here and committed. Re-run them from a client that can reach
Lightkeeper (Claude Code / Cowork).

Both scripts are idempotent — re-running them on current output is a no-op.

## Refreshing the data

### 1. Pull from Lightkeeper

Two `lightkeeper_get_portfolio_data` calls against `LKP_MASTER__PORT`, series
`Trading (non-additive)`, periodicity `daily`, `return_what=itmValues`, over
the trailing 12 months, with stat:

```
days-held-by-rollup, netTimeMethod=max, difference=false, units=raw
```

| rollup            | save response rows to                       |
|-------------------|---------------------------------------------|
| `Issuer`          | `tools/lk_pulls/days_held_<end-date>.json`  |
| `Analyst***Issuer`| `tools/lk_pulls/analyst_<end-date>.json`    |

`days_held_*.json` is the raw `[[issuer, days], ...]` array.
`analyst_*.json` is `{analyst_code: {issuer: days}}` (split the `A**B` index).

Then point `PULL` at the new filenames at the top of each script.

### 2. Regenerate

```bash
python3 tools/gen_portfolio_12mo.py portfolio_12mo.json
python3 tools/merge_analyst_map.py
```

## gen_portfolio_12mo.py

Rebuilds the 12-month master-portfolio membership set. Requires
**days-held >= 1** — a name that appears in the blotter but was never carried
across a day does not count as traded. Drops currency, option, swap-basket and
CFD rows. Every dropped row is recorded in `_meta.dropped_rows` so exclusions
stay auditable.

## merge_analyst_map.py

Merges Lightkeeper's `Analyst` blotter tag into `analyst_map.json`.

**Lightkeeper's Analyst attribute is a trading attribution, not a research
coverage map.** `BK/BT/JC/JEC/MS/SS` are real research owners; `DM` is the PM's
catch-all bucket and `RF` is excluded. Some covered names (e.g. HOOD) are never
held, so Lightkeeper has no row for them at all.

So the merge is deliberately conservative:

- Lightkeeper fills blanks only where it carries a real coverage code.
- `DM` / `RF` never create or overwrite an entry.
- Existing hand-maintained entries always win, including the two that
  disagree with Lightkeeper (`QLYS`, `WIX`) — those are printed on each run
  for a human to settle, never auto-changed.

Replacing the file wholesale with the Lightkeeper tag would be wrong: it would
reassign SHOP from JC to the PM bucket and leave SOFI and SPOT unowned.

## gen_ticker_master.py

Builds `ticker_master.json`, the BBG-keyed identifier map. Seeded from the
Lightkeeper pulls, then overlaid with curated `OVERRIDES` (foreign listings),
`CORPORATE_ACTIONS` (delisted / acquired / taken private) and `RENAMED_ONTO`
(old code folded in as an alias on the surviving entry).

Entries are marked `resolved` (curated) or `assumed` (inferred as a plain US
listing). Validate the assumed ones with `check_tickers.py`.

```bash
python3 tools/gen_ticker_master.py
```

## check_tickers.py

On-demand liveness scan — **not** part of the weekly job. Price-checks the map
and reports DARK (no price) and STALE (last trade too old) entries, which are
usually unrecorded corporate actions. Fix them in `gen_ticker_master.py`, then
regenerate.

```bash
python3 tools/check_tickers.py            # analyst-covered names only
python3 tools/check_tickers.py --all
```

## gen_earnings_dates.py

Builds `earnings_dates.json` — who has reported recently — from the TMT
Earnings Calendar workbooks in Dropbox:

    /Hari Kumar/06. Research/Earnings Calendar/TMT Earnings Calendar_<date>.xlsx

**The calendar is forward-looking**: one row per ticker carrying its NEXT
earnings date. When a company reports, the following snapshot rolls that row
forward a quarter — so no single file can tell you who just reported. The
generator reads several weekly snapshots and takes, per ticker, the latest
date that is already in the past. The 10/23 snapshot has SPOT at 10/22
Confirmed; the 10/30 snapshot has already moved it to 2/10/2027.

So: **keep the last 6–8 weekly workbooks in the input folder.** Dropping older
ones loses the prints they alone recorded. Skip the "DAILY VIEW" files — those
are a derived pivot, not the table.

Only `Confirmed` and `Tentative` rows are used. Bloomberg `Estimate` dates are
routinely off by days and must never trigger an analyst email.

```bash
# download the workbooks from Dropbox into a folder first
python3 tools/gen_earnings_dates.py ~/Downloads/earnings_cal
```

`stale_alerts.py` warns in the summary email once this file is more than
`earnings_snapshot_max_age_days` (14) old.
