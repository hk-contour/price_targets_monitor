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
