# Redenomination Bug in Country View

## Problem

Countries that redenominated their currency (Romania, Turkey, Russia, etc.) show
wild spikes in the Country View chart. Example: Romania's monthly wage index
jumps to ~2.5M units and then crashes — because raw wage values switch from old
lei (8,183,317 in 2004) to new lei (968 in 2005) at the redenomination boundary.

When all indicators are rebased to a common base year (e.g. 2001 = 100), the
pre-redenomination wages produce enormous index values because they're divided by
the tiny new-currency base value.

## Root Cause

Two data sources use different currency units for historical data:

- **World Bank FX API** (`PA.NUS.FCRF`): retroactively converts ALL exchange
  rates to the **current** local currency unit. Romania 2004 shows 3.26 RON/USD
  (new lei), not ~32,636 ROL/USD (old lei).
- **ILO wage data**: reports wages in the currency that was in use **at the
  time**. Romania 2004 shows 8,183,317 ROL (old lei).

So `value_usd = value / exchange_rate` produces garbage for pre-redenomination
years: 8,183,317 / 3.26 = 2,509,912 USD (should be ~250 USD).

CPI, Food CPI, Housing Price Index are dimensionless indices — not affected.

## What Was Done

### 1. Redenomination reference (`transform/redenominations.py`)

Created a comprehensive dictionary of all known currency redenominations:
35+ countries, multiple entries for countries with sequential redenominations
(Argentina ×4, Brazil ×5, Peru ×2, Zimbabwe ×3, etc.).

Format:
```python
REDENOMINATIONS = {
    "ROU": [(2005, 10_000)],       # old leu → new leu
    "TUR": [(2005, 1_000_000)],    # old lira → new lira
    "ARG": [
        (1970, 100),
        (1983, 10_000),
        (1985, 1_000),
        (1992, 10_000),
    ],
    ...
}
```

### 2. FX normalization in ETL (`transform/main.py`)

Added `normalize_fx_redenomination()` — for each FX record, multiplies the
exchange rate by the cumulative factor of all redenominations that happened AFTER
that year. This converts the WB-reported new-currency rate back to old-currency
rate, matching the source data.

Called in pipeline after `normalize_fx_eurozone()`:
```python
fx_normalized = normalize_fx_eurozone(fx_raw)
fx_normalized = normalize_fx_redenomination(fx_normalized)
```

### 3. Verification: all WB rates are retroactively converted

Checked every entry in the redenomination dict by comparing WB exchange rates
just before and after the switch year. Results:

- **Ratio ≈ 1** (e.g. ROU: 1.12, TUR: 1.06, POL: 0.94): WB clearly
  retroactively converted. Correction needed.
- **Ratio far from 1 but NOT near 1/factor** (e.g. ARG 1983: 0.25, BRA 1986:
  0.45): still WB-converted — the deviation from 1.0 is hyperinflation /
  devaluation happening simultaneously. If WB had NOT converted, the ratio would
  be ≈ 1/factor (e.g. 0.0001 for ÷10,000).

Conclusion: WB retroactively converts ALL rates. All entries need correction.

### 4. ETL re-run result

After the fix, Romania `value_usd` across the 2005 boundary:

| Year | value (local) | value_usd |
|------|--------------|-----------|
| 2003 | 6,637,868    | 199.94    |
| 2004 | 8,183,317    | 250.74    |
| 2005 | 968          | 332.23    |
| 2006 | 1,146        | 407.98    |

`value_usd` is now smooth across the redenomination. Previously it was
2,509,912 → 332 (broken).

### 5. Dashboard checkbox ("USD equivalent")

Added a checkbox in Country View (`dashboard/app.py`) that switches the rebase
from `value` (local currency) to `value_usd` (USD-adjusted). This should fix
the spike for redenomination countries when enabled.

## Current Status: NOT WORKING

The checkbox approach was implemented but **did not fix the visual spike** when
tested. Possible causes to investigate:

1. **Streamlit cache**: `@st.cache_data(ttl=300)` may serve stale data for up
   to 5 minutes after ETL re-run. Try clearing cache (hamburger menu →
   "Clear cache") or restarting the dashboard container.

2. **Base year issue**: the common base year is computed BEFORE the USD toggle,
   using all data including rows without `value_usd`. If the base year row lacks
   `value_usd` for some indicator, the rebase division produces NaN.

3. **Filtering gap**: when `use_usd=True`, rows with null `value_usd` are
   dropped. If this removes the base year row for an indicator, the rebase
   breaks.

4. **The checkbox might still be disabled**: `has_usd` requires
   `.notna().any()` but the check runs on the year-slider-filtered subset. If
   the filtered range has no `value_usd` data, the checkbox stays disabled.

## Suggested Next Steps

1. Clear Streamlit cache and verify the checkbox actually enables and the
   correct `val_col` is used (add `st.write(val_col, base_year)` for debug).

2. Check that the base year row has `value_usd` for all indicators. If not,
   recalculate base year after filtering to rows with `value_usd`.

3. Consider an alternative approach: instead of a USD toggle, detect
   redenomination countries from the reference and auto-correct `value` in the
   dashboard by multiplying pre-switch values by 1/factor. This avoids the
   USD conversion entirely and keeps the chart in local-currency terms.

   ```python
   # Pseudocode
   from transform.redenominations import REDENOMINATIONS
   if selected_country in REDENOMINATIONS:
       for switch_year, factor in REDENOMINATIONS[selected_country]:
           mask = country_df["year"] < switch_year
           country_df.loc[mask, "value"] /= factor
   ```

4. The USD toggle is still useful as a feature (shows real purchasing power in
   dollar terms), but should not be the primary fix for redenomination visual
   artifacts.

## Files Changed

- `transform/redenominations.py` — NEW: comprehensive redenomination reference
- `transform/main.py` — added `normalize_fx_redenomination()`, import
- `dashboard/app.py` — added "USD equivalent" checkbox in Country View
