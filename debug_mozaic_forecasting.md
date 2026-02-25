# Bug Briefing: Tile.to_df() Produces NaN on Holiday Dates

## The Bug

`Tile.to_df(quantile=0.5)` returns NaN in the `forecast` column for specific dates near holidays. Every tile across all countries/populations is affected equally. The dates vary by metric but are consistent across tiles.

Affected dates (from a run with `forecast_start_date=2026-02-17`, `forecast_end_date=2027-12-31`):

| Metric used in caller | NaN dates |
|---|---|
| DAU | 2026-12-27, 2027-08-07, 2027-08-14, 2027-08-15, 2027-08-22 |
| New Profiles | 2027-12-12, 2027-12-26 |
| Existing Engagement DAU | 2027-07-18, 2027-07-30, 2027-08-01, +9 more Jul/Aug 2027 |
| Existing Engagement MAU | 0 NaN |

The `Mozaic.to_df()` (aggregate-level) does NOT have this issue — only individual tile-level calls.

## How to Reproduce

This library is used by `mozaic-daily` (the caller repo). The caller invokes:
```python
mozaic_obj.to_granular_forecast_df(quantile=0.5)
```
which calls `tile.to_df(quantile)` for every tile and then `_standard_df_to_forecast_df()` which drops NaN rows (line 372: `df = df[~df['value'].isna()]`). The dropped rows become nulls downstream after an outer join across metrics.

There's no standalone test harness in this repo. To reproduce, run from the `mozaic-daily` repo:
```bash
python scripts/run_main.py --no-checkpoints --data-source legacy_desktop \
  --output-dir ./debug_legacy --forecast-start-date 2026-02-17
```
The debug instrumentation in that repo's `forecast.py` calls `tile.to_df(quantile=0.5)` per-tile and reports NaN dates.

## Where the NaN Is Created

`tile.py:105-107`:
```python
forecast_df["forecast"] = (
    self.forecast_reconciled + self.forecasted_holiday_impacts
).quantile(quantile, axis=1)
```

`forecast_reconciled` is a DataFrame of forecast samples (rows=dates, cols=samples). `forecasted_holiday_impacts` is the same shape. If either has NaN for a date, the sum has NaN, and `.quantile()` returns NaN.

## Most Likely Root Cause: `aggregate_holiday_impacts_upward`

`core.py:277-300` — `aggregate_holiday_impacts_upward()`:

For leaf tiles (not sub-mozaics), `forecasted_holiday_impacts` is computed at line 290:
```python
tile.forecasted_holiday_impacts = getattr(tile, attr).multiply(
    tile.proportional_holiday_effects.reset_index(drop=True), axis=0
)
```

This multiplies `forecast_reconciled` (DataFrame, rows=dates) by `proportional_holiday_effects` (Series, indexed by date, then reset to positional). **If `proportional_holiday_effects` has fewer rows than `forecast_reconciled`** (e.g., missing some forecast dates), the `.multiply(..., axis=0)` produces NaN for unmatched rows.

`proportional_holiday_effects` is set in `_predict_holiday_effects()` at line 250-253:
```python
self.proportional_holiday_effects = (
    df.groupby("date")["average_effect"].sum()
    .reindex(self.forecast_dates, fill_value=0)
)
```

The `.reindex(self.forecast_dates, fill_value=0)` should cover all dates. But this is set on the **country-level Mozaic**, then assigned to child tiles at line 269:
```python
tile.proportional_holiday_effects = self.proportional_holiday_effects
```

Possible issues:
1. **Index alignment**: `reset_index(drop=True)` at line 291 converts to positional index. If `forecast_reconciled` has a different number of rows than `proportional_holiday_effects`, positional alignment breaks.
2. **forecast_dates mismatch**: The Mozaic's `forecast_dates` (used in reindex) might differ from a tile's `forecast_reconciled` date range after reconciliation.
3. **NaN in `forecast_reconciled` itself**: If reconciliation (`_reconcile_top_down_by_rescaling`) introduces NaN for certain dates, multiplying by holiday effects preserves it.

## Secondary Suspect: `_reconcile_top_down_by_rescaling`

`core.py:126-151` — This rescales tiles to match the topline. If the topline has holiday impacts but individual tiles are being rescaled before holiday effects are applied, dates where holiday effects are large could create numerical issues (division by near-zero, etc.).

## What NOT to Investigate

- The outer join in the caller (`combine_tables`) — that's just where nulls become visible, not where they're created.
- Tile skipping / sparse data — all tiles are affected equally.
- Prophet/Stan nondeterminism — same dates fail every run.
- The `Mozaic.to_df()` path — that works fine; only `Tile.to_df()` is broken.

## Suggested Starting Points

1. Add a check in `aggregate_holiday_impacts_upward`: after line 291, verify that `tile.forecasted_holiday_impacts` has the same shape as `tile.forecast_reconciled` and no unexpected NaN.
2. Check whether `proportional_holiday_effects` after `reset_index(drop=True)` has the same length as `forecast_reconciled`'s row count.
3. Inspect what `forecast_reconciled` looks like on the NaN dates — is it already NaN before holiday impacts are added?
