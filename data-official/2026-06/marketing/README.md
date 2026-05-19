# Marketing-lift adjustment (code `m`) — 2026-06 forecast cycle

This directory holds the inputs for the `m` adjustment applied to the
June 2026 mobile forecast. The adjustment is bidirectional:

1. **Pre-mozaic subtraction** — `marketing_lift_daily` is subtracted from
   Fenix Android training rows so mozaic learns the no-marketing dynamic.
2. **Post-mozaic add-back** — the same lift is added to per-tile forecast
   rows so the final output reflects "campaign continues at modeled
   levels."

See `src/mozaic_daily/adjustments.py` (`Per-tile marketing-lift applier`
section) for the implementation and
`data-official/adjustment_codes.yaml` for the registry entry.

## Files

| File | Source | What it is |
|---|---|---|
| `marketing.json` | hand-written | The adjustment spec consumed by the pipeline |
| `marketing_lift_model.2026-05-16.parquet` | promoted from `.claude/worktrees/marketing-lift/marketing_lift_model/data/` | Daily DAU lift series (`marketing_lift_daily` column, indexed by `target_date`, span 2026-02-01 → 2026-12-31) |
| `marketing_lift_model.2026-05-16.meta.json` | promoted from worktree | Sidecar with full provenance: retention curve params, historical scaling, forward-projection params |

## Model summary

Source notebooks live in the `worktree-marketing-lift` worktree at
`marketing_lift_model/`:

- `01_signal_extraction.ipynb` — Fenix excess new-profiles & DAU lift from BigQuery
- `02_retention_fit.ipynb` — two-exponential retention curve (Fenix-only YoY context)
- `03_forecast_projection.ipynb` — gap-target convolution fit + piecewise-linear forward projection

Historical lift is `(excess_NP ⊛ retention)` scaled to match the
empirical `(actuals − April mozaic forecast)` gap at the ex-Iran
ALL-MOBILE rollup. Forward portion is piecewise linear with a slope
break at 2026-07-01 reducing the slope to 1/3 of its anchor value.

## Allocation

Per-country shares of Fenix Android DAU on a trailing 28-day window
ending at `training_end_date`. Computed once from the training data,
frozen for the entire forecast horizon. Documented v1 limitation:
assumes the Fenix country mix is roughly stationary through end-2026.

## Iran

Iran (IR) is already excluded from queries upstream; the spec's
`scope.exclude_countries` is a defensive belt-and-suspenders that
ensures no marketing lift leaks onto IR even if the upstream filter
ever regressed.
