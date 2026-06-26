# `data-official/iran_synthetic/` — synthetic Iran DAU

> **Status: RETIRED for forecasting as of July 2026 (kept for reference, not deleted).**
> Iran's internet has returned. Starting with the July 2026 cycle, IR goes back into
> native queries and the multi-month shutdown gap is masked via a mozaic-forecasting
> **training-exclusion ("gap holiday")** rather than excluded-and-modeled-back. See
> `data-official/2026-07/iran_gap_holiday_mozaic_handoff.md` and `2026-07/TODO_factors.md` §0.
> The synthetic add-back workflow below (Feb–June 2026) is superseded for this purpose.
> Recovery-curve modeling in `research/iran/` may still be referenced.

Iran was offline ~2026-02-28 through mid-2026. This directory holds the synthetic ALL-level Iran DAU values that, during that window, got summed into no-Iran forecasts to produce world-level numbers.

## Files

| File | What it holds |
|---|---|
| `iran_synthetic.parquet` | Desktop ALL-level synthetic DAU (historical + forecast) |
| `mobile/` | Mobile ALL-level synthetic DAU |
| `parameters.json` | Generation parameters (recovery curve shape, cap level, anchor date) |

## Producer

`scripts/generate_iran_synthetic.py` runs Mozaic for Iran alone and writes these files. Re-run when:
- The recovery model in `research/iran/iran_partial_recovery_model.ipynb` changes
- A new month's forecast needs synthetic Iran data extending further forward

## Consumer

`scripts/add_iran_to_forecast.py` reads `iran_synthetic.parquet` (and `mobile/`) and adds the values to a no-Iran forecast via summation, producing the `.plus_iran.parquet` artifacts under `data-official/{YYYY-MM}/<config>/`.
