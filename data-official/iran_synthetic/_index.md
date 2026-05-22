# `data-official/iran_synthetic/` — synthetic Iran DAU

Iran has been offline since ~2026-02-28. This directory holds the synthetic ALL-level Iran DAU values that get summed into no-Iran forecasts to produce world-level numbers.

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
