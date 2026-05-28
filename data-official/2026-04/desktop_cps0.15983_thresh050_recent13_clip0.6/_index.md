# `desktop_cps0.15983_thresh050_recent13_clip0.6/` — April 2026 production desktop config

Forecast_start_date: **2026-04-01**. Production desktop config for the April cycle: threshold=-0.05, cps=0.15983, recent_weeks=13, clip=0.6. Parameters verified against the fitted Mozaic pkl.

## Provenance

- Forecast parquets produced **2026-04-08** by `scripts/run_main.py` (original April run).
- `.meta.json` sidecars added **2026-05-18** by `scripts/migrate_forecast_names.py` (reconstructed provenance — the underlying parquets are unchanged from April 8).

## Files

| File | Size | What it is |
|---|---|---|
| `april_composite_forecast_28ma.adj-h.csv` | 23 KB | **Headline leadership CSV** — desktop+mobile composite, 28-day MA, headwind-adjusted. Copied from one level up. |
| `april_composite_forecast_28ma.adj-h.csv.meta.json` | 1.2 KB | Sidecar provenance for the composite CSV |
| `parameters.json` | 351 B | Model config used (threshold=-0.05, cps=0.15983, recent_weeks=13, clip=0.6) |
| `mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet` | 1.4 MB | Raw legacy-desktop DAU forecast, no adjustments, no Iran |
| `mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet.meta.json` | 906 B | Sidecar provenance for the raw parquet (sha1, commit, config) |
| `mozaic_daily_forecast.2026-04-01.ld-D.raw.plus_iran.parquet` | 417 KB | Same as above with synthetic Iran added back in (no-Iran model + Iran composition) |
| `mozaic_daily_forecast.2026-04-01.ld-D.raw.plus_iran.parquet.meta.json` | 906 B | Sidecar provenance for the plus_iran parquet |
| `mozaic_parts.raw.legacy.desktop.DAU.parquet` | 747 KB | Raw BigQuery query output (pre-forecast input data) for legacy-desktop DAU |
| `mozaic_objects.legacy_desktop.2026-04-01.pkl` | 693 MB | Fitted Mozaic objects (cloudpickled) — needed to re-render diagnostics without re-fitting |

## Notes

- "ld-D" = legacy_desktop + DAU. There's no glean-desktop equivalent in the April cycle; the production April composite is built from legacy desktop only.
- The composite CSV is a **copy** of `data-official/2026-04/april_composite_forecast_28ma.adj-h.csv`; the original at the parent path is canonical. The composite includes mobile contribution even though the per-source ingredients in this subdir are desktop-only.
- Load forecasts via `mozaic_daily.adjustments.load_forecast()` — it validates filename ↔ meta consistency.
