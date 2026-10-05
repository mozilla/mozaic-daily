# `data-official/2026-09/plots/` — September cycle charts

Every plot here is produced by a notebook cell or a named script the reader can re-run; all canonical
charts carried the DRAFT watermark until the 2026-09-15 publication.

| file(s) | producer |
|---|---|
| `desktop_current_*`, `mobile_current_*`, `all_current_*`, `mobile_ex_iran_*`, `mobile_organic_*`, `mobile_paid_only.png`, `desktop_adjustment_ladder.png` | `../september_canonical_v2026-09-04.ipynb` (last executed 2026-09-15) |
| `desktop_waterfall_*` (2025→Sep 2026 and `*_aug_vs_sep`) | `../september_desktop_waterfalls.ipynb` |
| `desktop_weekly_seasonality_*` | `../september_desktop_weekly_seasonality.ipynb` |
| `mobile_weekly_seasonality_*` | `../september_mobile_weekly_seasonality.ipynb` |
| `raw_intervals_28ma_bands.png` | `../september_raw_intervals.ipynb` |
| `desktop_daily_export_vs_published_ma.png` | `scripts/export_desktop_daily_csv.py` (check plot for the DAILY csv twin) |
| `kpi_sheet_2026-09-17.png` | `scripts/build_kpi_sheet_update.py` (check plot for `../kpi_sheet/`) |
| `prophet_decomposition_desktop.png` | `tooling/prophet_decompose/plot_decomposition.py` (added 2026-10-01; trend / seasonality / overlay decomposition of the canonical desktop fit) |

The headline comparison charts are `desktop_current_vs_august_with_2025.png`,
`mobile_current_vs_august_with_2025.png` and `all_current_vs_august_vs_flat.png`. Executive versions live
in `research/executive-plot/`.

**Present vs Archived.** All tracked PNGs; everything stays on disk.
