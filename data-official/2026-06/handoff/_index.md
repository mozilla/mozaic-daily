# `data-official/2026-06/handoff/` — stakeholder handoff snapshots

Point-in-time, shareable **packaged bundles** of the June 2026 canonical forecast curves for
sending to stakeholders. This is the canonical June forecast (the seam-smoothing bandaid
version — variance-matched transition, see `research/ma-seam-turbulence/`).

## What's here
- **`june_canonical_handoff_<date>.zip`** — a self-contained bundle: the ALL-level
  `june_canonical_curves.csv`, the 15 per-country CSVs, the `csv/README.md`, the global
  (world-rollup) headline desktop/mobile PNGs (with the gold Dec-15 stakeholder markers), the
  per-country plot PNGs + grids (desktop + mobile), and the diagnostic
  `seam_smoothing_report.html`. Dated by the forecast-start date (current: **2026-05-26**).
  Large → gitignored, archived to GCS with the rest of the cycle.

## What isn't here (source of truth — do NOT duplicate, regenerate instead)
The bundle is a **packaged copy**; the live, tracked sources are:
- ALL + per-country CSVs + README → `../csv/` (the small canonical CSVs are git-tracked).
- Per-country plots → `../csv/per_country/plots/{desktop,mobile}/`.
- Diagnostic report → `../../../research/ma-seam-turbulence/report.html`.

## Rebuilding a snapshot
```bash
source .venv/bin/activate
python3 data-official/2026-06/export_canonical_curves.py                # ALL CSV (needs BQ auth)
python3 data-official/2026-06/export_canonical_curves.py --per-country  # per-country CSVs
python3 data-official/2026-06/plot_per_country_curves.py                # desktop plots
python3 data-official/2026-06/plot_per_country_curves.py --platform mobile
python3 research/ma-seam-turbulence/build_report.py                     # report.html
```
Then stage those files + `report.html` into a folder and `zip -r` it here, dated by build day.
