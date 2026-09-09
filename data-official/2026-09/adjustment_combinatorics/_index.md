# `adjustment_combinatorics/` — which desktop adjustments ship in September?

Not every adjustment can be published this cycle; they are to be spread over several. This directory
holds the evidence for choosing: **every subset of the four droppable desktop overlays** (`i` India
excess, `j` Japan bot, `l` launch at login for new users, `o` MozillaOnline) scored at Dec-15, with the
Win10 headwind `h` applied in every case. Desktop only. Mobile (`p`, `t`, `u`) was deliberately left out.

Each subset is a real desktop forecast (g01 config, 2026-09-02 seam, cached raw pull) with exactly that
subset subtracted from training rows and added back after, so the Dec-15 numbers include the overlays'
interactions through the Prophet fit — they are not sums of single effects. The runs themselves live in
`../adjustment_ladder/<codes>.<key>/` (shared cache with the ladder, gitignored parquets/pkls).

## What's here

| file | what |
|---|---|
| `combinatorics_manifest.json` | one entry per subset: overlays enabled, run parquet path, plain Dec-15 28d-MA (no display layer); model config, overlay fingerprints, `h` Dec-15 value. Written by `scripts/build_adjustment_combinatorics.py` |
| `index.html` | the report (self-contained, images embedded): overview chart, Dec-15 table (vs August delivered 48,703,443, vs the all-in September build, vs the Low/Baseline/Stretch targets), then one section per combination grouped by number dropped, each with the canonical desktop chart (actuals, August delivered, this subset with `h`, 2025 calendar-aligned, targets, DRAFT watermark). Written by `scripts/render_adjustment_combinatorics.py` |
| `dec15_by_combination.csv` | the table as data |
| `counterfactuals.csv` | the counterfactual section as data: all five adjustments kept, `h` re-anchored so the all-in Dec-15 is exactly +479,000 vs August (anchor −1,089,347 instead of −726,000; display layer, exact, no rerun). Configured in the renderer's `CYCLE_SETTINGS[...]['counterfactuals']` |
| `plots/desktop_adj-<codes>.png` | one chart per combination; `plots/desktop_all_combinations.png` is the overview; `plots/desktop_counterfactual_<key>.png` per counterfactual |
| `desktop_actuals_daily.parquet` | cached BigQuery desktop actuals used by the charts (gitignored; `--refresh-actuals` re-queries) |

## What isn't here

- No mobile combinations, no cross-platform ALL totals.
- No decision. Nothing in this directory changes the canonical build; picking a subset is a spec/registry
  change plus a model rerun, recorded in `../_index.md`.
- No re-measurement of any curve: every overlay is the curve on disk as of the build date.

## Reproducing

```bash
source .venv/bin/activate
python scripts/build_adjustment_combinatorics.py --cycle 2026-09 --forecast-start-date 2026-09-02 \
    --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-02 \
    --config-from data-official/2026-09/desktop_g01_2026-09-02/<slug>/mozaic_daily_forecast.2026-09-02.ld-D.adj-ijlo.parquet.meta.json
python scripts/render_adjustment_combinatorics.py --cycle 2026-09
```

The build prompts before any model run (`--yes` only with explicit approval). A spec or curve edit
changes that overlay's fingerprint and re-runs only the subsets containing it.
