# MozillaOnline migration overlay (`o`)

> **STATUS (2026-07-07): WIRED with Brad's OFFICIAL model.** The `o` overlay is implemented
> (bidirectional, `legacy_desktop` DAU, `modern_windows` segment, fixed geo shares) and folded into
> the canonical `…adj-lmo.parquet`. The **active** artifact is
> `mozillaonline_migration_model.official.2026-06-29.parquet` (from `source_data/mozilla_online_forecast_jul.csv`
> via `build_official_series.py`; Dec-15 28d-MA ~567K). OS scope is **modern_windows-only by
> measurement** (winX users are pinned on Firefox too old to receive the migrating build). The
> placeholder described below (~673K Dec-15) is **retired** but kept for provenance. See `_index.md`.

---

*The following describes the retired data-grounded PLACEHOLDER (superseded by Brad's official model).*

Data-grounded placeholder model of the **MozillaOnline → canonical Firefox desktop
migration** tailwind, for the July 2026 forecast. Drop-in swap for Brad Ochocki
Szasz's official model.

## What this is

MozillaOnline (Mozilla's China distribution partner) is migrating its desktop
users onto mainline Firefox. In telemetry the migrating build is
`app_name = "Firefox Desktop MozillaOnline"`; as users update they flip to
`app_name = "Firefox Desktop"` (the canonical KPI series the forecast models), so
they newly count — an **additive tailwind**, ~93% China.

## Bidirectional overlay (same contract as marketing-lift `m`)

1. **Subtract** the migration DAU from desktop training rows before mozaic (so
   Prophet learns the pre-migration dynamic and doesn't extrapolate the ramp).
2. Run the forecast.
3. **Add** it back to per-country + ALL forecast rows.

> The tailwind is **already in recent training data** (the June canonical uptick),
> so the subtract step is essential.

## Files

| File | What |
|------|------|
| `mozillaonline_migration_model.placeholder.2026-06-25.parquet` | daily series: `migration_dau_daily`, `migration_dau_ma` on a `target_date` index |
| `mozillaonline.json` | spec: data_file, value_column, geo allocation shares, scope, applies_to_forecast_start |
| `mozillaonline_migration_model.placeholder.2026-06-25.meta.json` | provenance + tunables + validation + swap_instructions |
| `summary.html` | self-contained proof doc for a reviewing DS |
| `source_data/*.csv` | cached pre-June + migration-window telemetry inputs |
| `build_placeholder_model.py` | reproducible generator (tunable constants at top) |
| `*.png` | curve, decomposition, conservation/fit, cohorts, geo |

## Model (measured, not assumed)

- **Source / geo / cohorts** from pre-June `Firefox Desktop MozillaOnline` telemetry:
  source baseline ≈ 1,052,899 DAU; **CN 92.8%** + VPN/diaspora tail;
  **release ≫ ESR**.
- **Shape**: two-cohort logistic conversion (release 2026-06-02,
  ESR 2026-06-16) → **peak ~mid-July** → churn decline. The ramp is FIT
  to the observed canonical-DAU rise (28d-MA).
- **Magnitude is data-led** and sits **above** Brad's ~560K-by-Dec-15 — actuals are
  outpacing his conservative ramp (by design). Measured Dec-15 ≈
  **672,559** (vs Brad ~560K); peak ≈ **855,640**.

## Key caveat

`CHURN_ANNUAL` (post-peak decline) is the least data-grounded parameter — post-migration
churn isn't observable yet. Default = Brad's −45%/yr; retune in `build_placeholder_model.py`.

## Swap path

To replace with Brad's official model: drop the official daily-series parquet here with a 'migration_dau_daily' column on a 'target_date' DatetimeIndex; update mozillaonline.json 'data_file' + 'model_meta_file'; set 'placeholder': false; update 'allocation.shares' if the official model carries its own geo split. The applier contract (subtract-from-training / add-back-to-forecast, fixed country shares, within-country split proportional to OS-row DAU) is unchanged.
