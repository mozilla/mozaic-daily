# `o` — mozillaonline_migration, cycle 2026-09

**What it is:** The MozillaOnline → canonical Firefox desktop migration: the China distribution partner is moving its users onto mainline Firefox, and migrating users flip `app_name` and newly count as Firefox Desktop DAU. This is the **September refresh** of the curve — the official model export delivered 2026-09-02 (`source_data/mozilla_online_forecast_2026-09-02.csv`), replacing the July curve that August had carried forward stale (`../../2026-08/mozillaonline/`, frozen and still gated to 2026-08-02). Actuals run to 2026-08-31, one day short of the seam; the file's own forecast covers that day, which is allowed. The pipeline learns modern_windows without the migration and stacks the curve back on at face value, allocated by the fixed geo footprint carried from August (CN 92.77%, then HK, US, JP, SG, TW, DE, AU, ROW; IR excluded). Producer: Brad Ochocki Szasz's MozillaOnline model; the format is the `templates/tailwind/` contract exactly.

**Family:** per-tile overlay: subtracted from training rows before mozaic and added back after; **needs a model re-run**. **Platform:** desktop (`legacy_desktop`). **Sign:** tailwind (+).

## Files

| file | role |
|---|---|
| `mozillaonline/mozillaonline.json` | the spec, gated on `applies_to_forecast_start: 2026-09-02` |
| `mozillaonline_migration.2026-08-31.parquet` | what the pipeline loads: `mozillaonline_migration_dau_daily` on a `target_date` DatetimeIndex, `mozillaonline_migration_dau_ma`, `source` |
| `mozillaonline_migration.2026-08-31.meta.json` | provenance: source sha1, column mapping, coverage, hold-flat rule, checks |
| `source_data/mozilla_online_forecast_2026-09-02.csv` | the delivered file, byte for byte |
| `plots/mozillaonline_migration.2026-08-31.curve.png` | the curve's shape: daily + 28d mean, measured / projected / held, seam and Dec-15 marked |

## Coverage

| | |
|---|---|
| delivered | 2026-06-01 → 2026-12-31 |
| actuals through | 2026-08-31 |
| held flat from | 2027-01-01 at 644,169/day (mean of the final 28 delivered daily values) |
| horizon | 2026-01-01 → 2027-12-31 |
| Dec-15 28d MA | 668,839 |

## Allocation

Localized: fixed country shares {"CN": 0.9277, "HK": 0.0225, "US": 0.0152, "JP": 0.0098, "SG": 0.0083, "TW": 0.0041, "DE": 0.0011, "AU": 0.001, "ROW": 0.0103}; excluded: ['IR'].

## What is measured and what is assumed

| quantity | status |
|---|---|
| daily migration DAU 2026-06-01 → 2026-08-31 | **measured** (producer's telemetry, `type=actuals`) |
| 2026-09-01 → 2026-12-31 | **producer's model** (`type=forecast`) |
| 2027-01-01 → 2027-12-31 | **held flat** by us at the final 28-day mean, 644,169/day — no model covers 2027 |
| geo shares | **carried from August's spec**, not re-measured this cycle |
| modern_windows-only | by August's measurement (older-Windows users are pinned on Firefox too old to receive the migrating build) |

## Versus the curve August carried

| | August (July curve, stale) | September (this file) |
|---|--:|--:|
| daily at the seam | 705,104 | 916,569 |
| Dec-15 28d-MA | 567,549 | **668,839** |
| held-flat level (2027) | 550,268 | 644,169 |

The Dec-15 change in the curve is **+101,290**. Because `o` is subtracted from training and added back, the
realised effect on the published number is an empirical question answered by the rerun, not this delta.

## Where new files go

A refreshed curve for this cycle: re-run the ingest with `--replace`; the previous build moves to `mozillaonline_migration_REVERT_<date>/`. Cross-cycle analysis of this effect goes to `research/`.
