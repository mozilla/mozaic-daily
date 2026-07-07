# `data-official/2026-07/mozillaonline/` — MozillaOnline migration overlay

Inputs for the `o` adjustment (planned): the MozillaOnline → Firefox desktop
migration modeled as a bidirectional overlay (subtract from training before
mozaic, add back after), same OUTPUT pattern as marketing-lift `m`. The migrating
build is telemetry `app_name = "Firefox Desktop MozillaOnline"`; migration flips
users to `app_name = "Firefox Desktop"` (the canonical KPI). ~93% China; ~7% tail
(likely VPN / diaspora).

## What's here

**Built (Part A — data-grounded placeholder):**
- **`build_placeholder_model.py`** — reproducible generator; tunable constants at top.
- **`mozillaonline_migration_model.placeholder.2026-06-25.parquet`** (+ `.meta.json`) —
  daily `migration_dau_daily` + `migration_dau_ma` on a `target_date` index.
- **`mozillaonline.json`** — spec (data_file, value_column, geo allocation shares, scope,
  applies_to_forecast_start, placeholder flag).
- **`README.md`** — model summary + swap path.
- **`summary.html`** — self-contained proof doc for a reviewing DS.
- **`*.png`** — curve, decomposition, conservation/fit, cohorts, geo.
- **`source_data/*.csv`** — cached pre-June + migration-window telemetry inputs (legacy).

**Handoffs:**
- **`WIRING_HANDOFF.md`** — spec for a future agent to wire the `o` applier + tests (Part B).
- **`PLACEHOLDER_MODEL_HANDOFF.md`** — original handoff (predates Brad's numbers; superseded by
  the data-grounded build, kept for provenance).

Tests: `tests/test_mozillaonline_model.py` (artifact contract).

## Where new code goes
- Model retunes → edit constants in `build_placeholder_model.py` and re-run (deterministic).
- Brad's official model → swap per `mozillaonline_migration_model.*.meta.json` `swap_instructions`.
- Applier code → `src/mozaic_daily/adjustments.py` (see `WIRING_HANDOFF.md`); NOT here.
