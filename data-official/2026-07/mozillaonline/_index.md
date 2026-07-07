# `data-official/2026-07/mozillaonline/` — MozillaOnline migration overlay (`o`)

**WIRED (2026-07-07).** The MozillaOnline → Firefox desktop migration is a bidirectional overlay
(subtract from training before mozaic, add back after), adjustment code `o`, applied to
`legacy_desktop` DAU on the `modern_windows` segment. The migrating build is telemetry
`app_name = "Firefox Desktop MozillaOnline"`; migration flips users to `app_name = "Firefox Desktop"`
(the canonical KPI). ~93% China. Now folded into the canonical `…adj-lmo.parquet`.

**Active model = Brad's OFFICIAL model** (`mozillaonline_migration_model.official.2026-06-29.*`,
Dec-15 28d-MA ~567K). The data-grounded placeholder (~673K) is retired but kept for provenance.

## What's here

**Active (official model):**
- **`source_data/mozilla_online_forecast_jul.csv`** — Brad's official model export (daily `dau`, `dau_28ma`).
- **`build_official_series.py`** — reproducible: CSV → horizon-spanning parquet (held flat ~550K into 2027).
- **`mozillaonline_migration_model.official.2026-06-29.parquet`** (+ `.meta.json`) — daily
  `migration_dau_daily` + `migration_dau_ma` on a `target_date` index; the artifact the `o` applier reads.
- **`mozillaonline.json`** — spec: `type: desktop_overlay`, `value_column`, fixed geo `allocation.shares`
  (`flag_column: modern_windows`), `scope.exclude_countries: [IR]`, `applies_to_forecast_start: 2026-06-29`,
  `placeholder: false`.
- **`plots/`** — `mozillaonline_three_curve_isolation.png` (o-alone net effect) + migration curve
  (from `scripts/verify_mozillaonline_overlay.py`).

**Retired placeholder (provenance):**
- **`build_placeholder_model.py`**, **`mozillaonline_migration_model.placeholder.2026-06-25.*`**,
  **`README.md`**, **`summary.html`**, **`*.png`**, **`source_data/cn_*.csv` + `mozonline_geo_windows.csv`**.

**Handoffs (superseded):** `WIRING_HANDOFF.md` (Part B — now done), `PLACEHOLDER_MODEL_HANDOFF.md`.

Tests: `tests/test_mozillaonline_model.py` (placeholder artifact contract) + the `o` overlay section of
`tests/test_adjustments.py` (fixed shares, subtract/add, l+o stacking).

## Where new code goes
- Swap a newer official model → drop the CSV in `source_data/`, re-run `build_official_series.py`,
  bump `data_file`/`model_meta_file` in `mozillaonline.json`. Applier contract unchanged.
- OS scope is **modern_windows-only by measurement** — see the official meta `os_scope` block.
- Applier code → `src/mozaic_daily/adjustments.py` (`fixed_country_shares_from_spec`); NOT here.
