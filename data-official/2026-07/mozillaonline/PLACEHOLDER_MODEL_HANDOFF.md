# Handoff — Build a PLACEHOLDER MozillaOnline migration model

**You are a fresh Claude.** Read this whole file, then build the deliverable described in **Part A**. Part B is an optional stretch. Work on branch `july-forecast` in `/Users/brendanwells/work/mozaic-daily`. Activate the venv (`source .venv/bin/activate`) before running Python.

## Background (what this is)

MozillaOnline (Mozilla's China partner / distribution) is migrating its users onto mainline Firefox desktop. This shows up as **added desktop DAU**, concentrated in China. We model it as an **overlay** on the forecast, identical in structure to the existing **marketing-lift `m` adjustment** (your primary reference — see "Reference implementation" below):

1. **Subtract** the migration DAU from desktop training rows before mozaic, so Prophet learns the *pre-migration* dynamic and doesn't bake the migration ramp into its trend.
2. Run the mozaic forecast.
3. **Add** the migration DAU back to the forecast rows, so the delivered curve reflects "migration continues at modeled levels."

**This task builds a PLACEHOLDER.** Brad is producing the official migration model; it isn't ready. We need a stand-in now so the July pipeline can run end-to-end, and so that when Brad's model lands it's a **drop-in swap** (same file/column contract). Optimize for: conservative magnitude, tunable parameters, and a clean swap — not for accuracy.

### Geography (important)
- **>90% of the migration is China (CN).**
- The remaining ~10% is spread across other countries — **likely VPN users** appearing to be elsewhere.
- So the overlay is allocated across countries by a **fixed share map**, dominated by CN with a small distributed tail.

### Conservatism / validation note
Our migration ramp is **deliberately conservative** — slower than what actuals already show. The forecast is therefore *expected to sit below* realized CN DAU, and we **do not expect them to match**. Do not "fix" the ramp to chase actuals; err on the low side.

## Reference implementation — study this first

The marketing-lift `m` adjustment is the same bidirectional overlay pattern. Mirror its conventions exactly so this is consistent and swappable.

- Spec example: `data-official/2026-06/marketing/marketing.json`
- Model artifact + sidecar: `data-official/2026-06/marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet` (+ `.meta.json`)
- Doc: `data-official/2026-06/marketing/README.md`
- Applier code: `src/mozaic_daily/adjustments.py`, section **"Per-tile marketing-lift applier"** (~line 337) — `load_marketing_spec`, `load_marketing_lift_series`, `subtract_marketing_lift_from_training`, `add_marketing_lift_to_forecast`.
- Registry: `data-official/adjustment_codes.yaml` (codes `h`, `m`).
- File-naming/meta conventions: `src/mozaic_daily/adjustments.py` (`write_meta`, `_sha1_file`) and the repo-root `CLAUDE.md` "Forecast Artifact Naming Convention" section.

Inspect the marketing parquet to copy its shape:
```python
import pandas as pd
df = pd.read_parquet("data-official/2026-06/marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet")
print(df.columns.tolist(), df.index.name, df.shape)  # ['marketing_lift_daily','marketing_lift_ma'], 'target_date'
```

---

## Part A (PRIMARY) — the placeholder migration model artifact

Produce three files in `data-official/2026-07/mozillaonline/`:

### A1. Daily migration-DAU series — `mozillaonline_migration_model.placeholder.<RUN_DATE>.parquet`
- **Index:** `target_date`, a daily `DatetimeIndex` normalized to midnight.
- **Span:** from a training-history start (use `2026-01-01`, or earlier if convenient) **through the forecast horizon end**. Get the horizon from runtime config rather than hardcoding:
  ```python
  import sys; sys.path.insert(0, "src")
  from mozaic_daily.config import get_runtime_config
  rc = get_runtime_config(forecast_start_date_override="<RUN_DATE>")
  print(rc["forecast_start_date"], rc["forecast_end_date"])
  ```
  (June's marketing series ran `2026-02-01 → 2026-12-31`; match whatever `forecast_end_date` is for the July run.)
- **Columns:**
  - `migration_dau_daily` (float) — **total** migrated DAU added across all countries on that date (the per-country split is handled by the spec's allocation map, exactly like marketing's daily total + country shares).
  - `migration_dau_ma` (float) — 28-day moving average of the above, for plotting parity with the marketing artifact.
- **Shape of the curve (placeholder, tunable):**
  - Zero before `MIGRATION_START`.
  - A **conservative, slow ramp** from `MIGRATION_START` to a steady state — a gentle logistic or a slow linear ramp reaching `STEADY_STATE_DAU` over ~6–9 months. Keep it smooth (no cliffs) so the subtract/add-back doesn't inject changepoints.
  - Define these as clearly-named top-of-script constants so they're trivial to retune:
    - `MIGRATION_START` — **confirm with Brendan; unknown.** Migration has reportedly already begun (actuals outpace our ramp as of 2026-06), so pick a start in early/mid 2026 and flag it as a placeholder assumption.
    - `STEADY_STATE_DAU` — the big unknown. This is the eventual total migrated DAU. **Err low (conservative).** Sanity-check the scale against CN Firefox desktop DAU so it's proportionate, e.g. read `data-official/2026-06/csv/per_country/june_canonical_curves.CN.no-headwinds.csv` for CN's current desktop DAU level and pick a steady state that's a modest fraction of it. Document the number and your reasoning loudly in the meta + notes.
    - `RAMP_MONTHS`, ramp shape — your conservative choice; document it.

### A2. Spec — `mozillaonline.json`
Mirror `marketing.json`, adapted for desktop + country allocation:
```json
{
  "type": "mozillaonline_migration",
  "platform": "desktop",
  "data_file": "mozillaonline_migration_model.placeholder.<RUN_DATE>.parquet",
  "value_column": "migration_dau_daily",
  "allocation": {
    "key": "fixed_country_shares",
    "shares": { "CN": 0.92, "US": 0.02, "DE": 0.02, "JP": 0.02, "...": "..." },
    "within_country_os": "proportional_to_dau"
  },
  "scope": { "exclude_countries": ["IR"] },
  "model_meta_file": "mozillaonline_migration_model.placeholder.<RUN_DATE>.meta.json",
  "applies_to_forecast_start": "<RUN_DATE>",
  "placeholder": true,
  "notes": "PLACEHOLDER MozillaOnline migration overlay. >90% China; ~10% tail (likely VPN) across other countries. Conservative ramp — expected to undershoot CN actuals by design. Swap for Brad's official model when available (see meta.json swap instructions). Subtracted from desktop training rows by country share, added back to per-country + ALL forecast rows."
}
```
- `shares` must sum to **1.0** with CN ≥ 0.90. Pick a small, sensible tail (a handful of countries) — it's a placeholder.
- `within_country_os: "proportional_to_dau"` documents the intended Part-B rule: split a country's migration DAU across its `modern_windows` / `winX` / other rows in proportion to each row's existing DAU.

### A3. Sidecar meta — `mozillaonline_migration_model.placeholder.<RUN_DATE>.meta.json`
Provenance + tunables, mirroring the marketing model meta. Must include:
- `"placeholder": true`
- the parameter values used (`MIGRATION_START`, `STEADY_STATE_DAU`, `RAMP_MONTHS`, ramp shape, geo shares)
- the reasoning for `STEADY_STATE_DAU` (what you sized it against)
- mozaic-daily git commit (`git rev-parse HEAD`)
- a **`"swap_instructions"`** string: "To replace with the official model: drop the official daily-series parquet here with a `migration_dau_daily` column on a `target_date` index; update `mozillaonline.json` `data_file` + `model_meta_file`; set `placeholder: false`; update `allocation.shares` if the official model carries its own geo split."

### A4. Sanity plot + a short README.md
- Write `mozillaonline/README.md` (mirror the marketing README): what's here, the bidirectional pattern, the placeholder caveat, the swap path, and the allocation.
- Render a quick PNG of `migration_dau_daily` and its 28d MA (zero → ramp → steady state) to `mozillaonline/placeholder_curve.png` so Brendan can eyeball the shape. Follow the repo's plotting rules in `~/.claude/CLAUDE.md` (distinguishable tick labels; date axis `%b %d` / `%Y-%m-%d`).

### A5. Make it reproducible
Put the generator in a **named, versioned script** (NOT under `tmp/`) — e.g. `data-official/2026-07/mozillaonline/build_placeholder_model.py` — with the tunable constants at the top. It takes no required inputs and deterministically writes A1–A4. (Per `~/.claude/CLAUDE.md`: reusable scripts never live under `tmp/`.)

---

## Part B (STRETCH, only if time) — wire the `o` adjustment into the pipeline

This makes the placeholder actually usable in a forecast run. It mirrors the `m` applier but is **country-allocated** and **desktop-shaped**. Do this only after Part A is done and verify with Brendan first — it's real pipeline code with tests.

Differences from the `m` applier you must handle:
- **Desktop training schema** is `x` (date32), `country` (str), `modern_windows` (bool), `winX` (bool), `y` (int64). There are **no app-flag columns**; a country has up to 3 OS rows (modern_windows / winX / other = both False).
- **Allocation is by fixed country shares from the spec**, NOT a trailing-DAU share computed from data (so there's no `compute_*_shares` step — read `allocation.shares` directly).
- **Within a country**, distribute that country's migration DAU across its OS rows **proportional to each row's DAU** in the relevant window (`within_country_os: "proportional_to_dau"`).
- **Forecast DataFrame** (after `combine_tables`, before `update_desktop_format`): `target_date`, `country`, `population`, `source`, metric columns. Add back to per-country rows and the `country="ALL"` / `population="ALL"` rollups, symmetric to `add_marketing_lift_to_forecast`.

Steps:
1. Register code **`o`** (name `mozillaonline_migration`) in `data-official/adjustment_codes.yaml` with a `spec_glob` of `data-official/*/mozillaonline/mozillaonline.json`.
2. Add a clearly-labeled **"Per-tile MozillaOnline migration applier"** section to `src/mozaic_daily/adjustments.py`: `load_mozillaonline_spec`, `load_migration_series`, `subtract_migration_from_training`, `add_migration_to_forecast`. Keep the idempotency sentinel pattern (`df.attrs[...]`) and "return a copy, never mutate" discipline from the `m` functions.
3. Add tests in `tests/test_adjustments.py` mirroring the marketing tests — assert subtract→add-back is symmetric per (date, country) on a synthetic desktop frame, and that CN gets ~92% of the total.
4. Combined filename markers will sort alphabetically: headwinds+marketing+mozillaonline → `.adj-hmo.`, headwinds+mozillaonline → `.adj-ho.`.

## Definition of done (Part A)
- The four artifacts in `data-official/2026-07/mozillaonline/` exist and are internally consistent (spec `data_file`/`value_column`/`model_meta_file` point at real files; shares sum to 1.0).
- `load_marketing_lift_series`-style loading works on your parquet (DatetimeIndex, unique, sorted, value column present).
- The plot shows a smooth zero→ramp→steady-state curve.
- Every placeholder assumption is documented in the meta `notes` + README. Report the chosen `MIGRATION_START` and `STEADY_STATE_DAU` back to Brendan so he can confirm against Brad's numbers.

## Open items to confirm with Brendan before/while building
- `MIGRATION_START` (when did/does migration begin?).
- `STEADY_STATE_DAU` ballpark (how many total users migrating?).
- Whether the ~10% non-CN tail should be a few named countries or spread wider.
