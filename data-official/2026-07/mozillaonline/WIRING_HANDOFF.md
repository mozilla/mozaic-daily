# Handoff — wire the `o` (MozillaOnline migration) desktop overlay

**You are a fresh Claude, on branch `july-forecast` in `/Users/brendanwells/work/mozaic-daily`.**
Activate the venv (`source .venv/bin/activate`). Part A (the model artifact) is **done** — see this
directory's `README.md`, the `*.parquet` + `.meta.json` model, `mozillaonline.json`, `summary.html`.

> **UPDATED 2026-07-06.** The bidirectional overlay machinery you need now **already exists and is
> tested** — it was generalized when the launch-on-login `l` overlay was built. You are **not**
> writing new subtract/add appliers from scratch. You are adding a second consumer of the shared
> machinery, using `l` as a working reference. This is real pipeline code with tests — **confirm
> with Brendan before starting.**

## The reference implementation: launch-on-login (`l`)

`l` is a desktop bidirectional overlay that does exactly what `o` does (subtract the historical
tailwind from `legacy_desktop` training so Prophet doesn't extrapolate a one-time event, then add a
curve back to the forecast). Read these — `o` mirrors every one:

- **Appliers (generic, shared with `m`):** `src/mozaic_daily/adjustments.py`
  - `compute_country_shares(training_df, *, training_end_date, window_days, flag_column)`
  - `subtract_lift_from_training(df, *, daily_lift_series, country_shares, flag_column, sentinel_attr)`
  - `add_lift_to_forecast(df, *, daily_lift_series, country_shares, forecast_start, metric_column, population_value)`
  - `load_overlay_spec(path)` (validates `type == "desktop_overlay"`), `load_lift_series(spec, dir)`
- **Spec + artifact:** `data-official/2026-07/launch_on_login/lol.json` + `lol_tailwind.*.parquet` + `_index.md`
- **Pipeline wiring:** `src/mozaic_daily/main.py` — `_find_launch_on_login_spec_for_forecast`,
  `_apply_launch_on_login_pre_mozaic`, the subtract block (gated `data_source == LEGACY_DESKTOP`),
  the add-back block (after `combine_tables`, before `format_func`), threaded through
  `generate_forecasts` / `main` as `lol_spec_path` / `launch_on_login`.
- **CLI:** `--no-launch-on-login` in `scripts/run_main.py`.
- **Tests:** `tests/test_adjustments.py` — the `# Desktop overlay applier` section.
- **Verification:** `scripts/verify_lol_overlay.py` (3-curve isolation + conservatism plot).

## Desktop schema facts (confirmed, hold for `o` too)

- Desktop DAU training rows: `x`, `country`, boolean `modern_windows`, boolean `winX`, `y` (Int64).
  Rows with both False are the `other` (non-Windows) segment.
- After `combine_tables` (before `update_desktop_format`), the granular desktop forecast has
  `target_date`, `country`, `population` ∈ {`modern_windows`, `winX`, `other`, `ALL`}, `source`, `DAU`.
  So `population_value` in the add-back is a segment name, and `country="ALL"`/`population="ALL"` are
  the rollups. This is identical to how `l` is wired.

## The TWO real differences from `l` — and how the generic machinery handles them

1. **Fixed country shares (not computed from trailing DAU).** MozillaOnline is ~92.8% CN by a
   *fixed* spec allocation, not a data-derived share. **No new code needed:** build the
   `country_shares` Series directly from the spec instead of calling `compute_country_shares`:
   ```python
   country_shares = pd.Series(spec["allocation"]["shares"])   # {"CN": 0.9277, "HK": 0.0225, ...}
   ```
   then pass it into the *same* `subtract_lift_from_training` / `add_lift_to_forecast`. Respect
   `scope.exclude_countries` (IR): drop those keys before passing.

2. **Segment scope — the one genuine decision.** `l` lands entirely in `modern_windows`
   (`flag_column="modern_windows"`). Decide where MozillaOnline lands:
   - **If it's modern_windows-only** (China desktop is Windows-heavy — check the model): it's
     *literally the `l` path*. Set `flag_column`/`population_value="modern_windows"` and you are done
     with zero applier changes.
   - **If it must spread across all OS rows within a country** (`within_country: proportional_to_dau`
     in the current `mozillaonline.json`): the single-`flag_column` path won't cover winX/other. Add a
     small multi-segment variant — either loop the generic subtract/add once per segment with
     per-segment country_shares (shares × each segment's within-country DAU fraction), or add a
     `flag_columns=[...]` branch. Keep the copy/sentinel discipline. **Ask Brendan which scope** —
     for the KPI (ALL-desktop total) the country total is what matters; the OS split only changes
     per-tile Prophet fits.

## Steps (mirror `l`)

1. **Reconcile the spec to `desktop_overlay`.** Either (a) change `mozillaonline.json` `type` to
   `desktop_overlay` and express allocation as `{key: "fixed_shares", shares: {...}, flag_column: ...}`,
   or (b) add a thin `load_mozillaonline_spec` wrapper if you want to keep the `mozillaonline_migration`
   type. Prefer (a) so `load_overlay_spec` is reused. Confirm `value_column` = `migration_dau_daily`.
2. **Register code `o`** in `data-official/adjustment_codes.yaml` (`name: mozillaonline_migration`,
   `spec_glob: "data-official/*/mozillaonline/mozillaonline.json"`).
3. **Wire into `main.py`** mirroring the `l` block: `_find_mozillaonline_spec_for_forecast`,
   `_apply_mozillaonline_pre_mozaic` (builds fixed `country_shares` per §1, calls the generic
   subtract with `sentinel_attr="mozillaonline_subtracted"`), gate on `data_source == LEGACY_DESKTOP`,
   add-back after `combine_tables`. Thread `mozillaonline_spec_path` / `mozillaonline: bool` through
   `generate_forecasts` / `main`. **Note the sentinel:** `l` and `o` both touch the desktop frame, so
   `o` MUST use a distinct `sentinel_attr` (the generic subtract already supports this) — both can
   stack.
4. **CLI:** add `--no-mozillaonline` to `scripts/run_main.py`.
5. **Tests** in `tests/test_adjustments.py` mirroring the `l` desktop-overlay tests: fixed-shares
   allocation (CN ~92.8%, IR excluded), subtract world-invariant per date, add-back per-country +
   world rollups, distinct-sentinel stacking with `l`, no mutation.
6. **CLAUDE.md** adjustment table: add the `o` row; filename markers sort alphabetically
   (`.adj-lo.`, `.adj-hlmo.`, etc. — `l` then `m` then `o`).

## Canonical artifact — IMPORTANT sequencing (from Brendan)

Applying a bidirectional overlay requires **re-running the `legacy_desktop` DAU forecast** (the
current canonical desktop was carried forward, not re-run — see
`regenerate_canonical_forecast.py`). **Do the canonical desktop regeneration ONCE, with BOTH `l` and
`o` applied together**, rather than regenerating for `l` now and `o` tomorrow. So: land `o`, then
re-run `legacy_desktop` DAU with both overlays, swap that column into the canonical combined parquet
(extend `regenerate_canonical_forecast.py` the same way it swaps mobile DAU), and update
`july_canonical_v2026-06-29.ipynb` + the sidecar meta so the desktop marker becomes `.adj-lo.` (or
`.adj-hlo.` with the display-layer headwind).

## Definition of done
- `pytest tests/test_adjustments.py -k "mozillaonline or overlay" -v` passes.
- `scripts/verify_lol_overlay.py`-style check for `o`: a `legacy_desktop --metrics DAU` run with vs
  without `--no-mozillaonline` shows the expected CN Dec-15 delta (net of what Prophet already
  extrapolated — expect **less than** the raw +~670K, exactly as `l` netted +102K from a 125K curve).
- CLAUDE.md table + adjustment-codes registry + tests updated.

## Caveats to carry forward
- Magnitude is **data-led** and runs **above** Brad's ~560K (peak ≈856K, Dec-15 ≈673K). Expected
  (actuals outpace his conservative ramp). To anchor to 560K instead, retune `CHURN_ANNUAL` /
  `RESIDUAL_FRACTION` in `build_placeholder_model.py` and rebuild — do NOT patch the applier.
- Measured in **legacy** telemetry; `o` applies to `legacy_desktop` only (same as `l`). Confirm with
  Brendan if glean_desktop should also carry it.
- The placeholder model is a stand-in for Brad's official model; when his lands, swap the parquet
  (same column contract) — the wiring doesn't change.
