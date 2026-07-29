# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`). **Freshly opened — nothing produced yet.**

## Status

Empty scaffold. No forecast has been run for this cycle; there is no canonical output, no
`parameters.json`, and no adjustment specs yet. Until this section is replaced by a
"Current usable working set" list like July's, **the authoritative production numbers are still
July's** (`../2026-07/`, forecast_start 2026-07-06, desktop Dec-15 28d-MA 48,585,483 /
mobile 17,923,869).

## Expected layout

Mirrors the previous cycles — populate as the cycle progresses:

```
2026-08/
  desktop_<config-slug>/       # legacy_desktop DAU forecast + sidecar + parameters.json
  mobile_<config-slug>/        # glean_mobile DAU forecast + sidecar + parameters.json
  adjustments/headwind.json    # h — Win10 headwind ramp
  launch_on_login/lol.json     # l — desktop launch-on-login tailwind
  mozillaonline/*.json         # o — CN desktop migration
  marketing/marketing.json     # m — mobile marketing lift
  iran_fill/                   # Iran counterfactual-fill specs (package copy is authoritative)
  csv/august_canonical_curves.csv
  kpi_sheet/
  TODO_factors.md              # the cycle's planning doc — create this first
  _index.md
```

Every artifact needs its `.raw.` / `.adj-{codes}.` state marker and a sidecar `.meta.json`; write
them via `mozaic_daily.adjustments` (`insert_state_marker`, `write_meta`) and load through
`load_forecast`. See `../_index.md` and the root `CLAUDE.md`.

## Inherited from July — read before starting

- **`../2026-07/TODO_factors.md`** is the July planning doc. The August equivalent should start as a
  diff against it, not from scratch: carry forward the factors still live (Win10 headwind resize,
  the `l` / `o` / `m` overlays, Iran fill now that IR has fully recovered, telemetry opt-out) and
  retire the ones July settled.
- **Open question — the summer-trough shortfall.** `research/param-scans/aug22-retune/` established
  that **no exposed parameter combination lifts the Aug-2026 trough to target while holding Dec-15**
  (best sampled point still 0.385M short; `seasonality_regime=multiplicative` gets ~71% of the lift
  but plateaus and pushes Dec over band). Its recommendation is a **bidirectional summer-trough
  overlay** in the `l`/`o`/`m` family — subtract from training pre-mozaic, add back post-forecast,
  tapering to ~0 by Nov/Dec so the winter peak is held by construction. **This is a recommendation,
  not a decision** — it needs a go/no-go before any overlay is built. The target shape lives in
  `research/summer-slump/`.
- **Near-horizon scoring tooling** carried onto this branch: `scripts/score_near_horizon.py`
  (+ tests) and `scripts/run_aug_trough_gradient.py`. Both still default to July's anchor —
  repoint `DEFAULT_TARGET_DATE` / `DEFAULT_HEADWIND` and the driver's `FORECAST_START` /
  `RESULTS_ROOT` / probe lists before reuse.
- **`../2026-06/` is retained on purpose** even though it is N-2 — July reads several of its specs
  and curves. See `../_index.md` for the list and the September revisit note.

## Dependency note

The `seasonality_prior_scale` / `seasonality_regime` knobs used by the Aug-trough search require
`mozaic-forecasting-official` @ `configurable-model-params` (commits `126fe14`, `6f02912`), which is
**not pushed to that repo's origin**. Anything reproducing that search needs the local checkout.
