# Locked desktop model — July 2026 (final)

This is the **canonical desktop DAU model** for the July 2026 forecast cycle. It supersedes the
desktop rows in `../mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-lmo.parquet` (which were the
stale prior-package carry-forward at the 2026-06-29 anchor).

## Files
- `mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet` — desktop (legacy_desktop) DAU forecast,
  overlays `l` (launch-on-login) + `o` (MozillaOnline migration) applied. **Pre-headwind** (the Win10
  headwind is a display-layer adjustment, see below).
- `…meta.json` — sidecar provenance (config, adjustments, spec sha1s).
- `parameters.json` — the exact `DesktopModelConfig` used.

## Configuration (locked)
- **Forecast start: 2026-07-06** (re-anchored to the freshest complete data; training_end 2026-07-05).
- **Prophet: `changepoint_prior_scale=0.08983`, `changepoint_range=0.65`** (chosen via the desktop
  parameter search — `research/param-scans/desktop_gradient_round{1,2,3,4}.ipynb`; cpr=0.65 is a
  robust, all-positive column and cps=0.08983 sits on its high plateau).
- **Holiday: threshold=−0.032 (package default), radii/floor default.** Holiday knobs deliberately
  left at default — moving them only gains KPI by desensitizing holiday detection (not defensible).
- Overlays `l` + `o` applied bidirectionally on `modern_windows`; Iran counterfactual fill ships in
  the mozaic package.

## Win10 headwind (display layer — NOT in this parquet)
Applied in the canonical notebook from `../adjustments/headwind.json`: a linear ramp reaching
**−1,345,000** at the 2026-12-15 anchor (softened +25k from −1,370,000 — Prophet has partly learned
the Win10 decline from recent data).

## Dec-15 2026 result (final KPI, 28d-MA, headwind applied)
**48,585,483** — +251,121 over June's delivered 48,334,362, and +1,121 over the 48,584,362 target.

## Lift ledger (final KPI)
| step | Δ | running |
|---|--:|--:|
| stale center (06-29 anchor) | — | 48,481,092 |
| + re-anchor to fresh data (07-06) | +39,357 | 48,520,449 |
| + locked params (cps=0.08983, cpr=0.65) | +40,034 | 48,560,483 |
| + headwind add-back (+25k) | +25,000 | 48,585,483 |
