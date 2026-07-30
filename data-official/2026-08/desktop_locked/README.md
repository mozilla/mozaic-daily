# Desktop — August 2026 LOCKED (forecast_start 2026-07-28, s01 config, LOL 180K)

`legacy_desktop` DAU. **This is the canonical August desktop build.** Locked 2026-07-29.
Not yet a *delivered* forecast — `o` and `m` remain stale carry-forwards; see `../_index.md`.

## Result (28d-MA, post-headwind)

| quantity | value | vs superseded |
|---|--:|--:|
| **Aug-25 trough minimum** | **45,193,561** | **+1,359,887** |
| Aug-15 | 45,675,070 | +1,042,168 |
| **Dec-15** | **48,678,612** | **+5,642** |
| seam slope kink (model) | −20,604/day | +51,989 (72% smaller) |

Dec-15 sits **+5,642** from the previous canonical — 11% of the ±50,000 budget that was the binding
constraint on adopting any retune. Room remaining: 55,642 down, 44,358 up.

## Configuration — the s01 lock (NOT July's)

| param | value | July's lock |
|---|--:|--:|
| `seasonality_regime` | **multiplicative** | auto |
| `prophet_changepoint_prior_scale` | **0.1849** | 0.08983 |
| `prophet_changepoint_range` | **0.734** | 0.65 |
| `prophet_recent_weeks` | **17** | 13 |
| `prophet_n_changepoints` | **35** | 25 |
| `prophet_seasonality_prior_scale` | 0.00825 | 0.00825 |
| `holiday_threshold` / `max_radius` / `min_radius` / `effect_floor` | −0.032 / 5 / 3 / −0.6 | identical |

Trained through **2026-07-27**. Iran queried natively; shutdown gap covered by the mozaic package's
built-in counterfactual fill.

**Provenance.** s01 is the 28-point Latin-hypercube winner from *July's own* Aug-trough search
(`research/param-scans/aug22-retune/`), which measured it at Aug 44.675M / Dec-15 −60,032 and did **not**
adopt it — that search's deliverable was a negative result. Rebuilt on August data it satisfies all three
August objectives at once. Sensitivity gradient and the promotion evidence live in
`research/param-scans/summer-trough-v2/`.

**Holiday parameters are excluded from tuning by policy** — they are strictly local effects, so using
them to move a whole-season quantity is compensating for an overall trend with a small regional fix. The
canonical notebook's `[load-parquets]` asserts all four are at defaults, and
`scripts/run_trend_only_grid.py` refuses holiday overrides.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login, **180K ceiling**) + `o` (MozillaOnline, stale July carry-forward) applied
  bidirectionally on `modern_windows`.
  **Pre-headwind** — the Win10 headwind `h` is display-layer, applied in the canonical notebook.
- `…meta.json` — sidecar provenance (model config, adjustments + spec sha1s, commit).
- `parameters.json` — the exact `DesktopModelConfig` used.
- `mozaic_objects.legacy_desktop.2026-07-28.pkl` — fitted state. **Hard-linked** to the same inode under
  `research/param-scans/summer-trough-v2/s01_gradient/<slug>/` rather than duplicated (634MB).
- `mozaic_parts.raw.legacy.desktop.DAU.parquet` — symlink to the shared BQ pull under
  `../desktop_baseline_2026-07-28/…`. Gitignored; archive to GCS at button-down.

## Dec-15 attribution vs July delivered

| | Dec-15 28d-MA | step |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | 48,585,483 | — |
| + data refresh to the 07-28 anchor | 48,520,714 | −64,769 |
| + LOL ceiling 125,000 → 180,000 (derived residual) | 48,572,970 | +52,256 |
| + Win10 headwind −1,345,000 → −1,245,000 | 48,672,970 | +100,000 |
| **+ desktop model retune to s01 (measured)** | **48,678,612** | **+5,642** |

Net **+93,129 (+0.19%)**. The retune step is *measured* against the superseded build
(`../desktop_baseline_2026-07-28/…`), which is identical in data, overlays and headwind — that is why it
is attributable. Only the LOL step is a residual.

## Known caveats

- **More flexible trend model than has ever shipped** — `cps` 2.1× July's, `ncp` 1.4×, `cpr` 0.734 vs
  0.65. Weaker form of the objection that ruled out holiday tuning.
- **Sits on a bend, not a plateau** — `changepoint_range` dominates (+102,862 trough per +10% of center)
  and its curvature is 2.4× its own slope. Do not extrapolate the local gradient; measure.
- **Autumn is unvalidated** — the Oct–Nov plateau runs ~250–400K above the superseded build. It converges
  by Dec-15 because Dec-15 is the constrained point, not because the model was fit to be right there.
- **A residual seam step remains (+102,595) and is a display artifact** — `reconstruct_matched_daily`'s
  7-day centered trend degenerates to a weekday-only forward window at the seam. Not the old headwind
  step, which is fixed. Headline numbers unaffected; the seam-kink magnitude is contaminated. See
  `research/ma-seam-turbulence/diagnose_recon_edge_bias.py` and the canonical notebook's caveats.

## Reproduce

```bash
source .venv/bin/activate
python scripts/run_param_scan.py \
  --forecast-start-date 2026-07-28 \
  --raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825 \
  --results-dir <scratch> \
  --seasonality-regime multiplicative \
  --changepoint-prior-scale 0.1849 --changepoint-range 0.734 \
  --recent-weeks 17 --n-changepoints 35 --seasonality-prior-scale 0.00825 \
  --holiday-threshold -0.032 --holiday-max-radius 5 --holiday-min-radius 3 \
  --holiday-effect-floor -0.6
```

Requires the sibling mozaic checkout on `configurable-model-params` at `4f33650` or later
(`seasonality_regime`, `seasonality_prior_scale`, and the `to_slug` injectivity fix).
`run_main.py` cannot reproduce this config — it has no parameter flags.
