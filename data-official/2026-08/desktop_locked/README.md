# Desktop — August 2026 CANONICAL (forecast_start 2026-07-28, s01 config, LOL **200K**)

`legacy_desktop` DAU. **This is the canonical August desktop build.** Promoted 2026-07-29, replacing
the otherwise-identical 180K-ceiling build now at `../desktop_superseded_lol180k_2026-07-28/`.
Not yet a *delivered* forecast — `o` and `m` remain stale carry-forwards; see `../_index.md`.

## Result (28d-MA, post-headwind −1,245,000 ramping from the seam)

| quantity | value | vs the 180K build |
|---|--:|--:|
| **Aug-25 trough minimum** | **45,223,249** | +29,688 |
| Aug-22 | 45,263,042 | +29,150 |
| Sep-15 | 47,138,508 | +19,218 |
| Oct-15 | 48,540,196 | +9,162 |
| Nov-15 | 48,355,229 | +5,251 |
| **Dec-15** | **48,703,960** | **+25,348** |

Net vs July delivered: **+118,477 (+0.24%)**.

The 180K→200K delta is **not** the flat +20,000/day the ceiling difference implies, and is not monotone
in time. `l` is bidirectional — the extra 20,000/day is subtracted from `modern_windows` training rows
before mozaic as well as added back to the forecast — so Prophet refits on a different history and
redistributes the effect. It is also strongly config-dependent: the same ceiling change is worth
**+25,348 under s01 but only ~+7,211 under the previous cycle's flatter config**, because s01's 2.1×
changepoint flexibility responds far more to a modified training series. Never model a ceiling change
as a level shift.

## Configuration — the s01 lock

| param | value | previous cycle's lock |
|---|--:|--:|
| `seasonality_regime` | **multiplicative** | auto |
| `prophet_changepoint_prior_scale` | **0.1849** | 0.08983 |
| `prophet_changepoint_range` | **0.734** | 0.65 |
| `prophet_recent_weeks` | **17** | 13 |
| `prophet_n_changepoints` | **35** | 25 |
| `prophet_seasonality_prior_scale` | 0.00825 | 0.00825 |
| `holiday_threshold` / `max_radius` / `min_radius` / `effect_floor` | −0.032 / 5 / 3 / −0.6 | identical |

Trained through **2026-07-27**. Iran queried natively; the shutdown gap is covered by the mozaic
package's built-in counterfactual fill. `parameters.json` is the authoritative record.

**Holiday parameters are excluded from tuning by policy** — they are strictly local effects, so using
them to move a whole-season quantity is compensating for a trend with a small regional fix.
`[load-parquets]` in the canonical notebook asserts all four sit at package defaults.

## Dec-15 attribution — and what is NOT separable

| | Dec-15 28d-MA | step |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | 48,585,483 | — |
| + data refresh to the 07-28 anchor | 48,520,714 | −64,769 |
| + Win10 headwind −1,345,000 → −1,245,000 | 48,620,714 | +100,000 |
| **+ LOL ceiling 125,000 → 200,000 AND the s01 retune (combined)** | **48,703,960** | **+83,246** |

**The LOL and retune effects are not separated, deliberately.** Isolating the retune requires a build
identical to this one in every input except the model config. One exists at the 180K ceiling — the
frozen `../desktop_baseline_2026-07-28/` comparison point — but not at 200K, and **historical builds are
locked: they are never re-run, rebuilt, or replaced to manufacture one.** Published deltas were quoted
against those artifacts, and a chain whose links move cannot be audited. A combined step labelled as
combined beats a clean-looking number obtained by regenerating history.

**The retune's own like-for-like evidence** is in
`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`, which measures it against the
previous config on an identical 180K curve: trough +1,359,887 for +5,642 at Dec-15. Quote the retune
effect from there, not from this ledger. That notebook is deliberately left on 180K builds — repointing
it at 200K would confound the config evidence with the ceiling change.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login, **200K ceiling**) + `o` (MozillaOnline, stale carry-forward) applied bidirectionally
  on `modern_windows`. **Pre-headwind** — `h` is display-layer, applied in the canonical notebook.
- `…meta.json` — sidecar provenance (model config, adjustments + spec sha1s, commit). The `l` spec sha1
  is `e8b4a218…`; the 180K build's was `e23a6267…`.
- `parameters.json` — the exact `DesktopModelConfig` used.
- `mozaic_objects.legacy_desktop.2026-07-28.pkl` — fitted state (634MB, gitignored).
- `mozaic_parts.raw.legacy.desktop.DAU.parquet` — symlink to the shared BQ pull under
  `../desktop_baseline_2026-07-28/…`. Gitignored; archive to GCS at button-down.

## Known caveats

- **More flexible trend model than has ever shipped** — `cps` 2.1× the previous lock, `ncp` 1.4×,
  `cpr` 0.734 vs 0.65.
- **Sits on a bend, not a plateau** — `changepoint_range` dominates and its curvature is 2.4× its own
  slope. Do not extrapolate the local gradient; measure.
- **Autumn is unvalidated** — the Oct–Nov plateau runs ~250–400K above the pre-retune shape and
  converges by Dec-15 only because Dec-15 is the constrained point.
- **The LOL ceiling is unfalsifiable.** Measured data stops at 130,296 on 2026-06-23; the holdback
  control received the feature on 06-24, so no telemetry can ever adjudicate 165K vs 180K vs 200K. 200K
  is the least conservative of the three (~20K/day haircut against a ~220K convolution model).
- **The Win10 headwind magnitude is a forward judgement.** No excess decline is detectable in the
  Win10-exposed cohort against either its prior-year analogue or concurrent Mac/Linux; the anchor is
  only justified if attrition accelerates. See `research/headwinds/`.

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

Requires `lol.json` pointing at the `.cap200k` curve and the sibling mozaic checkout on
`configurable-model-params` at `4f33650` or later. `run_main.py` cannot reproduce this config — it has
no parameter flags.
