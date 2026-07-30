# Desktop — August 2026, SUPERSEDED (forecast_start 2026-07-28, July's params, LOL 180K)

`legacy_desktop` DAU. **SUPERSEDED 2026-07-29** by `../desktop_locked/`, which carries the s01 retune
and a 200K LOL ceiling. **FROZEN — never re-run, rebuilt or replaced.** Kept on disk deliberately: its
raw BQ pull is the shared cache every scan symlinks, and its Dec-15 below is the anchor point the
ledger's retune step is pinned from. Do not delete it before button-down.

> **Role note, corrected 2026-07-29.** This file previously said the canonical notebook *loads* this
> build to measure the retune step. It no longer does — every ledger step is now a pinned constant, so
> the notebook has no dependency on this path (the parquet is gitignored and GCS-bound, which is why).
> The retune step is still *derived from* this build's Dec-15, measured against the s01 build at the same
> 180K ceiling; the like-for-like comparison lives in
> `research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`. **No number in this file changed.**

Its Dec-15 (**48,672,970**) and Aug-25 trough (**43,833,674**) are the baseline every August delta is
quoted against.

⚠️ **The directory name is a misnomer.** It originally held the 125K-LOL *baseline*, which the
2026-07-29 LOL-165K rebuild **overwrote in place** (deliberately — 125K was July's number). The name is
kept because the canonical notebook and the committed sidecar reference this path. The superseded
baseline's Dec-15 was **48,520,714**, preserved here and in git commit `140412e`.

Single config subdir: `cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/`.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login, **180K ceiling**) + `o` (MozillaOnline migration, stale July carry-forward) applied
  bidirectionally on `modern_windows`.
  **Pre-headwind** — the Win10 headwind is a display-layer adjustment applied in the canonical notebook.
- `…meta.json` — sidecar provenance (model config, adjustments + spec sha1s, commit).
- `parameters.json` — the exact `DesktopModelConfig` used.
- `mozaic_objects.legacy_desktop.2026-07-28.pkl`, `mozaic_parts.raw.legacy.desktop.DAU.parquet` —
  fitted state and the pre-forecast BQ aggregate. Gitignored; archive to GCS at button-down.

## Configuration — July's lock, unchanged

`cps=0.08983`, `changepoint_range=0.65`, `n_changepoints=25`, `recent_weeks=13`,
`holiday_threshold=−0.032`, `max_radius=5`, `min_radius=3`, `effect_floor=−0.6`.

Identical to `../../2026-07/desktop_locked/parameters.json`. The `_sps0.00825` in the slug is the
newly-exposed `seasonality_prior_scale` at its default, which reproduces the value July hardcoded — so
the slug differs from July's while the behaviour does not.

Trained through **2026-07-27**. Iran queried natively; shutdown gap covered by the mozaic package's
built-in counterfactual fill.

## Result (Dec-15 2026, 28d-MA, headwind applied)

**48,672,970** — **+87,487 (+0.18%)** vs July's delivered 48,585,483.

Aug-22 summer trough: **43,921,488** (post-headwind, seam-anchored ramp; it was 43,453,752 under the superseded 2026-04-01 ramp start).

Attribution ledger:

| | Dec-15 28d-MA | step |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | 48,585,483 | — |
| Aug baseline (125K LOL, hw −1,345,000) — superseded | 48,520,714 | −64,769 data refresh |
| Aug, LOL 180K (hw −1,345,000) — implied | 48,572,970 | +52,256 LOL ceiling |
| **Aug current (180K LOL, hw −1,245,000)** | **48,672,970** | +100,000 headwind |

**Only the LOL step involved this parquet.** Raising the ceiling required rebuilding it (`l` is baked in);
neither headwind change did — `h` is applied to the 28-day MA in the canonical notebook, never to the
training frame. The amplitude change moves Dec-15 by exactly the anchor delta; the ramp re-anchoring
(`start_date` 2026-04-01 → 2026-07-28, applied 2026-07-29) moves Dec-15 by **zero** and instead lifts the
near term, removing the seam discontinuity. See `../adjustments/_index.md`.

The LOL curve is +55,000/day higher than the baseline's at every forecast date and the realised effect was
+52,256 — **95% pass-through**. The extra training subtraction covers only 39 recent days
(2026-06-19 → 2026-07-27, mean +37,433/day), which barely shifts the fitted trend five months out, so
most of the add-back survives. The notebook asserts this pass-through lands in 0.5–1.5×; near-zero would
indicate the bidirectional subtract leg misfired.

**Build history at this anchor** (same data throughout): 125K/−1,345,000 → 48,520,714 ·
165K/−1,345,000 → 48,561,795 · 165K/−1,295,000 → 48,611,795 · **180K/−1,245,000 → 48,672,970**.

## Reproduce

See the command block in `../_index.md`. `scripts/run_param_scan.py` is the producer —
`run_main.py` cannot reproduce this config (no parameter flags).
