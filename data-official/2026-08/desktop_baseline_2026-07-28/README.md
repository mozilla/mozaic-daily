# Desktop — August 2026, SUPERSEDED (forecast_start 2026-07-28, July's params, pre-200K LOL ceiling)

`legacy_desktop` DAU. **SUPERSEDED 2026-07-29** by `../desktop_locked/`, which carries the s01 retune
and a 200K LOL ceiling. **FROZEN — never re-run, rebuilt or replaced.** Kept on disk deliberately: its
raw BQ pull is the shared cache every scan symlinks, and its Dec-15 below is the anchor point the
ledger's retune step is pinned from. Do not delete it before button-down.

> **Role note, corrected 2026-07-29.** This file previously said the canonical notebook *loads* this
> build to measure the retune step. It no longer does — every ledger step is now a pinned constant, so
> the notebook has no dependency on this path (the parquet is gitignored and GCS-bound, which is why).
> The retune step is still *derived from* this build's Dec-15, measured against the s01 build on the same
> LOL curve; the like-for-like comparison lives in
> `research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`. **No number in this file changed.**

> ⚠️ **This build's LOL curve was deleted 2026-07-30**, along with every other non-200K variant, at the
> user's instruction. The build itself is untouched and frozen, and its numbers below stand — but it can
> no longer be rebuilt from scratch. That is not a practical loss: **historical builds are locked and are
> never re-run.** Its comparison partner (the s01 build on the same curve) survives under
> `research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_…_regimemultiplicative/`, so the retune
> delta remains reproducible by differencing the two.

Its Dec-15 (**48,672,970**) and Aug-25 trough (**43,833,674**) are the baseline every August delta is
quoted against.

⚠️ **The directory name is a misnomer.** It originally held the July-ceiling *baseline*, which the
2026-07-29 rebuild **overwrote in place** (deliberately — that ceiling was July's number). The name is
kept because the canonical notebook and the committed sidecar reference this path. The superseded
baseline's Dec-15 was **48,520,714**, preserved here and in git commit `140412e`.

Single config subdir: `cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/`.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login, **a since-deleted pre-200K ceiling**) + `o` (MozillaOnline migration, stale July carry-forward) applied
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

> **⚠️ SUPERSEDED — this is not the cycle's published number.** This build is **FROZEN** (July's
> parameters, an LOL curve that no longer exists on disk). The canonical August desktop build is
> `../desktop_locked/` at **48,697,603** (g01 params, LOL 200K, headwind −1,220,000). This
> directory is kept for exactly two reasons: its raw BQ pull is the shared cache every scan and
> isolation run symlinks, and its Dec-15 below is the anchor the canonical notebook's retune step is
> pinned from. **Never re-run or rebuild it.**

**48,672,970** — **+87,487 (+0.18%)** vs July's delivered 48,585,483.

Aug-22 summer trough: **43,921,488** (post-headwind, seam-anchored ramp; it was 43,453,752 under the superseded 2026-04-01 ramp start).

Attribution ledger:

| | Dec-15 28d-MA | step |
|---|--:|--:|
| July delivered (July's LOL ceiling, hw −1,345,000, 07-06 anchor) | 48,585,483 | — |
| Aug baseline (July's ceiling, hw −1,345,000) — superseded | 48,520,714 | −64,769 data refresh |
| Aug, raised ceiling (hw −1,345,000) — implied | 48,572,970 | +52,256 LOL ceiling |
| **This build, as of 2026-07-29 (hw −1,245,000)** | **48,672,970** | +100,000 headwind |

The cycle then moved past this build: the ceiling was raised again to 200,000, the headwind attenuated a
third time to −1,220,000, and the model retuned to s01 and then g01, ending at **48,697,603** in
`../desktop_locked/`. The full six-step chain is in `../_index.md` § Attribution ledger.

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

**Build history at this anchor.** This directory was overwritten in place several times as the LOL ceiling
was raised and the headwind anchor attenuated; the intermediate numbers were removed from this file on
2026-07-30 with the curves they belonged to. This build's own result is the **48,672,970** above. The
canonical ledger is in `../_index.md`.

## Reproduce

See the command block in `../_index.md`. `scripts/run_param_scan.py` is the producer —
`run_main.py` cannot reproduce this config (no parameter flags).
