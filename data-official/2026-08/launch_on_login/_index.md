# `data-official/2026-08/launch_on_login/` — launch-on-login desktop tailwind (`l`)

Bidirectional overlay on `legacy_desktop` DAU, `modern_windows` segment: the historical rise is
subtracted from training rows before mozaic (so Prophet learns the no-LOL dynamic), then the capped
curve is added back to the per-country forecast.

| file | role |
|---|---|
| `lol.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `lol_tailwind.2026-07-29.parquet` | **active** curve (`lol_lift_daily`), 2026-01-01 → 2027-12-31 |
| `lol_tailwind.2026-07-29.model_meta.json` | model provenance for the active curve |
| `lol_tailwind.2026-06-29.*` | **superseded** July curve (125K cap) — kept, not deleted |
| `plots/lol_tailwind_curve.png` | measured excess vs delivered curve |
| `LOL_165K_HANDOFF.md` | the brief this rebuild was executed from |

## The curve (rebuilt 2026-07-29, 165K ceiling)

| span | value |
|---|---|
| ≤ 2026-05-07 | 0 (pre-rollout) |
| 2026-05-08 → **2026-06-23** | **measured** — FF152-excluded, interpolated 7d trailing MA of the holdback-experiment excess; 130,296 at the last clean day |
| 2026-06-24 → 2026-07-05 | **extrapolated** — linear ramp at 2,714/day (the recorded ~19,000/wk rise at the cutoff) |
| 2026-07-06 → 2027-12-31 | flat at **165,000** |

August's ceiling is 165,000; July's was 125,000. The old cap bit *below* the measurement
(`measured_daily_excess_at_last_clean` was 138,376 while the curve read 125,000 from 2026-06-19), so
un-clamping 2026-06-19 → 2026-06-23 was part of the rebuild.

**Which series the cap applies to.** `.clip(upper=cap)` is applied to the FF152-excluded, interpolated
7d trailing MA — *not* the raw daily excess (138,376 at the cutoff) and *not* the raw 7d-MA including
the FF152 transient (~145,000). The ramp anchors on the smoothed series (130,296) so the seam at
2026-06-24 is continuous. Anchoring on either other quantity would shift the curve by 8–15K/day.

## Measured vs extrapolated

Everything after **2026-06-23** is extrapolation by construction. Contamination begins 2026-06-24: the
holdback control group received the feature, so the counterfactual is permanently gone. No fresh
telemetry can extend the clean window — querying recent data shows the excess "collapsing," which is
an artifact of the control being treated, not a decay. Do not re-measure past that date.

## Conservatism

The curve models no retention decay in either direction; the conservatism is entirely the **flat
ceiling**. The measured effect had *not* plateaued at the cutoff (still rising ~19K/wk) and is stopped
dead anyway — growth we cannot validate is never extrapolated forward. An independent convolution
model of the same effect (`~/work/launch-on-login/dau_model_convolution.ipynb`) lands near
220,000/day, so 165,000 is a standing haircut of roughly 55K/day.

Note the curve is not strictly monotone: the 7d-MA of the measured excess carries day-of-week residue,
giving six small downward steps inside the measured window (2026-05-27 → 05-30 and 2026-06-21, 06-23;
largest −1,207, ≈1% of level). That is smoothing noise, not a modelled drop-off. From 2026-06-24
onward the curve is strictly non-decreasing.

## Provenance note for the August baseline

The committed baseline sidecar
`../desktop_baseline_2026-07-28/*/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet.meta.json`
records `lol.json` at `spec_sha1: 99d2bdb72d2c3595c074de27a22c9ebc3edeedba` — that is the 125K-curve
version of the spec, which this rebuild replaced in place. The baseline forecast was built against the
superseded curve; the `lol_tailwind.2026-06-29.*` files stay on disk so it remains reproducible.

## Producer

`~/work/launch-on-login/build_lol_tailwind.py`, parameterised on `--cap` / `--run-date` /
`--ramp-slope-per-day` / `--out-dir` / `--meta-suffix`. Defaults reproduce the July 125K artifact
byte-for-byte (verified: sha1 `03f21345268dfbdf7cb7b2df8203c35cc5c0ff86`); the pre-parameterisation
copy is preserved at `~/work/launch-on-login/archive/build_lol_tailwind.2026-06-29.125k.py`. That
directory is **not** a git repo — copy before editing. The exact invocation for the active curve is in
`produced_by_invocation` in its model meta.

**Where new files go:** refreshed curve builds for this cycle (new dated parquet + model meta), and
diagnostic plots under `plots/`.
