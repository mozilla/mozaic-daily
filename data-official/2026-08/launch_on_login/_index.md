# `data-official/2026-08/launch_on_login/` — launch-on-login desktop tailwind (`l`)

Bidirectional overlay on `legacy_desktop` DAU, `modern_windows` segment: the historical rise is
subtracted from training rows before mozaic (so Prophet learns the no-LOL dynamic), then the capped
curve is added back to the per-country forecast.

| file | role |
|---|---|
| `lol.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `lol_tailwind.2026-07-29.cap180k.{parquet,model_meta.json}` | alternate — 180K ceiling (active 2026-07-29 until the 200K switch) |
| `lol_tailwind.2026-07-29.{parquet,model_meta.json}` | alternate — 165K ceiling |
| `lol_tailwind.2026-07-29.cap200k.{parquet,model_meta.json}` | **ACTIVE** curve — 200K ceiling (selected 2026-07-29) |
| `lol_tailwind.2026-06-29.*` | **superseded** July curve (125K cap) — kept, not deleted |
| `plots/lol_tailwind_curve{,.cap180k,.cap200k}.png` | measured excess vs delivered curve, per variant |
| `LOL_165K_HANDOFF.md` | the brief the 2026-07-29 rebuild was executed from |

## Which curve is active

**Exactly one curve is live: whatever `lol.json` names in `data_file`.** All three 2026-07-29 variants
share an anchor, a slope, and a producer — only the ceiling differs. Switching is a two-line edit:

```jsonc
"data_file":       "lol_tailwind.2026-07-29.cap180k.parquet",        // current
"model_meta_file": "lol_tailwind.2026-07-29.cap180k.model_meta.json",
// 165K -> drop ".cap180k" from both;  200K -> swap it for ".cap200k"
```

Nothing else changes — **leave `applies_to_forecast_start: "2026-07-28"` alone**. Overlay specs are
matched by exact string equality on that field; a run at a date no spec claims applies **no** overlays
and silently emits `.raw.` instead of `.adj-lo`. Update the `notes` field's "ACTIVE CURVE" sentence
when you switch, and re-run the forecast — the curve is baked into the parquet, so a swap without a
re-run changes nothing downstream.

## The curves (all three built 2026-07-29)

Identical construction — zero before the 2026-05-08 rollout; **measured** (FF152-excluded, interpolated
7d trailing MA) to 130,296 at 2026-06-23; then **extrapolated** on a linear ramp at 2,714/day (the
recorded ~19,000/wk) to the ceiling, flat through 2027-12-31. The ceiling is the only difference:

| ceiling | first reached | days of plateau inside training (to 2026-07-27) | haircut vs ~220K model |
|--:|---|--:|--:|
| 165,000 | 2026-07-06 | 22 | ~55K/day |
| 180,000 | 2026-07-12 | 16 | ~40K/day |
| **200,000** (active) | **2026-07-19** | **9** | **~20K/day** |

All three reach ceiling before the 2026-07-27 training end, so all are fully in effect across the whole
forecast horizon. The higher the ceiling the later the plateau, so the last weeks of training see a
rising subtraction rather than a flat one — at 200K the plateau is only 9 days wide inside training.

July's ceiling was 125,000; it bit *below* the measurement
(`measured_daily_excess_at_last_clean` was 138,376 while the curve read 125,000 from 2026-06-19), so
un-clamping 2026-06-19 → 2026-06-23 was part of the 2026-07-29 rebuild.

**Which series the cap applies to.** `.clip(upper=cap)` is applied to the FF152-excluded, interpolated
7d trailing MA — *not* the raw daily excess (138,376 at the cutoff) and *not* the raw 7d-MA including
the FF152 transient (~145,000). The ramp anchors on the smoothed series (130,296) so the seam at
2026-06-24 is continuous. Anchoring on either other quantity would shift the curve by 8–15K/day.

## Measured vs extrapolated

Everything after **2026-06-23** is extrapolation by construction. Contamination begins 2026-06-24: the
holdback control group received the feature, so the counterfactual is permanently gone. No fresh
telemetry can extend the clean window — querying recent data shows the excess "collapsing," which is
an artifact of the control being treated, not a decay. Do not re-measure past that date.

Note this means the ceiling choice is **entirely** an extrapolation judgement: 125K, 165K, 180K and
200K are indistinguishable on measured data, which stops at 130,296. Nothing in telemetry can
adjudicate between them, and nothing will — the counterfactual died on 2026-06-24.

## Conservatism

The curves model no retention decay in either direction; the conservatism is entirely the **flat
ceiling**. The measured effect had *not* plateaued at the cutoff (still rising ~19K/wk) and is stopped
dead anyway — growth we cannot validate is never extrapolated forward. An independent convolution
model of the same effect (`~/work/launch-on-login/dau_model_convolution.ipynb`) lands near
220,000/day; the haircut against it is the margin column in the table above, and it narrows as the
ceiling rises. At 200K that margin is ~20K/day, so the ceiling is no longer meaningfully conservative
against the convolution model — it is close to betting on it.

Neither curve is strictly monotone: the 7d-MA of the measured excess carries day-of-week residue,
giving six small downward steps inside the measured window (2026-05-27 → 05-30 and 2026-06-21, 06-23;
largest −1,207, ≈1% of level). That is smoothing noise, not a modelled drop-off. From 2026-06-24
onward both curves are strictly non-decreasing.

## Provenance note for the August baseline

The committed baseline sidecar
`../desktop_baseline_2026-07-28/*/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet.meta.json`
records `lol.json` at `spec_sha1: 99d2bdb72d2c3595c074de27a22c9ebc3edeedba` — the 125K-curve version of
the spec, which the 2026-07-29 rebuild replaced in place. That baseline (Dec-15 28d-MA 48,520,714
post-headwind) was built against the superseded curve; the `lol_tailwind.2026-06-29.*` files stay on
disk so it remains reproducible. Every later `lol.json` edit moves the sha1 again, so treat the
sidecar's value as "which curve that forecast saw," not as a check against the current file.

## Producer

`~/work/launch-on-login/build_lol_tailwind.py`, parameterised on `--cap` / `--run-date` /
`--ramp-slope-per-day` / `--out-dir` / `--meta-suffix` / `--name-suffix` / `--note`. Defaults reproduce
the July 125K artifact byte-for-byte (verified: sha1 `03f21345268dfbdf7cb7b2df8203c35cc5c0ff86`); the
pre-parameterisation copy is preserved at
`~/work/launch-on-login/archive/build_lol_tailwind.2026-06-29.125k.py`. That directory is **not** a git
repo — copy before editing.

`--name-suffix` is what lets cap variants for one cycle coexist without overwriting each other's
parquet, meta, *or* plot. The exact invocation for each curve is in `produced_by_invocation` in its
model meta, so a new ceiling is one command:

```bash
python3 ~/work/launch-on-login/build_lol_tailwind.py --cap <N> --run-date 2026-07-29 \
    --ramp-slope-per-day 2714.29 --name-suffix .cap<N>k \
    --out-dir data-official/2026-08/launch_on_login --meta-suffix .model_meta.json \
    --note "..."
```

**Where new files go:** further curve variants for this cycle (dated parquet + model meta, tagged with
`--name-suffix`), and diagnostic plots under `plots/`.
