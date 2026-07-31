# `data-official/2026-08/launch_on_login/` — launch-on-login desktop tailwind (`l`)

Bidirectional overlay on `legacy_desktop` DAU, `modern_windows` segment: the historical rise is
subtracted from training rows before mozaic (so Prophet learns the no-LOL dynamic), then the capped
curve is added back to the per-country forecast.

**This cycle has exactly one curve: the 200,000 DAU/day ceiling.** The 125K / 165K / 180K variants
that existed earlier in the cycle were deleted on 2026-07-30 — see "Deleted alternates" below.

| file | role |
|---|---|
| `lol.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `lol_tailwind.2026-07-29.cap200k.{parquet,model_meta.json}` | **the** curve — 200K ceiling, selected 2026-07-29 |
| `plots/lol_tailwind_curve.cap200k.png` | measured excess vs delivered curve |

## The curve

Zero before the 2026-05-08 rollout; **measured** (FF152-excluded, interpolated 7d trailing MA) to
130,296 at 2026-06-23; then **extrapolated** on a linear ramp at 2,714/day (the recorded ~19,000/wk)
to the 200,000 ceiling, flat through 2027-12-31.

The ceiling is first reached **2026-07-19**, leaving only **9 days of plateau inside training** (which
ends 2026-07-27). So the last weeks of training see a rising subtraction rather than a flat one, and
the curve is fully in effect across the whole forecast horizon.

**Which series the cap applies to.** `.clip(upper=200_000)` is applied to the FF152-excluded,
interpolated 7d trailing MA — *not* the raw daily excess (138,376 at the cutoff) and *not* the raw
7d-MA including the FF152 transient (~145,000). The ramp anchors on the smoothed series (130,296) so
the seam at 2026-06-24 is continuous. Anchoring on either other quantity would shift the curve by
8–15K/day.

The curve is not strictly monotone inside the measured window: the 7d-MA carries day-of-week residue,
giving six small downward steps (2026-05-27 → 05-30 and 2026-06-21, 06-23; largest −1,207, ≈1% of
level). That is smoothing noise, not a modelled drop-off. From 2026-06-24 onward it is strictly
non-decreasing.

## Measured vs extrapolated

Everything after **2026-06-23** is extrapolation by construction. Contamination begins 2026-06-24: the
holdback control group received the feature, so the counterfactual is permanently gone. No fresh
telemetry can extend the clean window — querying recent data shows the excess "collapsing," which is
an artifact of the control being treated, not a decay. Do not re-measure past that date.

**The 200,000 ceiling is therefore an extrapolation judgement and is unfalsifiable.** Measured data
stops at 130,296. Nothing in telemetry can adjudicate it, and nothing ever will. Deleting the
alternates removed the *menu*, not the uncertainty — do not read the single remaining curve as a
measured quantity.

## Conservatism

The curve models no retention decay in either direction; the conservatism is entirely the **flat
ceiling**. The measured effect had *not* plateaued at the cutoff (still rising ~19K/wk) and is stopped
dead anyway — growth we cannot validate is never extrapolated forward.

An independent convolution model of the same effect
(`~/work/launch-on-login/dau_model_convolution.ipynb`) lands near **220,000/day**, so 200,000 is only
a **~20K/day haircut**. At this ceiling the curve is no longer meaningfully conservative against that
model — it is close to betting on it.

## Deleted alternates (2026-07-30)

The 125K (`lol_tailwind.2026-06-29.*`), 165K (`lol_tailwind.2026-07-29.*`) and 180K
(`lol_tailwind.2026-07-29.cap180k.*`) curves, their plots, `LOL_165K_HANDOFF.md`, and the
`aug_lol165` / `aug_lol180` run logs were deleted at the user's instruction — they were being
referenced far more than a superseded alternate warrants. All were git-tracked, so they are
recoverable from history (`git log --diff-filter=D -- data-official/2026-08/launch_on_login/`).

Two consequences worth knowing:

- **`../desktop_baseline_2026-07-28/` is no longer reproducible from the working tree.** Its committed
  sidecar records `lol.json` at `spec_sha1: 99d2bdb72d2c3595c074de27a22c9ebc3edeedba` — the 125K-curve
  version of the spec — and that curve file is gone. The build itself is on disk and frozen; only a
  from-scratch rebuild is blocked. It was never going to be re-run anyway (locked builds never are).
- **The August attribution ledger now carries one combined `125,000 → 200,000` step of +77,604** instead
  of two. The underlying split is still reproducible — the deleted 180K build was the same run as
  `research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_…_regimemultiplicative/`, whose copy
  survives — but `data-official/` no longer presents it as two steps. See `../_index.md`.

To resurrect an alternate, rebuild it from the producer rather than reverting the deletion — the
invocation is recorded below and in `produced_by_invocation` in the surviving model meta.

## Producer

`~/work/launch-on-login/build_lol_tailwind.py`, parameterised on `--cap` / `--run-date` /
`--ramp-slope-per-day` / `--out-dir` / `--meta-suffix` / `--name-suffix` / `--note`. Defaults reproduce
the July 125K artifact byte-for-byte (verified: sha1 `03f21345268dfbdf7cb7b2df8203c35cc5c0ff86` — that
artifact still exists at `../../2026-07/launch_on_login/`, which was deliberately left untouched); the
pre-parameterisation copy is preserved at
`~/work/launch-on-login/archive/build_lol_tailwind.2026-06-29.125k.py`. That directory is **not** a git
repo — copy before editing.

The exact invocation for the active curve is in `produced_by_invocation` in its model meta:

```bash
python3 ~/work/launch-on-login/build_lol_tailwind.py --cap 200000 --run-date 2026-07-29 \
    --ramp-slope-per-day 2714.29 --name-suffix .cap200k \
    --out-dir data-official/2026-08/launch_on_login --meta-suffix .model_meta.json \
    --note "..."
```

## Changing the ceiling

Editing `lol.json`'s `data_file` / `model_meta_file` is **not** sufficient — the curve is baked into
the forecast parquet, so a spec swap without a model re-run changes nothing downstream. The full path
is: build the new curve with the producer, point the spec at it, re-run
`scripts/run_param_scan.py` into a **new** results directory, and add a ledger step.

**Leave `applies_to_forecast_start: "2026-07-28"` alone.** Overlay specs are matched by exact string
equality on that field; a run at a date no spec claims applies **no** overlays and silently emits
`.raw.` instead of `.adj-lo`.

**Where new files go:** a new curve variant for this cycle (dated parquet + model meta, tagged with
`--name-suffix`) and diagnostic plots under `plots/`. If you add a variant, do not leave it lying
around after the decision is made — that accumulation is what this cleanup undid.
