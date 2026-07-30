# Desktop — August 2026, s01 config with the 180K LOL ceiling — **SUPERSEDED**

`legacy_desktop` DAU, forecast_start 2026-07-28, s01 model config. **This was the canonical August
desktop build from 2026-07-29 until later the same day**, when the launch-on-login ceiling was raised
165K → 180K → **200K** and the build was re-run. It now lives here rather than in `../desktop_locked/`.

**Nothing here is wrong.** It differs from the current canonical in exactly one input: the LOL curve
ceiling (180,000/day vs 200,000/day). Model config, data, seam, headwind and the `o`/`m` overlays are
identical. Kept, not deleted, so the ceiling decision stays auditable.

## Result (28d-MA, post-headwind −1,245,000 ramping from the seam)

| quantity | this build (180K) | current canonical (200K) | delta |
|---|--:|--:|--:|
| Aug-25 trough minimum | 45,193,561 | 45,223,249 | +29,688 |
| Aug-22 | 45,233,893 | 45,263,042 | +29,150 |
| Sep-15 | 47,119,290 | 47,138,508 | +19,218 |
| Oct-15 | 48,531,034 | 48,540,196 | +9,162 |
| Nov-15 | 48,349,978 | 48,355,229 | +5,251 |
| **Dec-15** | **48,678,612** | **48,703,960** | **+25,348** |

The delta is **not** the flat +20,000/day the ceiling difference would suggest, and it is not monotone
in time. `l` is a *bidirectional* overlay: the extra 20,000/day is subtracted from `modern_windows`
training rows before mozaic as well as added back to the forecast, so Prophet refits on a different
history and redistributes the effect. Largest near-horizon, smallest in November, +25,348 at Dec-15.
**Do not model a ceiling change as a level shift.**

## Configuration

Identical to the current canonical — see `../desktop_locked/README.md` for the full s01 table
(`multiplicative`, cps 0.1849, cpr 0.734, recent 17, ncp 35, sps 0.00825, holidays at package
defaults). `parameters.json` in this directory is the authoritative record for *this* build.

## Why it was superseded

The three 2026-07-29 LOL curves (165K / 180K / 200K) share one anchor, one slope and one producer —
only the ceiling differs. Measured data stops at 130,296 on 2026-06-23, and the counterfactual died on
2026-06-24 when the holdback control received the feature, so **no telemetry can ever adjudicate
between them.** 200K was selected as the smallest haircut (~20K/day) against an independent
convolution model of the same effect that lands near 220,000/day; 180K was a ~40K/day haircut, 165K
~55K/day. That is a forward judgement and is recorded as one in `../launch_on_login/lol.json`.

## Files

Same layout as the canonical build. `mozaic_objects.…pkl` (634MB) is hard-linked to the copy under
`research/param-scans/summer-trough-v2/s01_gradient/<slug>/` rather than duplicated;
`mozaic_parts.raw.…parquet` is a symlink to the shared BQ pull under
`../desktop_baseline_2026-07-28/…`. Both gitignored — archive to GCS at button-down.

**Do not re-run or edit anything here.** If the ceiling decision is revisited, build a new directory.

## History preserved from this build's own tenure as canonical

Its promotion evidence — the s01 config gradient, the three acceptance criteria, and the
Dec-15/trough/seam-kink sensitivity — is in `research/param-scans/summer-trough-v2/`. That notebook
still loads from `s01_gradient/`, i.e. from **180K** builds, and is deliberately left that way: it
exists to justify the *model config* against the previous canonical on an identical LOL curve, and
repointing it at 200K would confound the retune evidence with the ceiling change.
