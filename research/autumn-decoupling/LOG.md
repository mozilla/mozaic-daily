# Autumn-decoupling exploration — findings log

Append-only. Dead ends stay in, with the reason they died.

---

## Round 1 — 2026-07-30 — Attribution and the data-side premise check

### F1. The s01 retune owns only about half the autumn gap

Ladder built from the four on-disk desktop builds (`attribute_autumn.py`). 28d-MA, post-headwind.

| rung | Aug-25 trough | Sep-15 | Oct-15 | Nov-15 | Dec-15 |
|---|--:|--:|--:|--:|--:|
| 0. July delivered | 43,261,424 | 45,679,737 | 47,852,445 | 47,909,107 | 48,585,483 |
| 1. + Aug data & LOL 125→180K | +60,127 | +116,964 | +13,689 | +16,728 | −12,513 |
| 2. + Aug headwind (re-anchor & −100K) | +512,124 | +434,851 | **+324,460** | **+210,390** | +100,000 |
| 3. + s01 model retune | +1,359,887 | +887,738 | **+340,440** | **+213,753** | +5,642 |
| 4. + LOL 180→200K **[LOCKED]** | +29,688 | +19,218 | +9,162 | +5,251 | +25,348 |
| **total** | **+1,961,825** | **+1,458,771** | **+687,751** | **+446,122** | **+118,476** |

At Oct-15 the split is 47% headwind convention / 49% retune; at Nov-15, 47% / 48%.

The headwind contribution is **not** a model effect. August re-anchored the `h` ramp to start at the
seam (2026-07-28) instead of 2026-04-01, holding the same Dec-15 anchor. That is Dec-15-neutral by
construction and *raises every interior date*, because the old convention had already ramped to
76% of full value by Oct-15 versus 56% under the new one.

Consequence: attributing the whole autumn bulge to the retune overstates it roughly 2×.

### F2. History does not support a decoupled autumn

`seasonal_shape.py`. Each year's desktop DAU 28d-MA normalized to its own Jun-15 — a pre-summer
baseline that for 2026 is settled actuals and therefore identical across both forecasts.

| year | trough/base | Oct/base | Nov/base |
|---|--:|--:|--:|
| 2021 | 0.9097 | 1.0048 | 1.0084 |
| 2022 | 0.9146 | 1.0044 | 1.0011 |
| 2023 | 0.9374 | 1.0179 | 1.0225 |
| 2024 | 0.9143 | 1.0000 | 0.9977 |
| 2025 | 0.9286 | 0.9938 | 1.0009 |
| mean | 0.9209 | 1.0042 | 1.0061 |

Regressing autumn on trough depth across years: **Oct slope +0.309 (r=0.404), Nov slope +0.542
(r=0.630), n=5.** History says a raised summer *should* carry roughly a third of that lift into
October and half into November.

The model's own July→August passthrough, pre-headwind, is **Oct +0.245, Nov +0.159** — *below* the
historical slope on both. The s01 retune is already more conservative about autumn than five years
of actuals would justify.

Where the two curves sit against the historical band, post-headwind:

| curve | Oct/base | vs history |
|---|--:|---|
| historical range | 0.9938 – 1.0179 | mean 1.0042 |
| July delivered | 0.9874 | **below the entire 5-year range** |
| August LOCKED | 1.0016 | inside the range, just under the mean |

On the published chart it is July's October that is the anomaly, not August's.

**Caveats.** n=5, and the Oct correlation is weak (r=0.40, not significant). 2026's own Jun-15
baseline may be atypical — the year had a large Jan–Feb spike decaying all through H1. The Oct/base
ratio also drifts down across 2023→2025, which a level regression does not capture.

### F3. Prophet, as mozaic wires it, cannot bend the curve after the seam

Read of `mozaic/models.py:200-298` (installed package, `configurable-model-params` @ `4f33650`).

- Prophet is constructed with `n_changepoints` + `changepoint_range` only. There is **no
  `changepoints=[...]` list and no `add_regressor` call anywhere in the package.**
- `changepoint_range=0.734` confines every changepoint to the first 73.4% of *history*. Forward of
  the seam the trend is a straight line — constant slope, in logistic or log-linear space.

So the user's stated intuition is confirmed at the code level: **no trend knob can raise August
without raising October, because forward of the seam there is nothing to bend.** Trend knobs
(`cps`, `cpr`, `ncp`, `recent_weeks`) can only change the level and tilt of that one straight line.

The only mechanism that can differentially shape August against October is **yearly seasonality**.
That narrows the admissible lever set to:

1. `prophet_seasonality_prior_scale` — the one continuous knob on yearly-seasonality amplitude.
2. `seasonality_regime` / `seasonality_corr_threshold` — per-tile regime flips.
3. Holiday knobs — **excluded by policy.**

### F4. The "multiplicative" flip is not a seasonality-only change

`models.py:242-247`: forcing `seasonality_regime="multiplicative"` also flips `growth`
logistic→linear **and** switches the fit into log space (`y = log(y+1)`, exponentiated back).
The code comment calls this pinning "mode+growth to the matching tested quadrant."

So s01's regime flip cannot be decomposed into a seasonality part and a trend part by toggling the
flag — it is atomic. `seasonality_corr_threshold` under `regime="auto"` is the continuous version,
but it interpolates the *fraction of tiles* on each side, flipping whole tiles at a time. Per the
in-code note, the legacy 0.0 cutoff puts 37.5% of tiles but only 7.6% of DAU on the multiplicative
side; the memory records `ROW/modern_windows` (27% of desktop weight) flipping at corr −0.1465.
Expect a coarse staircase, not a smooth dial.

---

## 2026-07-30 — LOL curve cleanup: repointed one build, numbers unchanged

The non-200K launch-on-login curves were deleted repo-wide from the August cycle at the user's
instruction, along with the `data-official/2026-08/desktop_superseded_lol180k_2026-07-28/` build this
module read as rung 3.

**Nothing in this module's findings moved.** That build was the same run as
`research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative/`
— identical sidecar (`model_config`, `adjustments_applied` including the `l` spec sha1 `e23a6267`,
commit) and the two `.pkl` files were hard links to one inode. `curves.py` now reads the surviving copy
under key `s01_prev_ceiling` (was `s01_180k`), and re-running `attribute_autumn.py` reproduced every
figure in the table above exactly, rung 3 Dec-15 included (48,678,612). That reproduction is itself the
verification that the twin is the same build.

Rung labels no longer name the intermediate ceiling by value, since no file with that ceiling exists.
The table above is left as originally written — this log is append-only, and its "LOL 125→180K" label is
an accurate record of what was run at the time.

**What is genuinely lost:** the ability to *rebuild* any pre-200K artifact, since the curves are gone.
Not a practical constraint here — this module is read-only with respect to forecast artifacts and
historical builds are locked anyway.
