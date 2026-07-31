# August 2026 Canonical Forecast Curves — CSV exports

Static, plot-ready exports of the August 2026 canonical forecast review
(`../august_canonical_v2026-07-28.ipynb`). Read them into pandas and plot — no
BigQuery, no model code needed. This is the set to hand off / upload.

| File | Scope | Headwind applied? |
|---|---|---|
| `august_canonical_curves.csv` | Desktop, Mobile, and ALL (Desktop+Mobile) world totals | **Yes** |
| `august_dec15_summary.csv` | The Dec-15 headline + summer trough per platform | **Yes** |

All curve values are **28-day moving averages of daily active users (DAU)**, in
absolute user counts, daily from **2026-01-01 through 2026-12-31**.

**Rounding policy:** every published figure is derived from already-rounded values, never
from the underlying floats. So `all_* == desktop_* + mobile_*` exactly on every row, and
`delta_vs_july` is exactly the difference of the two columns beside it. Deriving from the
floats is marginally more accurate but leaves the file self-contradictory (desktop's Dec-15
delta would print 118,476 next to columns differing by 118,477); at 10⁻⁸ relative error that
accuracy buys nothing and the off-by-one costs real trust. The assertions in the producing
cell are therefore exact equality, not tolerance-based.

- **Desktop** = Firefox Desktop only (`legacy_desktop`; Glean desktop is excluded).
- **Mobile** = Fenix (Android) + Firefox iOS + Focus Android + Focus iOS (`glean_mobile`).
- **ALL** = Desktop + Mobile, summed date-by-date.

Iran is included **natively** in every column (the mozaic package auto-applies a
counterfactual "what Iran would have been with no shutdown" fill during training).
There is no `plus_iran` / `no_iran` split.

---

## Read this before quoting the headline

August lands **+119,215 ALL** above July. That number is **not** "what the fresher
data says" — the data refresh alone pointed **down**. Two upward judgement calls,
neither of which currently has a validation artifact, more than reversed it:

| lever | change | Dec-15 desktop effect | basis |
|---|---|--:|---|
| data refresh (07-06 → 07-28 anchor) | — | **−64,769** | what the fresher data said |
| `l` launch-on-login ceiling | 125,000 → 200,000/day | +77,604 | extrapolation judgement; **unfalsifiable** |
| `h` Win10 headwind anchor | −1,345,000 → −1,245,000 | +100,000 | calibration judgement |
| desktop model retune to s01 | — | +5,642 | measured like-for-like |
| `h` ramp start (2026-04-01 → seam) | — | 0 | convention correction |

Two further caveats on the current build:

- **`o` (MozillaOnline) and `m` (marketing lift) are ~4–5 week-stale carry-forwards
  from July**, unchanged. Re-measuring them is the main reason the upstream cycle
  notes still describe this build as not final.
- **The launch-on-login ceiling cannot be validated against data.** The holdback
  control received the feature on 2026-06-23, so the counterfactual is permanently
  gone and no fresh telemetry can adjudicate the ceiling. At a ~20K/day haircut
  against a ~220K convolution model, 200,000 is the least conservative end of the
  range that was considered. The lower variants were deleted on 2026-07-30, which
  removed the menu but not the uncertainty — do not read the single remaining
  curve as a measured or consensus value.

Full attribution ledger (it closes to a residual of −0, and the notebook asserts it):
`../_index.md` § Attribution ledger.

---

## Prompt for an AI agent

> I have a CSV of Firefox DAU forecast curves. The first column is `date` (daily,
> 2026-01-01 through 2026-12-31). Every other column is a **28-day moving average
> of DAU**. Columns are grouped into `desktop_*`, `mobile_*`, and `all_*` (all_ =
> desktop + mobile). Load it into pandas (parse `date` as datetime) and make line
> charts — one per platform group — with date on the x-axis and DAU (formatted in
> millions) on the y-axis. Some columns are intentionally blank in part of the
> year: the `*_current_august` forecast columns only start at the forecast date
> **2026-07-28** and are empty before it; the `*_actuals` columns stop a couple of
> days before today because the most recent telemetry day is still landing. Let
> those gaps render as gaps. Format y-axis ticks to two decimal places of a million
> — the ranges are narrow enough that a whole-million formatter renders adjacent
> ticks identically.

---

## `august_canonical_curves.csv`

Nine series: 3 platforms (desktop / mobile / ALL) × 3 curves (actuals / prior-July
forecast / current-August forecast). Forecast curves **include the headwind
adjustment** (see "What's baked in").

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. 2026-01-01 .. 2026-12-31. |
| `desktop_actuals` | Desktop | Observed DAU. Ends 2026-07-28. |
| `desktop_prior_july` | Desktop | **July** forecast (prior cycle). Full-year. |
| `desktop_current_august` | Desktop | **August** forecast (current cycle). Blank before 2026-07-28. |
| `mobile_actuals` | Mobile | Observed DAU. Ends 2026-07-28. |
| `mobile_prior_july` | Mobile | July forecast (prior cycle). Full-year. |
| `mobile_current_august` | Mobile | August forecast (current cycle). Blank before 2026-07-28. |
| `all_actuals` | ALL | `desktop_actuals + mobile_actuals`. |
| `all_prior_july` | ALL | `desktop_prior_july + mobile_prior_july`. |
| `all_current_august` | ALL | `desktop_current_august + mobile_current_august`. |

### Key Dec-15 numbers (28-day MA of DAU)

| Series | July (prior) | August (current) | Δ vs July | Δ % |
|---|---:|---:|---:|---:|
| Desktop | 48,585,483 | 48,703,960 | +118,477 | +0.24% |
| Mobile | 17,923,869 | 17,924,607 | +738 | +0.00% |
| **ALL** | **66,509,352** | **66,628,567** | **+119,215** | **+0.18%** |

Mobile is essentially flat because nothing mobile-side changed this cycle: it carries
July's locked parameters, `l` is desktop-only, and mobile's headwind amplitude did not
move. The whole ALL delta is desktop.

### What's baked in (`current_august` forecast columns)

- **Win10 headwind (`h`)** — a linear ramp added on top of the raw model output over
  the forecast horizon, **ramping from the seam (2026-07-28)** to its Dec-15 anchor:
  - Desktop anchor: **−1,245,000 DAU** at 2026-12-15
  - Mobile anchor: **−27,162 DAU** at 2026-12-15

  The seam start is new this cycle. Under July's convention the ramp began 2026-04-01
  but was only *applied* from the seam forward, so it switched on at 45.7% of full
  value (−569,419) as a one-day level step — which accounted for 100.9% of the seam
  discontinuity visible in July's chart. Ramping from the seam hits the identical
  Dec-15 anchor starting from zero, so **the KPI is untouched and the near term lifts**.
- **Desktop tailwinds** (applied per-tile inside the forecast, already in the parquet):
  - **Launch-on-login (`l`)** — **200,000 DAU/day** ceiling (July: 125,000). See the
    caveat above: this is extrapolation, not measurement.
  - **MozillaOnline migration (`o`)** — CN distribution-partner migration onto
    mainline Firefox. **Unchanged carry-forward from July; stale.**
- **Mobile marketing lift (`m`)** — the Fenix paid-marketing campaign, applied
  per-tile inside the forecast. **Unchanged carry-forward from July; stale.**
- **Desktop model** — retuned to the **s01** config this cycle
  (`seasonality_regime=multiplicative, cps=0.1849, cpr=0.734, recent=17, ncp=35`).
  This is what lifts the summer trough by +1,359,887 for only +5,642 at Dec-15.
  Mobile carries July's locked parameters unchanged.
- **Iran** — native counterfactual fill applied during training inside the mozaic
  package, so every column is already Iran-inclusive.

The `prior_july` columns are last cycle's published July forecast, carrying **July's
own** headwind spec (−1,345,000 desktop, ramping from 2026-04-01) applied from July's
own seam (2026-07-06). They span the entire window. Because the two cycles use
different ramp conventions, **only the Dec-15 comparison is apples-to-apples** — between
the seam and Dec-15 the two curves sit on different conventions by construction.

---

## `august_dec15_summary.csv`

| Column | Meaning |
|---|---|
| `series` | `Desktop`, `Mobile`, or `ALL`. |
| `measurement_date` | `2026-12-15`. |
| `current_august` | August forecast Dec-15 28d-MA DAU (incl. headwind). |
| `prior_july` | July forecast Dec-15 28d-MA DAU. |
| `delta_vs_july` | `current_august − prior_july`. |
| `delta_pct_vs_july` | Same as a percentage, 3 dp. |
| `summer_trough_min` | Minimum of the current curve over 2026-07-28 .. 2026-10-15. |
| `summer_trough_date` | Date that minimum falls on. |

| Series | Dec-15 | Summer trough | on |
|---|---:|---:|---|
| Desktop | 48,703,960 | 45,223,249 | 2026-08-25 |
| Mobile | 17,924,607 | 17,015,132 | 2026-08-16 |
| ALL | 66,628,567 | 62,295,635 | 2026-08-25 |

**There are no `target` / `vs_target` columns this cycle.** July carried a stakeholder
desktop target of 48,584,362, but **no August target has been set**. Carrying July's
number in a column named `target` would read as an August target, so it is omitted
rather than shipped stale. (The gold low/baseline/stretch markers on the plots are
June-cycle aspirational benchmarks, reused only so the vertical scale stays comparable
across cycles — they are likewise not August targets.)

The trough columns are new this cycle. The summer trough is the near-horizon KPI the
s01 retune was adopted to move, so a summary reporting only Dec-15 would omit the
single largest change of the cycle.

**Desktop's trough is measured at Aug-25, not Aug-22**, because Aug-25 is exactly 28
days past the seam — its MA window is entirely forecast, so the value is independent of
the seam-splice convention. Aug-22 sits inside the 27-day transition zone and reads
~41K apart under the two conventions. For continuity with earlier August builds,
Aug-22 reads Desktop 45,263,042 · Mobile 17,056,561 · ALL 62,319,604.

---

## Why some columns are blank in part of the year

- The **`*_current_august`** columns are the new forecast, drawn forecast-only: they
  start on the forecast date (**2026-07-28**) and are empty before it.
- The **`*_actuals`** columns are observed data and stop before the file was generated
  (telemetry lands with a lag — the export uses a
  `CURRENT_DATE("America/Los_Angeles") - 2` cutoff; last actual day is **2026-07-28**).
  The LA-time clamp matters: plain `CURRENT_DATE()` is UTC, so after ~5pm PDT it bumps
  to tomorrow and `-2` lands on a still-partial day, which poisons the trailing MA.
- The **`*_prior_july`** columns span the entire window.

This is expected — leave the gaps as gaps when plotting.

---

## Seam smoothing (forecast 28dMA)

The forecast 28-day moving averages are smoothed across the actuals→forecast seam. A
naive trailing 28dMA blends raw actuals into its window for the first 27 forecast days,
which makes the curve oscillate for ~a month before settling. The export uses the
variance-matched `display_ma` from `src/mozaic_daily/seam_ma.py`. **Every date from
forecast-start +27 days onward (including the Dec-15 headline) is byte-identical to the
naive average** — only the visual seam transition changed.

Two seam artifacts were fixed for this cycle, and it is worth knowing what changed
because the curve now *looks* different at the seam:

1. **The headwind step** (−564,262 in earlier charts) was the ramp-convention bug
   described above. Re-anchoring to the seam removed it.
2. **A +102,595 upward display artifact** in the MA reconstruction (`Fix A`,
   2026-07-29): the trend estimator deseasonalized with a 7-day centered mean computed
   on the forecast only, which at the seam degenerates to a weekday-only forward window
   and read ~10% high. The fix divides by the forecast's own day-of-week profile before
   smoothing. Residual distortion on this build is **+102 DAU**.

**The curve therefore leaves the actuals heading slightly DOWN (−107,445), and that is
correct** — the old upward step was masking a real decline. The model's own plain 28d-MA
steps −107,547, so the display now tracks it to ~100 DAU. Dec-15 and everything from
seam+27 onward were byte-identical before and after the fix.

Full diagnosis: `research/ma-seam-turbulence/LOG.md` § Fix A, and
`../seam_fix_before_after.ipynb`.

---

## Minimal plotting recipe

```python
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

df = pd.read_csv("august_canonical_curves.csv", parse_dates=["date"]).set_index("date")
millions = FuncFormatter(lambda v, _: f"{v/1e6:.2f}M")

for platform in ["desktop", "mobile", "all"]:
    cols = [c for c in df.columns if c.startswith(platform)]
    ax = df[cols].plot(figsize=(14, 6), title=f"2026 {platform.title()} DAU — 28-day MA")
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(millions)
    plt.tight_layout()
    plt.show()
```

`df[cols].plot` skips NaNs automatically, so blank early-year cells render as gaps.

---

## Provenance / regenerating

Generated by the `# [csv-export]` cell in `../august_canonical_v2026-07-28.ipynb`.
Both platforms share the forecast seam **2026-07-28** (trained through 2026-07-27).

- **Forecast parquets (current August):**
  - Desktop: `../desktop_locked/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet`
    (legacy_desktop DAU; s01 params; overlays `l` at the 200K ceiling + `o`).
  - Mobile: `../mobile_baseline_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet`
    (glean_mobile DAU; July's locked params; overlay `m`).
  - `new_profiles` is not exported here — these are DAU-only curves.
- **Prior forecast (July):** `../../2026-07/desktop_locked/` and
  `../../2026-07/mobile_refresh_2026-07-06/`, with July's own headwind spec from
  `../../2026-07/adjustments/`.
- **Adjustment spec (headwind):** `../adjustments/headwind.json`
  (desktop −1,245,000 / mobile −27,162 at the 2026-12-15 anchor, ramping from 2026-07-28).
- **Actuals:** `telemetry.active_users_aggregates` (desktop) and
  `glean_telemetry.active_users_aggregates` (mobile) in BigQuery, through
  `CURRENT_DATE("America/Los_Angeles") - 2`.

Each daily series is converted to a 28-day moving average; the headwind ramp value at
each date is then added to the forecast MA over the forecast horizon. The mobile `m`
and desktop `l`/`o` overlays are applied per-tile inside the forecast, so they are
already in the parquets.

### Checks that run as assertions, not eyeballs

The producing notebook aborts rather than emitting a plausible wrong number:

1. **Config lock** — each sidecar's model config is compared field-by-field against its
   lock (desktop: s01, 6 params; mobile: July's, 8 params), plus desktop's four holiday
   knobs against package defaults (they are excluded from tuning by policy).
2. **State markers** — `load_forecast(..., require_state=["l","o"])` / `["m"]` pins which
   adjustments must be present. A run whose spec date failed to match would have emitted
   `.raw.` and fails loudly here.
3. **Prior-curve reproduction** — July's delivered Dec-15 values (48,585,483 /
   17,923,869) are hardcoded and the rebuilt prior curve must match within 1,000 DAU.
   Both reproduce at **drift 0**, which is what licenses quoting the deltas at all.
4. **Attribution-ledger closure** — the five pinned steps must sum to the measured
   Dec-15. Residual is **−0**.
5. **CSV round-trip** — both files are re-read from disk after writing and their Dec-15
   values re-checked against the in-memory curves, and the forecast columns are asserted
   to start exactly at the seam.

Re-run with:

```bash
# Rebuilds csv/august_canonical_curves.csv + csv/august_dec15_summary.csv
# and all nine plots under plots/. Needs BigQuery access for the actuals.
source .venv/bin/activate
jupyter nbconvert --to notebook --execute --inplace \
  data-official/2026-08/august_canonical_v2026-07-28.ipynb
```
