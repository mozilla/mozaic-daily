# July 2026 Canonical Forecast Curves — CSV exports

Static, plot-ready exports of the July 2026 canonical forecast review
(`../july_canonical_v2026-06-29.ipynb`). Read them into pandas and plot — no
BigQuery, no model code needed. This is the set to hand off / upload.

| File | Scope | Headwind applied? |
|---|---|---|
| `july_canonical_curves.csv` | Desktop, Mobile, and ALL (Desktop+Mobile) world totals | **Yes** |
| `july_dec15_summary.csv` | The single Dec-15 headline number per platform | **Yes** |

All curve values are **28-day moving averages of daily active users (DAU)**, in
absolute user counts, daily from **2026-01-01 through 2026-12-31**.

- **Desktop** = Firefox Desktop only (`legacy_desktop`; Glean desktop is excluded).
- **Mobile** = Fenix (Android) + Firefox iOS + Focus Android + Focus iOS (`glean_mobile`).
- **ALL** = Desktop + Mobile, summed date-by-date.

> **New this cycle:** Iran is included **natively** in every column (the mozaic
> package auto-applies a counterfactual "what Iran would have been with no
> shutdown" fill during training). There is no `plus_iran` / `no_iran` split like
> last cycle — every series is a single Iran-inclusive curve.

---

## Prompt for an AI agent

> I have a CSV of Firefox DAU forecast curves. The first column is `date` (daily,
> 2026-01-01 through 2026-12-31). Every other column is a **28-day moving average
> of DAU**. Columns are grouped into `desktop_*`, `mobile_*`, and `all_*` (all_ =
> desktop + mobile). Load it into pandas (parse `date` as datetime) and make line
> charts — one per platform group — with date on the x-axis and DAU (formatted in
> millions) on the y-axis. Some columns are intentionally blank in part of the
> year: the `*_current_july` forecast columns only start at the forecast date
> **2026-07-06** and are empty before it; the `*_actuals` columns stop a couple of
> days before today because the most recent telemetry day is still landing. Let
> those gaps render as gaps.

---

## `july_canonical_curves.csv`

Nine series: 3 platforms (desktop / mobile / ALL) × 3 curves (actuals / prior-June
forecast / current-July forecast). Forecast curves **include the headwind
adjustment** (see "What's baked in").

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. 2026-01-01 .. 2026-12-31. |
| `desktop_actuals` | Desktop | Observed DAU. Ends ~2 days before export (last day: 2026-07-05). |
| `desktop_prior_june` | Desktop | **June** forecast (prior cycle). Full-year. |
| `desktop_current_july` | Desktop | **July** forecast (current cycle). Blank before 2026-07-06. |
| `mobile_actuals` | Mobile | Observed DAU. Ends ~2 days before export. |
| `mobile_prior_june` | Mobile | June forecast (prior cycle). Full-year. |
| `mobile_current_july` | Mobile | July forecast (current cycle). Blank before 2026-07-06. |
| `all_actuals` | ALL | `desktop_actuals + mobile_actuals`. |
| `all_prior_june` | ALL | `desktop_prior_june + mobile_prior_june`. |
| `all_current_july` | ALL | `desktop_current_july + mobile_current_july`. |

### Key Dec-15 numbers (28-day MA of DAU)

| Series | June (prior) | July (current) | Δ vs June |
|---|---:|---:|---:|
| Desktop | 48,334,362 | 48,585,483 | +251,121 |
| Mobile | 17,511,100 | 17,923,869 | +412,768 |
| ALL | 65,845,462 | 66,509,352 | +663,890 |

Desktop July lands **+1,121** above the stakeholder target of 48,584,362. The
desktop change vs June is the net of the two new desktop tailwinds (launch-on-login
and the MozillaOnline migration) against a slightly larger Win10 headwind. The
mobile lift is the Fenix paid-marketing campaign (below).

### What's baked in (`current_july` forecast columns)

- **Headwind (`h`)** — a linear ramp from 2026-04-01 to a Dec-15 anchor, added on
  top of the raw model output over the forecast horizon:
  - Desktop anchor: **−1,345,000 DAU** at 2026-12-15
  - Mobile anchor: **−27,162 DAU** at 2026-12-15
- **Desktop tailwinds** (applied per-tile inside the forecast, already in the
  parquet):
  - **Launch-on-login (`l`)** — 125K flat conservative cap.
  - **MozillaOnline migration (`o`)** — CN distribution-partner migration onto
    mainline Firefox (Brad's official model, ~567K Dec-15 28d-MA).
- **Mobile marketing lift (`m`)** — the Fenix paid-marketing campaign (June-anchored
  "total" variant), applied per-tile inside the forecast.
- **Iran** — the native counterfactual fill is applied during training inside the
  mozaic package, so every column is already Iran-inclusive.

The `prior_june` columns are last cycle's published June forecast (which carried
its own headwind + marketing baked in) and span the entire window for comparison.

---

## `july_dec15_summary.csv`

The one headline number per platform on the measurement date (2026-12-15), matching
the table above.

| Column | Meaning |
|---|---|
| `series` | `Desktop`, `Mobile`, or `ALL`. |
| `measurement_date` | `2026-12-15`. |
| `current_july` | July forecast Dec-15 28d-MA DAU (incl. headwind). |
| `prior_june` | June forecast Dec-15 28d-MA DAU. |
| `delta_vs_june` | `current_july − prior_june`. |
| `target` | Stakeholder target (Desktop only; blank for Mobile/ALL). |
| `vs_target` | `current_july − target` (Desktop only). |

---

## Why some columns are blank in part of the year

- The **`*_current_july`** columns are the new forecast, drawn forecast-only: they
  start on the forecast date (**2026-07-06**) and are empty before it.
- The **`*_actuals`** columns are observed data and stop a couple of days before the
  file was generated (telemetry lands with a lag — the export uses a
  `CURRENT_DATE("America/Los_Angeles") - 2` cutoff; last actual day is 2026-07-05).
- The **`*_prior_june`** columns span the entire window.

This is expected — leave the gaps as gaps when plotting.

---

## Seam smoothing (forecast 28dMA)

The forecast 28-day moving averages are smoothed across the actuals→forecast seam.
A naive trailing 28dMA blends raw actuals into its window for the first 27 forecast
days, which makes the curve oscillate for ~a month before settling. The export
reuses June's variance-matched `display_ma` (from
`../../2026-06/export_canonical_curves.py`): it rebuilds the first forecast weeks so
the trailing 28d window rides the forecast's true trend instead of a straight line.
**Every date from forecast-start +27 days onward (including the Dec-15 headline) is
byte-identical to the naive average** — only the visual seam transition changed.
Full diagnosis, backtest, and before/after examples:
`research/ma-seam-turbulence/report.html`.

---

## Minimal plotting recipe

```python
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

df = pd.read_csv("july_canonical_curves.csv", parse_dates=["date"]).set_index("date")
millions = FuncFormatter(lambda v, _: f"{v/1e6:.1f}M")

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

Generated by the `# [csv-export]` cell in `../july_canonical_v2026-06-29.ipynb`.
Both platforms share the forecast seam **2026-07-06**.

- **Forecast parquets (current July):**
  - Desktop: `../desktop_locked/` (legacy_desktop DAU, forecast_start 2026-07-06;
    params `cps=0.08983 / cpr=0.65 / threshold=−0.032`; overlays `l` + `o`).
  - Mobile: `../mobile_refresh_2026-07-06/` (glean_mobile DAU, `grad_moderate` grid
    params; overlay `m`).
  - `new_profiles` is not exported here — these are DAU-only curves.
- **Prior forecast (June):** last cycle's published June forecast curves.
- **Adjustment spec (headwind):** `../adjustments/headwind.json`
  (desktop −1,345,000 / mobile −27,162 at the 2026-12-15 anchor).
- **Actuals:** `telemetry.active_users_aggregates` (desktop) and
  `glean_telemetry.active_users_aggregates` (mobile) in BigQuery, through
  `CURRENT_DATE("America/Los_Angeles") - 2`.

Each daily series is converted to a 28-day moving average; the headwind ramp value
at each date is then added to the forecast MA over the forecast horizon. The mobile
marketing lift and the desktop `l`/`o` overlays are applied per-tile inside the
forecast, so they are already in the parquets.

Re-run with:

```bash
# Rebuilds csv/july_canonical_curves.csv + csv/july_dec15_summary.csv.
# Open and run every cell of the canonical notebook (needs BigQuery access):
source .venv/bin/activate
jupyter nbconvert --to notebook --execute --inplace \
  data-official/2026-07/july_canonical_v2026-06-29.ipynb
```
