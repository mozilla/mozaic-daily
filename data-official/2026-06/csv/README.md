# June 2026 Canonical Forecast Curves — CSV exports

Static, plot-ready exports of the June 2026 canonical forecast review
(`../june_canonical_v2026-05-27.ipynb`). Read them into pandas and plot — no
BigQuery, no model code needed.

Two kinds of file live here:

| File | Scope | Headwind applied? | Iran columns? |
|---|---|---|---|
| `june_canonical_curves.csv` | **ALL** (Desktop+Mobile world total) | **Yes** | Yes (`*_plus_iran` / `*_no_iran`) |
| `per_country/june_canonical_curves.<CC>.no-headwinds.csv` | One per forecasted country | **No** (raw model output) | No (synthetic Iran is ALL-level only) |

All values in every file are **28-day moving averages of daily active users
(DAU)**, in absolute user counts, daily from **2026-01-01 through 2026-12-31**.

- **Desktop** = Firefox Desktop only (Glean desktop is excluded from these exports).
- **Mobile** = Fenix (Android) + Firefox iOS + Focus Android + Focus iOS.

---

## Prompt for an AI agent

> I have a CSV of Firefox DAU forecast curves. The first column is `date` (daily,
> 2026-01-01 through 2026-12-31). Every other column is a **28-day moving average
> of DAU**. Columns are grouped into `desktop_*` and `mobile_*`. Load it into
> pandas (parse `date` as datetime) and make two line charts — one for the
> `desktop_*` columns, one for the `mobile_*` columns — with date on the x-axis and
> DAU (formatted in millions) on the y-axis. Some columns are intentionally blank
> early in the year (the "current_june" forecast columns only start at the forecast
> date **2026-05-26**; the "actuals" columns stop a couple of days before today
> because the most recent telemetry day is still landing). Let those gaps render as
> gaps.

---

## `june_canonical_curves.csv` (ALL aggregate)

The 12 series from the canonical review: 6 desktop + 6 mobile. Forecast curves
**include the headwind adjustment** (see "What's baked in").

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. 2026-01-01 .. 2026-12-31. |
| `desktop_actuals_all_countries` | Desktop | Observed DAU, all countries (incl. Iran). Ends ~2 days before export. |
| `desktop_actuals_excl_ir` | Desktop | Observed DAU, excluding country `IR`. |
| `desktop_prior_april_plus_iran` | Desktop | **April** forecast (prior cycle), synthetic Iran added back. Full-year. |
| `desktop_prior_april_no_iran` | Desktop | April forecast (prior cycle), Iran excluded. Full-year. |
| `desktop_current_june_plus_iran` | Desktop | **June** forecast (current cycle), synthetic Iran. Blank before 2026-05-26. |
| `desktop_current_june_no_iran` | Desktop | June forecast (current cycle), Iran excluded. Blank before 2026-05-26. |
| `mobile_actuals_all_countries` | Mobile | Observed DAU, all countries (incl. Iran). |
| `mobile_actuals_excl_ir` | Mobile | Observed DAU, excluding `IR`. |
| `mobile_prior_april_plus_iran` | Mobile | April forecast (prior cycle), synthetic Iran. Full-year. |
| `mobile_prior_april_no_iran` | Mobile | April forecast (prior cycle), Iran excluded. Full-year. |
| `mobile_current_june_plus_iran` | Mobile | June forecast (current cycle), synthetic Iran. Blank before 2026-05-26. |
| `mobile_current_june_no_iran` | Mobile | June forecast (current cycle), Iran excluded. Blank before 2026-05-26. |

### Key Dec-15 numbers (28-day MA of DAU)

| Series | April | June | Δ vs April |
|---|---:|---:|---:|
| Desktop no-Iran | 46,891,136 | 46,893,112 | +1,976 |
| Desktop +Iran | 47,832,386 | 47,834,362 | +1,976 |
| Mobile no-Iran | 16,389,749 | 16,911,773 | +522,024 |
| Mobile +Iran | 16,991,373 | 17,511,100 | +519,727 |

June desktop is sized to land within ~2k of April's Dec-15 28dMA. The mobile lift
comes from the Fenix paid-marketing campaign (below); it is **not** a headwind
difference.

### What's baked in (ALL file only)

- All ALL-file forecast curves include the **headwind adjustment** (a linear ramp
  from 2026-04-01 to a Dec-15 anchor):
  - April desktop anchor: −1,497,870 DAU
  - June desktop anchor: **−1,420,000 DAU** (~5% addback vs April — Prophet
    absorbed some of the predicted Win10 headwind into its trend on Apr–May actuals)
  - Mobile anchor: −27,162 DAU on both cycles
- The **mobile** June forecast also has the **Fenix paid-marketing lift** baked in
  (v2 hybrid model: empirical actuals through 2026-05-20, then anchored at
  training-end-gap plus the v2 marketing-team CSV's growth shape).
- The Dec-15 stakeholder marker points (low / baseline / stretch) from the source
  notebook are **not** in this CSV — they're zero-dimensional markers, not curves.

---

## `per_country/june_canonical_curves.<CC>.no-headwinds.csv`

One file per forecasted country. `<CC>` is a country code or `ROW` (rest of world
— every country outside the 14 named markets, excluding Iran, matching how the
no-Iran forecast partitions countries). 15 files: AR, BR, CA, CN, DE, FR, ID, IN,
IT, JP, MX, PL, ROW, RU, US.

These are **raw model output**:

- **No headwind.** The headwind is an ALL-level total (−1.42M desktop / −27.2k
  mobile at Dec-15) and cannot be meaningfully split per country, so it is not
  applied here. The `.no-headwinds.` in the filename flags this.
- **No plus-Iran columns.** Synthetic Iran exists only at the ALL aggregate; per
  country there is a single forecast curve per platform.

Summing the per-country `*_current_june` columns reproduces the ALL **raw** (pre-
headwind) forecast; it therefore sits above `june_canonical_curves.csv`'s
`*_current_june_no_iran` by exactly the headwind ramp at each date.

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. 2026-01-01 .. 2026-12-31. |
| `desktop_actuals` | Desktop | Observed DAU for this country/bucket. Ends ~2 days before export. |
| `desktop_prior_april` | Desktop | April forecast (prior cycle), raw. Full-year. |
| `desktop_current_june` | Desktop | June forecast (current cycle), raw. Blank before 2026-05-26. |
| `mobile_actuals` | Mobile | Observed DAU for this country/bucket. |
| `mobile_prior_april` | Mobile | April forecast (prior cycle), raw. Full-year. |
| `mobile_current_june` | Mobile | June forecast (current cycle), raw. Blank before 2026-05-26. |

---

## Why some columns are blank in part of the year

- The **`*_current_june*`** columns are the new forecast, drawn forecast-only:
  they start on the forecast date (**2026-05-26**) and are empty before it.
- The **`*_actuals*`** columns are observed data and naturally stop a couple of
  days before the file was generated (telemetry lands with a lag — the export uses
  a `CURRENT_DATE("America/Los_Angeles") - 2` cutoff to avoid a still-landing day).
- The **`*_prior_april*`** columns span the entire window.

This is expected — leave the gaps as gaps when plotting.

---

## Seam smoothing (forecast 28dMA)

The forecast 28-day moving averages are smoothed across the actuals→forecast seam. A naive
trailing 28dMA blends raw actuals into its window for the first 27 forecast days; because the
forecast's weekly (weekday/weekend) amplitude is damped relative to recent actuals for
high-swing countries, that blend made the curve oscillate for ~a month before settling. The
export's `display_ma` replaces those first 27 forecast days with a **variance-matched
transition**: the forecast's daily values are rebuilt to carry the recent actuals' weekly
amplitude, so the trailing 28d window cancels the weekly cycle and the transition rides the
forecast's *true trend* (curvature and all) rather than a straight line. **Every date from
forecast-start +27 days onward (including the Dec-15 headline) is byte-identical to the naive
average** — only the visual seam transition changed. Validated out-of-sample on the April
forecast: on the **global ALL-level curve** the transition's shape error vs realized drops ~70%
(desktop). This is a global-curve smoother — a few small high-volatility countries can do worse
on demeaned shape (the April forecast's own curvature was wrong there), and the highest-swing
countries (AR, BR) keep a small <~1% kink at the +27d hand-off; the global hand-off is smooth.
Full diagnosis, backtest, and before/after examples: `research/ma-seam-turbulence/report.html`.

---

## Minimal plotting recipe

```python
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

df = pd.read_csv("june_canonical_curves.csv", parse_dates=["date"]).set_index("date")
millions = FuncFormatter(lambda v, _: f"{v/1e6:.1f}M")

for platform in ["desktop", "mobile"]:
    cols = [c for c in df.columns if c.startswith(platform)]
    ax = df[cols].plot(figsize=(14, 6), title=f"2026 {platform.title()} DAU — 28-day MA")
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(millions)
    plt.tight_layout()
    plt.show()
```

`df[cols].plot` skips NaNs automatically, so blank early-year cells render as gaps.
The same recipe works on any `per_country/*.csv` file.

---

## Provenance / regenerating

Generated by `../export_canonical_curves.py`, which reproduces the series from
`../june_canonical_v2026-05-27.ipynb`.

- **Forecast parquets:**
  - June desktop: `../desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw[.plus_iran].parquet`
  - June mobile: `../mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.gm-D.adj-m[.plus_iran].parquet`
  - April desktop: `../../2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw[.plus_iran].parquet`
  - April mobile: `../../2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw[.plus_iran].parquet`
- **Adjustment specs (ALL file only):**
  - June: `../adjustments/headwind.json` + `../marketing/marketing.json` (v2 hybrid lift)
  - April: `../../2026-04/adjustments/headwind.json` (no marketing-lift on April; pre-campaign)
- **Actuals:** `telemetry.active_users_aggregates` (desktop) and
  `glean_telemetry.active_users_aggregates` (mobile) in BigQuery, with
  `submission_date BETWEEN '2025-12-04' AND CURRENT_DATE("America/Los_Angeles") - 2`.
  Per-country actuals bucket every country outside the 14 named markets into `ROW`
  and exclude `IR`.

Each daily series is converted to a 28-day moving average. For the ALL file, the
headwind ramp value at each date is then added to the MA over the forecast horizon
(forecast-start onward); mobile marketing-lift is applied per-tile inside the
forecast itself, so it is already in the parquet. The per-country files apply no
post-forecast adjustment.

Re-run with:

```bash
# ALL canonical curves (csv/june_canonical_curves.csv)
source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py

# One no-headwind CSV per country (csv/per_country/)
source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py --per-country
```

(both require BigQuery access for the actuals).
