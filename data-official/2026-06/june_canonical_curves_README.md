# June 2026 Canonical Forecast Curves — data + how to use it

**File:** `june_canonical_curves.csv`
**What it is:** a static, plot-ready export of the curves from the June 2026 canonical forecast review (`june_canonical_v2026-05-27.ipynb`). Read it into pandas and plot — no BigQuery, no model code needed.

---

## Prompt for an AI agent

> I have a CSV, `june_canonical_curves.csv`. The first column is `date` (daily, 2026-01-01 through 2026-12-31). Every other column is a **28-day moving average of DAU** for Firefox. There are 6 desktop columns and 6 mobile columns. Load it into pandas (parse `date` as a datetime), then make two line charts — one for the `desktop_*` columns and one for the `mobile_*` columns — with date on the x-axis and DAU on the y-axis. Some columns are intentionally blank early in the year (the "current_june" forecast columns only start at the forecast date, **2026-05-26**; the "actuals" columns stop a couple of days before today because the most recent telemetry day is usually still landing). Just let those gaps render as gaps. Format the y-axis in millions.

---

## Columns

All values are **28-day moving averages of daily active users (DAU)**, in absolute user counts.

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. Spans 2026-01-01 .. 2026-12-31. |
| `desktop_actuals_all_countries` | Desktop | Observed DAU, all countries (incl. Iran). Ends ~2 days before the export date. |
| `desktop_actuals_excl_ir` | Desktop | Observed DAU, excluding country `IR` (Iran). |
| `desktop_prior_april_plus_iran` | Desktop | **April** forecast (prior cycle), with synthetic Iran added back. Full-year. |
| `desktop_prior_april_no_iran` | Desktop | April forecast (prior cycle), Iran excluded. Full-year. |
| `desktop_current_june_plus_iran` | Desktop | **June** forecast (current cycle), with synthetic Iran. Blank before 2026-05-26. |
| `desktop_current_june_no_iran` | Desktop | June forecast (current cycle), Iran excluded. Blank before 2026-05-26. |
| `mobile_actuals_all_countries` | Mobile | Observed DAU, all countries (incl. Iran). |
| `mobile_actuals_excl_ir` | Mobile | Observed DAU, excluding `IR`. |
| `mobile_prior_april_plus_iran` | Mobile | April forecast (prior cycle), with synthetic Iran. Full-year. |
| `mobile_prior_april_no_iran` | Mobile | April forecast (prior cycle), Iran excluded. Full-year. |
| `mobile_current_june_plus_iran` | Mobile | June forecast (current cycle), with synthetic Iran. Blank before 2026-05-26. |
| `mobile_current_june_no_iran` | Mobile | June forecast (current cycle), Iran excluded. Blank before 2026-05-26. |

- **Mobile** = Fenix (Android) + Firefox iOS + Focus Android + Focus iOS.
- **Desktop** = Firefox Desktop only (Glean desktop is excluded from this export).

### Why some columns are blank in part of the year

- The two **`*_current_june_*`** columns are the new forecast. They are drawn forecast-only, so they start on the forecast date (**2026-05-26**) and are empty before it — matching the source notebook exactly.
- The **`*_actuals_*`** columns are observed data and naturally stop a couple of days before the file was generated (telemetry lands with a lag — the export uses a `CURRENT_DATE("America/Los_Angeles") - 2` cutoff to avoid pulling a still-landing partial day).
- The **`*_prior_april_*`** columns span the entire window.

This is expected — leave the gaps as gaps when plotting.

### Key Dec-15 numbers (28-day MA of DAU)

| Series | April | June | Δ vs April |
|---|---:|---:|---:|
| Desktop no-Iran | 46,891,136 | 46,893,112 | +1,976 |
| Desktop +Iran | 47,832,386 | 47,834,362 | +1,976 |
| Mobile no-Iran | 16,389,749 | 16,911,773 | +522,024 |
| Mobile +Iran | 16,991,373 | 17,511,100 | +519,727 |

June desktop is sized to land within ~2k of April's Dec-15 28dMA. The mobile lift comes from the Fenix paid-marketing campaign (see "What's baked in" below); it is **not** a headwind difference.

### What's baked in

- All forecast curves already include the **headwind adjustment** (a linear ramp from 2026-04-01 to a Dec-15 anchor):
  - April desktop anchor: −1,497,870 DAU
  - June desktop anchor: **−1,420,000 DAU** (~5% addback vs April — Prophet absorbed some of the predicted Win10 headwind into its trend on Apr–May actuals)
  - Mobile anchor: −27,162 DAU on both cycles
- The **mobile** June forecast also has the **Fenix paid-marketing lift** baked in (v2 hybrid model: empirical actuals through 2026-05-20, then anchored at training-end-gap plus the v2 marketing-team CSV's growth shape).
- The Dec-15 stakeholder marker points (low / baseline / stretch) from the source notebook are **not** in this CSV by request — they're zero-dimensional markers, not curves.

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

`df[cols].plot` skips NaNs automatically, so the blank early-year cells render as gaps.

---

## Provenance / regenerating

Generated by `data-official/2026-06/export_canonical_curves.py`, which reproduces the series from `june_canonical_v2026-05-27.ipynb`:

- **Forecast parquets:**
  - June desktop: `data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw[.plus_iran].parquet`
  - June mobile: `data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.gm-D.adj-m[.plus_iran].parquet`
  - April desktop: `data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw[.plus_iran].parquet`
  - April mobile: `data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw[.plus_iran].parquet`
- **Adjustment specs:**
  - June: `data-official/2026-06/adjustments/headwind.json` + `data-official/2026-06/marketing/marketing.json` (v2 hybrid lift)
  - April: `data-official/2026-04/adjustments/headwind.json` (no marketing-lift on April; pre-campaign)
- **Actuals:** `telemetry.active_users_aggregates` (desktop) and `glean_telemetry.active_users_aggregates` (mobile) in BigQuery, with `submission_date BETWEEN '2025-12-04' AND CURRENT_DATE("America/Los_Angeles") - 2`.

Each daily series is converted to a 28-day moving average, then the headwind ramp value at each date is added to the MA over the forecast horizon (forecast-start onward). Mobile marketing-lift is applied per-tile inside the forecast itself (subtract from training, add back to forecast), so it's already in the parquet — no additional handling at export time.

Re-run with:

```bash
source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py
```

(requires BigQuery access for the actuals).
