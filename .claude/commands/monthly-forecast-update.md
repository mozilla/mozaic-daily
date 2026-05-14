# Monthly Forecast Update

Run a fresh forecast update using the no-Iran + modeled Iran workflow. This is the standard monthly refresh cadence — not a parameter-tuning exercise.

## Constants

```
DATA_DIR        = data-official/<YYYY-MM>/          # e.g. data-official/2026-05/
IRAN_DIR        = data-official/iran_synthetic/
IRAN_DESKTOP    = data-official/iran_synthetic/iran_synthetic.parquet
IRAN_MOBILE     = data-official/iran_synthetic/mobile/iran_synthetic.parquet
IRAN_PARAMS     = data-official/iran_synthetic/parameters.json
PARAMS_RECORD   = forecast-parameters/<DATE>.md     # committed; created during this run
```

---

## Decision: Parameters

Check whether this month uses the same model parameters as the last official run.

**"Same as last run?"**

→ **YES** — Load the most recent file from `forecast-parameters/`. Use those values for `DESKTOP_CONFIG` and `MOBILE_CONFIG`. Proceed to **Decision: Iran Synthetic**.

→ **NO** — Show the user the parameters from the most recent `forecast-parameters/` file as a suggested starting point.

  **"Use last month's parameters?"**

  → **YES** — Copy that file to `forecast-parameters/<DATE>.md`. Use those values. Proceed to **Decision: Iran Synthetic**.

  → **NO** — Create a blank `forecast-parameters/<DATE>.md` from the template below. Ask the user to fill it in, then **EXIT**. Resume when the file is complete.

  ```markdown
  # Forecast Parameters — <DATE>

  ## Desktop (legacy_desktop)
  - prophet_recent_weeks:
  - changepoint_prior_scale:
  - holiday_threshold:
  - holiday_max_radius:
  - holiday_min_radius:
  - holiday_effect_floor:

  ## Mobile (glean_mobile)
  - prophet_recent_weeks:
  - changepoint_prior_scale:
  - holiday_threshold:
  - holiday_max_radius:
  - holiday_min_radius:
  - holiday_effect_floor:
  ```

---

## Decision: Headwinds

Check whether this month reuses existing headwind values.

**"Use existing headwinds?"**

→ **YES** — Load `DATA_DIR` from the previous month (e.g. `data-official/2026-04/headwinds.json`). Proceed to **Decision: Iran Synthetic**.

→ **NO** — Show the user last month's `headwinds.json` as a suggested starting point.

  **"Use last month's headwinds as a base?"**

  → **YES** — Copy to `data-official/<YYYY-MM>/headwinds.json`. Ask the user to edit values as needed. Proceed once confirmed.

  → **NO** — Create a blank `data-official/<YYYY-MM>/headwinds.json` and **EXIT**. Resume when populated.

---

## Decision: Iran Synthetic

Compare `DESKTOP_CONFIG` and `MOBILE_CONFIG` against the parameters recorded in `IRAN_PARAMS` (if that file exists).

**"Do both configs match the Iran synthetic parameters?"**

→ **YES** — Reuse `IRAN_DESKTOP` and `IRAN_MOBILE`. Proceed to **Step 0**.

→ **NO** (or `IRAN_PARAMS` doesn't exist yet) — Iran synthetic must be regenerated with the new parameters. Run:

  ```bash
  source .venv/bin/activate
  python scripts/generate_iran_synthetic.py \
    --output-dir data-official/iran_synthetic \
    --data-sources legacy_desktop \
    --desktop-config '{"prophet_changepoint_prior_scale": <CPS_D>, "prophet_recent_weeks": <RW_D>, "holiday_threshold": <HT_D>, "holiday_max_radius": <HMX_D>, "holiday_min_radius": <HMN_D>, "holiday_effect_floor": <HEF_D>}'

  python scripts/generate_iran_synthetic.py \
    --output-dir data-official/iran_synthetic/mobile \
    --data-sources glean_mobile \
    --mobile-config '{"prophet_changepoint_prior_scale": <CPS_M>, "prophet_recent_weeks": <RW_M>, "holiday_threshold": <HT_M>, "holiday_max_radius": <HMX_M>, "holiday_min_radius": <HMN_M>, "holiday_effect_floor": <HEF_M>}'
  ```

  Each run saves both a parquet and a `parameters.json` in its output directory. Confirm both exist, then proceed to **Step 0**.

---

## Step 0 — Derive slugs and create output directories

Compute each platform's slug using the `ModelConfig.to_slug()` format:

```
cps{changepoint_prior_scale}_thresh{int(abs(holiday_threshold)*1000)}_recent{prophet_recent_weeks}_clip{abs(holiday_effect_floor)}
```

Examples with April defaults:
- Desktop → `cps0.15983_thresh32_recent13_clip0.6`
- Mobile → `cps0.02_thresh32_recent13_clip0.6`

Set destination paths:
```
DEST_DESKTOP = data-official/<YYYY-MM>/desktop_<desktop_slug>/
DEST_MOBILE  = data-official/<YYYY-MM>/mobile_<mobile_slug>/
```

Create directories:
```bash
mkdir -p "$DEST_DESKTOP" "$DEST_MOBILE"
```

---

## Step 1 — Run no-Iran desktop forecast

Write `tmp/run_desktop_<DATE>.py` with the confirmed desktop parameters:

```python
import sys; sys.path.insert(0, 'src')
import datetime
from mozaic_daily.data import get_aggregate_data, get_queries
from mozaic_daily.config import get_runtime_config, STATIC_CONFIG
from mozaic_daily.forecast import get_desktop_forecast_dfs
from mozaic_daily.tables import combine_tables, update_desktop_format, format_output_table
from mozaic_daily.queries import DataSource
from mozaic.models import DesktopModelConfig

config = DesktopModelConfig(
    prophet_recent_weeks=<RW_D>,
    prophet_changepoint_prior_scale=<CPS_D>,
    holiday_threshold=<HT_D>,
    holiday_max_radius=<HMX_D>,
    holiday_min_radius=<HMN_D>,
    holiday_effect_floor=<HEF_D>,
)

runtime = get_runtime_config(forecast_start_date_override='<DATE>')
datasets = get_aggregate_data(
    get_queries(runtime['country_string'], data_source_filter={DataSource.LEGACY_DESKTOP}),
    STATIC_CONFIG['default_project'],
    checkpoints=True,
    output_dir='tmp/run_desktop_<DATE>',
)
source_data = datasets['desktop']['legacy']
result = get_desktop_forecast_dfs(
    source_data, runtime['forecast_start_date'], runtime['forecast_end_date'], config=config,
)
combined = combine_tables(result.dfs)
update_desktop_format(combined, data_source=DataSource.LEGACY_DESKTOP.value)
df = format_output_table(combined, runtime['forecast_start_date'], runtime['forecast_run_dt'])
df.to_parquet('<DEST_DESKTOP>/mozaic_daily_forecast.<DATE>.ld-D.parquet', index=False)
print(f'Done. Shape: {df.shape}, dates: {df.target_date.min()} to {df.target_date.max()}')
```

Run it:
```bash
source .venv/bin/activate
python tmp/run_desktop_<DATE>.py
```

~20–30 minutes. Do not interrupt.

**Verify:** Confirm the output parquet exists and print shape + date range (the script prints this on completion).

---

## Step 2 — Run no-Iran mobile forecast

Write `tmp/run_mobile_<DATE>.py` with the confirmed mobile parameters:

```python
import sys; sys.path.insert(0, 'src')
import datetime
from mozaic_daily.data import get_aggregate_data, get_queries
from mozaic_daily.config import get_runtime_config, STATIC_CONFIG
from mozaic_daily.forecast import get_mobile_forecast_dfs
from mozaic_daily.tables import combine_tables, update_mobile_format, format_output_table
from mozaic_daily.queries import DataSource
from mozaic.models import MobileModelConfig

config = MobileModelConfig(
    prophet_recent_weeks=<RW_M>,
    prophet_changepoint_prior_scale=<CPS_M>,
    holiday_threshold=<HT_M>,
    holiday_max_radius=<HMX_M>,
    holiday_min_radius=<HMN_M>,
    holiday_effect_floor=<HEF_M>,
)

runtime = get_runtime_config(forecast_start_date_override='<DATE>')
datasets = get_aggregate_data(
    get_queries(runtime['country_string'], data_source_filter={DataSource.GLEAN_MOBILE}),
    STATIC_CONFIG['default_project'],
    checkpoints=True,
    output_dir='tmp/run_mobile_<DATE>',
)
source_data = datasets['mobile']['glean']
result = get_mobile_forecast_dfs(
    source_data, runtime['forecast_start_date'], runtime['forecast_end_date'], config=config,
)
combined = combine_tables(result.dfs)
update_mobile_format(combined, data_source=DataSource.GLEAN_MOBILE.value)
df = format_output_table(combined, runtime['forecast_start_date'], runtime['forecast_run_dt'])
df.to_parquet('<DEST_MOBILE>/mozaic_daily_forecast.<DATE>.gm-D.parquet', index=False)
print(f'Done. Shape: {df.shape}, dates: {df.target_date.min()} to {df.target_date.max()}')
```

Run it (can be started in a **separate terminal in parallel with Step 1** — the two runs share no state):
```bash
source .venv/bin/activate
python tmp/run_mobile_<DATE>.py
```

**Verify:** Confirm the output parquet exists and review the shape + date range printout.

---

## Step 3 — Save parameters to each output folder

Write `parameters.json` into each destination so the run is self-documenting:

```python
import json

desktop = {
    "platform": "legacy_desktop",
    "forecast_start_date": "<DATE>",
    "prophet_recent_weeks": <RW_D>,
    "changepoint_prior_scale": <CPS_D>,
    "holiday_threshold": <HT_D>,
    "holiday_max_radius": <HMX_D>,
    "holiday_min_radius": <HMN_D>,
    "holiday_effect_floor": <HEF_D>,
}
with open("<DEST_DESKTOP>/parameters.json", "w") as f:
    json.dump(desktop, f, indent=2)

mobile = {
    "platform": "glean_mobile",
    "forecast_start_date": "<DATE>",
    "prophet_recent_weeks": <RW_M>,
    "changepoint_prior_scale": <CPS_M>,
    "holiday_threshold": <HT_M>,
    "holiday_max_radius": <HMX_M>,
    "holiday_min_radius": <HMN_M>,
    "holiday_effect_floor": <HEF_M>,
}
with open("<DEST_MOBILE>/parameters.json", "w") as f:
    json.dump(mobile, f, indent=2)
```

Also write `data-official/<YYYY-MM>/headwinds.json` if it doesn't already exist (from the headwinds decision above).

Confirm `forecast-parameters/<DATE>.md` exists and is complete.

---

## Step 4 — Add Iran back to each forecast

```bash
python scripts/add_iran_to_forecast.py \
  --input "$DEST_DESKTOP/mozaic_daily_forecast.<DATE>.ld-D.parquet"

python scripts/add_iran_to_forecast.py \
  --input "$DEST_MOBILE/mozaic_daily_forecast.<DATE>.gm-D.parquet"
```

Each script writes a `.plus_iran.parquet` alongside the input. **Verify:** confirm all four parquet files exist in their respective destination folders before continuing.

---

## Step 5 — Create the analysis notebook

The notebook lives alongside the data in `DATA_DIR`. Identify the most recent `*_composite_forecast.ipynb` (anywhere in the project root), determine the current month name, and copy it:

```bash
cp <PREV_MONTH>_composite_forecast.ipynb data-official/<YYYY-MM>/<MONTH>_composite_forecast.ipynb
```

Do not overwrite the previous month's notebook.

Update the `setup` cell:

- `MOBILE_NO_IRAN_PATH`   → `<DEST_MOBILE>/mozaic_daily_forecast.<DATE>.gm-D.parquet`
- `MOBILE_PLUS_IRAN_PATH` → `<DEST_MOBILE>/mozaic_daily_forecast.<DATE>.gm-D.plus_iran.parquet`
- `DESKTOP_NO_IRAN_PATH`  → `<DEST_DESKTOP>/mozaic_daily_forecast.<DATE>.ld-D.parquet`
- `DESKTOP_PLUS_IRAN_PATH`→ `<DEST_DESKTOP>/mozaic_daily_forecast.<DATE>.ld-D.plus_iran.parquet`
- `FORECAST_START`        → forecast start date
- `BQ_START`              → 28 days before `DISPLAY_START`
- `HEADWINDS`             → load from `data-official/<YYYY-MM>/headwinds.json`
- `csv_path`              → `"data-official/<YYYY-MM>/<MONTH>_composite_forecast_28ma.csv"`

Do not change `HEADWIND_ANCHOR`, `DISPLAY_END`, or `MEASUREMENT_DATE` unless the user asks.

---

## Step 6 — Run the notebook and verify

Run all cells in `data-official/<YYYY-MM>/<MONTH>_composite_forecast.ipynb`. Sanity check:

- `desktop-dec15` and `mobile-dec15` cells print reasonable Dec 15 28-day MA values (compare against previous month for plausibility)
- `export-csv` writes `<MONTH>_composite_forecast_28ma.csv` to `data-official/<YYYY-MM>/` successfully
- No cells error out

Report the Dec 15 values to the user for review before calling the task done.

---

## Notes

- Steps 1 and 2 can run simultaneously in separate terminals — they use separate output directories and share no state.
- If a run fails mid-way, raw BigQuery data is checkpointed in `tmp/run_desktop_<DATE>/` or `tmp/run_mobile_<DATE>/`. Re-running the script resumes from checkpoints.
- Run scripts in `tmp/` are single-use and not committed.
- `parameters.json` files in each output folder and `forecast-parameters/<DATE>.md` are the durable record — these are committed (parquet files are not).

---

## Follow-up items

- **Bootstrap Iran parameters** — `data-official/iran_synthetic/parameters.json` and `data-official/iran_synthetic/mobile/parameters.json` do not exist yet. Create them manually (one-time) by running `generate_iran_synthetic.py` with no config args (uses defaults), or by writing the JSON directly from the known April parameters. Until this exists, the Iran decision tree above will always trigger a regeneration.
- **GCP parquet storage** — parquet files are archived to GCS but the process for doing so (gsutil path, bucket name, sync cadence) is not yet documented in this skill. Investigate and add a "Step 7 — Archive to GCS" section once the workflow is understood.
