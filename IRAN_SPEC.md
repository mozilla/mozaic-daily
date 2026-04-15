# IRAN_SPEC.md — Implementation Spec for Iran Internet Shutdown Workaround

This document is a machine-readable specification for a Claude Code instance to implement three git branches in the mozaic-daily repository. Read this entire document before starting implementation. Follow the CLAUDE.md file in this repository for all code quality, testing, and documentation standards.

## Background

Iran's internet has been shut down since approximately 2026-02-28 due to geopolitics. The mozaic-daily pipeline currently queries BigQuery for Iran (IR) data as one of the top 15 DAU markets. Since the shutdown, Iran's telemetry data is missing/zero, which corrupts the world-level forecast.

We need to produce two alternative forecasts:
- **"World w/ fake Iran"**: Uses synthetic Iran data (generated from a pre-shutdown forecast) spliced into real data for all other countries
- **"World w/o Iran"**: Completely excludes Iran from all data and aggregation

## Implementation: Three Branches

Each branch is a **direct code modification** off of `main`. No flags, no conditionals, no options threaded through the codebase. Each branch is a self-contained version of the code that does exactly one thing.

### Important constants used across branches

```
IRAN_SHUTDOWN_DATE = "2026-02-27"  # One day before confirmed shutdown, to clear any ramp effects
SYNTHETIC_FORECAST_START = "2026-02-27"
SYNTHETIC_FORECAST_END = "2027-12-31"  # Standard: Dec 31 of (forecast_start_year + 1)
SYNTHETIC_PARQUET_PATH = "data/iran_synthetic/iran_synthetic.parquet"
```

---

## Branch 1: `iran-synthetic-generation`

**Base**: `main`
**Purpose**: Generate synthetic Iran forecast data and save to parquet

### New file: `scripts/generate_iran_synthetic.py`

This standalone script generates a complete Iran-only forecast using the mozaic pipeline and saves the forecast values (with holiday effects included) as a parquet file that Branch 2 will later splice into real data.

#### Step-by-step logic:

1. **Query BigQuery for Iran-only data.** Use the existing `QuerySpec` infrastructure but with `countries = "'IR'"` instead of the full country list.

   ```python
   from mozaic_daily.queries import QUERY_SPECS, ADDITIONAL_HOLIDAYS
   from google.cloud import bigquery

   # Build and execute all 12 queries (3 data sources x 4 metrics) for Iran only
   for spec in QUERY_SPECS.values():
       sql = spec.build_query("'IR'")
       df = bigquery.Client(project).query(sql).to_dataframe()
       # Store in datasets dict structure: datasets[platform][source][metric] = df
   ```

   The `datasets` dict must match the structure expected by mozaic-daily's forecast functions:
   ```python
   datasets = {
       "desktop": {"glean": {}, "legacy": {}},
       "mobile": {"glean": {}}
   }
   ```

2. **Run mozaic for each data source.** Use the existing forecast functions from `mozaic_daily.forecast`:

   ```python
   from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
   from mozaic_daily.queries import DataSource, ADDITIONAL_HOLIDAYS

   FORECAST_START = "2026-02-27"
   FORECAST_END = "2027-12-31"

   # For each data source, run the appropriate forecast function
   # Desktop Glean:
   dfs_glean_desktop = get_desktop_forecast_dfs(
       datasets["desktop"]["glean"], FORECAST_START, FORECAST_END,
       additional_holidays=ADDITIONAL_HOLIDAYS.get(DataSource.GLEAN_DESKTOP, []),
   )
   # Desktop Legacy:
   dfs_legacy_desktop = get_desktop_forecast_dfs(
       datasets["desktop"]["legacy"], FORECAST_START, FORECAST_END,
       additional_holidays=ADDITIONAL_HOLIDAYS.get(DataSource.LEGACY_DESKTOP, []),
   )
   # Mobile Glean:
   dfs_glean_mobile = get_mobile_forecast_dfs(
       datasets["mobile"]["glean"], FORECAST_START, FORECAST_END,
       additional_holidays=ADDITIONAL_HOLIDAYS.get(DataSource.GLEAN_MOBILE, []),
   )
   ```

   Each `dfs_*` is a `Dict[str, pd.DataFrame]` mapping metric names to DataFrames. Each DataFrame has columns: `target_date`, `country`, `population`, `source`, `value`.

   The `value` column for rows where `source == "forecast"` includes holiday effects. This is because `to_granular_forecast_df()` uses the `forecast` column from `Mozaic.to_df()`, which is `(forecast_reconciled + forecasted_holiday_impacts).clip(lower=0)`.

3. **Convert forecast output back to BQ-format DataFrames.** This is the critical transformation. The mozaic output has `population` column values that need to be converted back to boolean segment columns.

   **How populations are derived (in mozaic-forecasting `mozaic/utils.py:50-54`):**
   ```python
   cols = list(set(df.columns) - {"x", "y", "country"})
   df["population"] = (
       df[cols]
       .apply(lambda row: "_".join(col for col in cols if row[col]), axis=1)
       .replace("", "other")
   )
   ```

   For desktop data, `cols` = `['modern_windows', 'winX']` (boolean columns from BQ).
   For mobile data, `cols` = `['fenix_android', 'firefox_ios', 'focus_android', 'focus_ios']`.

   The population is the underscore-joined names of columns where the boolean is True. If all booleans are False, population = `"other"`.

   **Reverse mapping function:**

   ```python
   DESKTOP_SEGMENT_COLUMNS = ['modern_windows', 'winX']
   MOBILE_SEGMENT_COLUMNS = ['fenix_android', 'firefox_ios', 'focus_android', 'focus_ios']

   def population_to_segment_bools(population: str, segment_columns: list[str]) -> dict[str, bool]:
       """Convert a population name back to boolean segment columns.

       Args:
           population: Population string from mozaic output (e.g., "modern_windows", "other")
           segment_columns: List of boolean column names for this platform

       Returns:
           Dict mapping column names to boolean values
       """
       if population == "other":
           return {col: False for col in segment_columns}

       # The population name is a single column name (most common case)
       if population in segment_columns:
           return {col: (col == population) for col in segment_columns}

       # Handle compound populations (multiple True columns joined by "_")
       # Parse by checking which subsets of column names join to match the population string
       active_cols = set()
       remaining = population
       for col in sorted(segment_columns, key=len, reverse=True):  # longest first
           if col in remaining:
               active_cols.add(col)
               remaining = remaining.replace(col, "", 1).strip("_")

       if not remaining and active_cols:
           return {col: (col in active_cols) for col in segment_columns}

       raise ValueError(f"Cannot reverse-map population '{population}' to segment columns {segment_columns}")
   ```

4. **Build the output DataFrame.** For each data source's forecast output:

   a. Filter to `source == "forecast"` rows only (we want synthetic future data, not training actuals)
   b. Filter to `country == "IR"` (skip `"ALL"` aggregate rows)
   c. Filter to `population != "ALL"` (skip aggregate rows)
   d. Apply `population_to_segment_bools()` to create boolean columns
   e. Rename: `target_date` -> `x`, `value` -> `y`
   f. Drop `source` and `population` columns
   g. Add identifier columns: `platform`, `telemetry_source`, `metric`

5. **Combine and save.** Concatenate all DataFrames and save:

   ```python
   combined_df.to_parquet("data/iran_synthetic/iran_synthetic.parquet", index=False)
   ```

   The final parquet schema:
   ```
   platform: str ("desktop" or "mobile")
   telemetry_source: str ("glean" or "legacy")
   metric: str ("DAU", "New Profiles", "Existing Engagement DAU", "Existing Engagement MAU")
   x: datetime64 (date)
   country: str (always "IR")
   y: float64 (metric value)
   modern_windows: bool (null/False for mobile rows)
   winX: bool (null/False for mobile rows)
   fenix_android: bool (null/False for desktop rows)
   firefox_ios: bool (null/False for desktop rows)
   focus_android: bool (null/False for desktop rows)
   focus_ios: bool (null/False for desktop rows)
   ```

#### Script interface:

```
python scripts/generate_iran_synthetic.py [--project PROJECT]
```

- `--project`: BQ project ID (default: `moz-fx-data-bq-data-science`)
- The script should print progress for each data source and metric being processed
- It should create `data/iran_synthetic/` if it doesn't exist
- On completion, print a summary: number of rows, date range, data source/metric combinations found

#### New directory: `data/iran_synthetic/`

- Create with a `.gitkeep` file so the directory is tracked
- Add `data/iran_synthetic/*.parquet` to `.gitignore`

#### Tests: `tests/test_generate_iran_synthetic.py`

Test the `population_to_segment_bools()` function (extract it to a testable location, e.g., a helper module or within the script with an importable function):
- `population_to_segment_bools("modern_windows", DESKTOP_SEGMENT_COLUMNS)` -> `{"modern_windows": True, "winX": False}`
- `population_to_segment_bools("winX", DESKTOP_SEGMENT_COLUMNS)` -> `{"modern_windows": False, "winX": True}`
- `population_to_segment_bools("other", DESKTOP_SEGMENT_COLUMNS)` -> `{"modern_windows": False, "winX": False}`
- `population_to_segment_bools("fenix_android", MOBILE_SEGMENT_COLUMNS)` -> `{"fenix_android": True, "firefox_ios": False, "focus_android": False, "focus_ios": False}`
- `population_to_segment_bools("other", MOBILE_SEGMENT_COLUMNS)` -> all False
- `population_to_segment_bools("unknown_value", DESKTOP_SEGMENT_COLUMNS)` -> raises ValueError

Test the full conversion pipeline with synthetic (mock) mozaic output:
- Create a small DataFrame mimicking `to_granular_forecast_df()` output
- Run it through the conversion
- Verify output has correct boolean columns, correct x/y column names, no ALL rows

No BQ calls in tests — mock the BigQuery client.

#### Documentation:

- Add a docstring to the script explaining its purpose and relationship to Branch 2
- Update CLAUDE.md with a brief note about the Iran synthetic data workflow under a new section

---

## Branch 2: `world-with-fake-iran`

**Base**: `main`
**Purpose**: Run the normal forecast pipeline but with synthetic Iran data spliced into the training data

### Modified file: `src/mozaic_daily/data.py`

Add a new function:

```python
IRAN_SHUTDOWN_DATE = "2026-02-27"
SYNTHETIC_PARQUET_PATH = "data/iran_synthetic/iran_synthetic.parquet"

def splice_iran_synthetic_data(
    datasets: dict,
    synthetic_parquet_path: str,
    shutdown_date: str,
    training_end_date: str,
) -> dict:
    """Replace Iran data from shutdown_date onward with synthetic forecast data.

    Loads synthetic Iran forecast data from a parquet file and splices it into
    the real datasets, replacing any real Iran data from the shutdown date onward.

    The synthetic data was generated by running mozaic for Iran alone with
    forecast_start_date=2026-02-27, and the forecast values (including holiday
    effects) serve as synthetic historical data.

    Args:
        datasets: Nested dict {platform: {source: {metric: DataFrame}}} from get_aggregate_data()
        synthetic_parquet_path: Path to the combined synthetic Iran parquet file
        shutdown_date: Date string (YYYY-MM-DD) from which to replace Iran data
        training_end_date: Date string (YYYY-MM-DD) — only splice synthetic data through this date

    Returns:
        Modified datasets dict with Iran data spliced in

    Raises:
        FileNotFoundError: If synthetic parquet file doesn't exist
    """
```

Logic:
1. Load the synthetic parquet file. Raise `FileNotFoundError` with a descriptive message if it doesn't exist:
   `f"Synthetic Iran data not found at {synthetic_parquet_path}. Run scripts/generate_iran_synthetic.py first."`
2. Convert `shutdown_date` and `training_end_date` to datetime for comparison
3. Iterate over `datasets[platform][source][metric]`:
   a. Build filter: match `platform`, `telemetry_source` (the `source` key maps to telemetry_source: "glean" or "legacy"), and `metric`
   b. Filter synthetic_df to matching rows
   c. Filter to dates: `x >= shutdown_date AND x <= training_end_date`
   d. Drop the identifier columns (`platform`, `telemetry_source`, `metric`) from the filtered synthetic data
   e. Also drop any segment columns that are all-null (e.g., mobile segment columns when processing desktop data)
   f. From the real dataset DataFrame, remove rows where `country == 'IR' AND x >= shutdown_date`
   g. Concatenate the real data (with IR removed from shutdown onward) with the filtered synthetic data
   h. Sort by `x`, `country`
   i. Replace the entry in datasets
4. Return datasets

### Modified file: `src/mozaic_daily/main.py`

After the `datasets = get_aggregate_data(...)` call (around line 258), add:

```python
from .data import splice_iran_synthetic_data

# Splice synthetic Iran data into real datasets
datasets = splice_iran_synthetic_data(
    datasets,
    synthetic_parquet_path="data/iran_synthetic/iran_synthetic.parquet",
    shutdown_date="2026-02-27",
    training_end_date=config['training_end_date'],
)
```

This is a direct code modification. On this branch, every run splices. No flags.

### Tests: `tests/test_splice_iran_data.py`

Create test fixtures that mimic the BQ output format:

```python
def make_desktop_df(countries, dates, value=100.0):
    """Create a mock desktop DataFrame with the correct BQ-output columns."""
    rows = []
    for d in dates:
        for c in countries:
            rows.append({"x": d, "country": c, "modern_windows": True, "winX": False, "y": value})
            rows.append({"x": d, "country": c, "modern_windows": False, "winX": True, "y": value * 0.5})
    return pd.DataFrame(rows)
```

Test cases:
1. **Basic splice**: Real data has IR through 2026-03-30. After splice, IR data from 2026-02-27 onward is replaced with synthetic values. IR data before 2026-02-27 is unchanged. Non-IR data is completely unchanged.
2. **Date truncation**: Synthetic parquet has data through 2027-12-31, but splice only includes through training_end_date.
3. **Missing parquet file**: Raises FileNotFoundError with the expected message.
4. **Column schema preservation**: Output DataFrames have the same columns and dtypes as input.
5. **Multiple data sources**: Test that filtering by platform/telemetry_source/metric works correctly (desktop glean data doesn't get mobile synthetic data).

#### Documentation:

- Docstring on `splice_iran_synthetic_data()`
- Update CLAUDE.md with note about the Iran splice workflow

---

## Branch 3: `world-without-iran`

**Base**: `main`
**Purpose**: Run the normal forecast pipeline with Iran completely excluded from all data

### Modified file: `src/mozaic_daily/config.py`

Line 96-98, remove `"IR"` from `top_DAU_markets`:

```python
# Before:
top_DAU_markets = set(
    ["US", "BR", "CA", "MX", "AR", "IN", "ID", "JP", "IR", "CN", "DE", "FR", "PL", "RU", "IT"]
)

# After:
top_DAU_markets = set(
    ["US", "BR", "CA", "MX", "AR", "IN", "ID", "JP", "CN", "DE", "FR", "PL", "RU", "IT"]
)
```

### Modified file: `src/mozaic_daily/queries.py`

In `QuerySpec.build_query()` (line 206-235), add `AND country != 'IR'` to the WHERE clause so Iran data is completely excluded from query results. Without this, Iran data would silently flow into the ROW (rest-of-world) bucket via the `IF(country IN (...), country, 'ROW')` SQL logic.

```python
# Before:
def build_query(self, countries: str) -> str:
    where_clause = f'{self.where_clause} AND {self.date_constraints.to_sql_clause()}'
    ...

# After:
def build_query(self, countries: str) -> str:
    where_clause = f'{self.where_clause} AND {self.date_constraints.to_sql_clause()} AND country != "IR"'
    ...
```

This is a direct code modification. On this branch, Iran is always excluded.

### Tests:

1. Verify `"IR"` is not in `get_runtime_config()['countries']`
2. Verify `"IR"` is not in `get_runtime_config()['country_string']`
3. Verify `build_query()` output contains `country != "IR"` in the WHERE clause
4. Existing validation tests should still pass (validation_countries dynamically derives from config)

#### Documentation:

- Brief comment in config.py explaining why IR is excluded
- Update CLAUDE.md with note about the Iran exclusion

---

## Key Files Reference

### mozaic-daily (this repo)

| File | Purpose |
|------|---------|
| `src/mozaic_daily/config.py:96-98` | Country list (`top_DAU_markets`) — Branch 3 modifies this |
| `src/mozaic_daily/queries.py:206-235` | `QuerySpec.build_query()` — generates SQL, Branch 3 adds IR exclusion |
| `src/mozaic_daily/queries.py:275-458` | `QUERY_SPECS` dict — all 12 query specifications |
| `src/mozaic_daily/queries.py:470-472` | `ADDITIONAL_HOLIDAYS` — holiday calendars per data source |
| `src/mozaic_daily/data.py:134-195` | `get_aggregate_data()` — fetches BQ data into datasets dict |
| `src/mozaic_daily/main.py:249-258` | Where datasets are loaded — Branch 2 inserts splice call after this |
| `src/mozaic_daily/forecast.py:54-150` | `get_forecast_dfs()` — runs mozaic pipeline |
| `src/mozaic_daily/forecast.py:153-179` | `get_desktop_forecast_dfs()` — desktop wrapper |
| `src/mozaic_daily/forecast.py:182-208` | `get_mobile_forecast_dfs()` — mobile wrapper |

### mozaic-forecasting (external dependency)

| File | Purpose |
|------|---------|
| `mozaic/utils.py:34-86` | `populate_tiles()` — how populations are derived from boolean columns |
| `mozaic/utils.py:89-144` | `curate_mozaics()` — reconciliation pipeline |
| `mozaic/core.py:354-401` | `Mozaic.to_df()` — how `forecast` column is computed (includes holidays) |
| `mozaic/core.py:437-470` | `Mozaic.to_granular_forecast_df()` — per-country/population output |
| `mozaic/core.py:409-428` | `_standard_df_to_forecast_df()` — how `value` is derived from `forecast` |
| `mozaic/core.py:288-297` | Holiday effect clip at -0.6 (now with warning) |
| `mozaic/tile.py:28-44` | `Tile.__post_init__()` — how tiles are created from historical data |

### Data format: BQ query output

Desktop queries return DataFrames with columns:
```
x: datetime64 (date — may be submission_date or first_seen_date depending on metric)
country: str (2-letter ISO code or 'ROW')
modern_windows: bool
winX: bool
y: float64 (metric value, already summed by GROUP BY ALL)
```

Mobile queries return DataFrames with columns:
```
x: datetime64
country: str
fenix_android: bool
firefox_ios: bool
focus_android: bool
focus_ios: bool
y: float64
```

### Data format: mozaic `to_granular_forecast_df()` output

```
target_date: datetime64
country: str (individual country code, or 'ALL')
population: str (segment name like 'modern_windows', 'fenix_android', 'other', or 'ALL')
source: str ('actual' or 'forecast')
value: float64
```

The `value` for `source == "forecast"` rows is derived from:
`(forecast_reconciled + forecasted_holiday_impacts).clip(lower=0).quantile(0.5, axis=1)`

This **includes holiday effects**. This is critical — we specifically want holiday dips in the synthetic data so that when the World pipeline detrends holidays, it will correctly identify and remove them, producing a smooth detrended series.

---

## Implementation Order

Implement the branches in this order:
1. **Branch 1** (`iran-synthetic-generation`) first — it has no dependencies
2. **Branch 3** (`world-without-iran`) second — it's the simplest (two-line change)
3. **Branch 2** (`world-with-fake-iran`) last — it depends on understanding Branch 1's output format

Each branch should be implemented, tested, and committed independently.

---

## Validation Checklist

### Branch 1 validation (after running the script):
- [ ] Parquet file exists at `data/iran_synthetic/iran_synthetic.parquet`
- [ ] Contains rows for all 3 data sources: glean_desktop (4 metrics), legacy_desktop (4 metrics), glean_mobile (4 metrics) = 12 combinations
- [ ] `country` column is always `"IR"`
- [ ] No `"ALL"` values in country or population-derived columns
- [ ] Dates start at 2026-02-27 and extend through 2027-12-31
- [ ] Boolean segment columns are correctly populated (no compound populations like `"modern_windows_winX"`)
- [ ] `y` values are plausible (positive, in a reasonable range for Iran — typically thousands to low hundreds of thousands for DAU)

### Branch 2 validation (after running `scripts/run_main.py`):
- [ ] Pipeline completes without errors
- [ ] Output parquet includes IR as a country
- [ ] IR data from 2026-02-27 onward comes from synthetic source (values should match the synthetic parquet, not be zero/missing)
- [ ] Non-IR country data is completely unchanged from a normal run
- [ ] The spliced data range is exactly shutdown_date through training_end_date (not through 2027)

### Branch 3 validation (after running `scripts/run_main.py`):
- [ ] Pipeline completes without errors
- [ ] IR does not appear anywhere in the output
- [ ] ROW values are identical to a normal run (IR is not folded into ROW)
