# Marketing-lift scenario notebook

A self-contained notebook that takes a weekly paid-DAU forecast CSV from the marketing team and produces a 2026 mobile-DAU plot in the same style as our internal forecast diagnostics. Headwinds are applied automatically; nothing else needs configuring beyond pointing the notebook at the CSV.

## Snapshot

This bundle is a one-time snapshot of the 2026-06 forecast cycle, frozen at **training-end 2026-05-16**. The bundled parquets reflect production state at that date. To run scenarios against a later cycle, request a refreshed bundle.

## What the stakeholder provides

A single CSV with weekly paid-DAU values. **Required:** one Monday-date column and one total-paid-DAU column. **Allowed:** any other columns you want — they're ignored.

Example (matches `example_csv.csv` bundled here):

```
week,paid_dau_gt_1yr,paid_dau_lte_12mo,total_paid_dau
2026-01-05 00:00:00,"150,109","694,655","844,764"
2026-01-12 00:00:00,"150,668","710,848","861,517"
...
```

The notebook reads only `week` and `total_paid_dau`. The other columns can stay; they have no effect.

**Format rules** (the notebook checks them and fails loudly if violated):

- All date values must fall on **Mondays**.
- Comma-thousands (`"1,019,515"`) is fine.
- One row per week. Gaps and out-of-order rows are tolerated as long as every row is a Monday.
- The CSV may extend beyond 2026 — the notebook will only use values up through 2026-12-31.

## How to run

1. Drop the CSV anywhere on your machine (the bundle folder is fine).
2. Launch Jupyter **from inside this folder**, then open `scenario_notebook.ipynb`.
3. Edit only the `[user-config]` cell at the top:
   ```python
   CSV_PATH = "example_csv.csv"          # path to your CSV (relative to this folder, or absolute)
   CSV_DATE_COLUMN = "week"              # name of the Monday-date column
   CSV_LIFT_COLUMN = "total_paid_dau"    # name of the total paid-DAU column
   SCENARIO_LABEL = "My scenario"        # appears in the plot legend
   ```
4. **Run All**. The full notebook executes in under five seconds.

## Reading the plot

Four lines on the plot, plus three gold markers at Dec 15:

| Line | What it is |
|---|---|
| **Black — Actuals** | Observed ex-Iran mobile DAU, 28-day moving average, through May 18 |
| **Blue — April N-1 forecast** | What we forecast back in April (no marketing assumed), with headwinds applied. Reference for "would we have hit the target without re-forecasting?" |
| **Green — Your scenario** | The no-marketing baseline plus your CSV's implied lift, with headwinds applied |
| **Orange dashed — No-marketing baseline** | What the June refit predicts if no further marketing is applied. The vertical gap between Green and Orange is your scenario's implied marketing benefit |
| **Gold markers** | The Stretch / Base / Low targets at Dec 15 |

The summary cell at the end prints exact Dec-15 28-day-MA values and gap-vs-target deltas.

## The hybrid lift recipe (what `[build-hybrid-lift]` does)

The notebook constructs the daily marketing-lift series this way:

```
For d <  2026-04-06 (campaign launch): lift(d) = 0
For 2026-04-06 ≤ d ≤ 2026-05-16:       lift(d) = empirical Fenix Android gap on that day
                                                 (June actuals − April forecast, bundled)
For d > 2026-05-16:                    lift(d) = gap_at_2026-05-16
                                                 + (your_csv_daily(d) − your_csv_daily(2026-05-16))
```

Translation: the historical six weeks of post-launch lift come from observed data, not from the CSV. Your CSV only determines the **forward growth pattern** from training-end onward. This makes the Validation-A fit (model lift vs empirical gap) zero by construction over the historical window — the model can't be "wrong" about the past.

## Math caveats (read before reporting numbers)

- The scenario line is computed as `no_marketing_baseline + hybrid_lift`. The bundled `no_marketing_baseline` is **derived** from the production pipeline as `(production_hybrid_forecast − production_lift)` at the world rollup — i.e. the Prophet "no-marketing" trajectory the m-applier learned when it subtracted the lift from training. This gives a smooth join with actuals at forecast start and reproduces the production v2 hybrid Dec-15 value (16,836,397) to the dollar when the example CSV is fed through.
- All numbers are mobile-only (Fenix + iOS + Focus), excluding Iran (the 2026-02-28 internet shutdown).
- The hybrid stitch assumes the campaign's **growth pattern from training-end onward** is what the CSV captures. The **level** at training-end is anchored to the empirical Fenix gap, not the CSV.

## Bundle contents

```
scenario_notebook.ipynb          # the notebook
example_csv.csv                  # a working template (the v2 marketing CSV)
data/
  no_mktg_baseline_world_daily.parquet   # June raw mobile forecast, world rollup
  april_n1_world_daily.parquet           # April N-1 mobile forecast, world rollup
  fenix_gap_daily.parquet                # empirical Fenix gap, historical
  mobile_actuals_daily.parquet           # observed mobile DAU (ex-Iran)
  headwind_spec.json                     # linear-ramp headwind spec for 2026-06
  stakeholder_targets.json               # the three Dec-15 target values
  snapshot_manifest.json                 # snapshot date and source-file references
```

## Dependencies

The notebook imports only `pandas`, `numpy`, `matplotlib`, and Python stdlib. No `mozaic`, no `google.cloud.bigquery`, no BigQuery access at runtime. Any recent jupyter + pandas/numpy/matplotlib install will run it (tested against pandas 3.0 / Python 3.14).
