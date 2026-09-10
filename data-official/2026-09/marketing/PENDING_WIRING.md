# Pending wiring — paid-DAU curve pulled 2026-09-10

Written by `scripts/pull_paid_dau_curve.py`. This directory holds a **new, unwired** paid curve. Nothing the
forecast reads was changed. Wiring is a separate step; delete this file once it is done.

- new curve: `data-official/2026-09/marketing/marketing_lift_model.gmio_uac_meta_total_ci90lo.2026-09-02.pull2026-09-10.parquet`
- its meta: `data-official/2026-09/marketing/marketing_lift_model.gmio_uac_meta_total_ci90lo.2026-09-02.pull2026-09-10.meta.json`
- `organic.json` currently points at: `../marketing/marketing_lift_model.gmio_uac_meta_total.2026-09-02.pull2026-09-09.parquet`
- level at seam: 1,606,341; at Dec-15: 1,826,168; at year end: 1,840,192
- feed tables: mozdata.analysis.ahe_gmio_weekly_metrics_ci90_20260909, mozdata.analysis.ahe_gmio_weekly_paid_dau_by_channel_ci90_20260909, mozdata.analysis.ahe_gmio_weekly_paid_dau_by_country_ci90_20260909, mozdata.analysis.ahe_gmio_weekly_paid_dau_totals_ci90_20260909, mozdata.analysis.ahe_gmio_weekly_paid_dau_views_ci90_20260909
- **variant: `ci90lo`** — this is not the point estimate. Wiring it replaces the point-estimate
  paid level with this variant for the published mobile forecast; that is a deliberate decision, not a refresh.

## To wire (not done here)

1. In `data-official/*/organic/organic.json` for seam 2026-09-02: set `paid_forecast.data_file` to
   `../marketing/marketing_lift_model.gmio_uac_meta_total_ci90lo.2026-09-02.pull2026-09-10.parquet` and `paid_forecast.value_column` to `paid_dau_level_daily`.
   There is no anchor: the level is read as delivered. Do not add `anchor_paid_dau`.
2. Pin the new curve in `tests/test_organic.py` (Dec-15 level) and run `pytest tests/test_organic.py -q`.
3. Update this directory's `_index.md` (which pull is live, numbers table) and the cycle `_index.md` ledger row for `p`.
4. Mobile rerun; then re-measure `paid_seam_step`.
5. Delete this file.
