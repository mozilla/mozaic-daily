# `test_data/` — fixtures for the test suite

Frozen parquet snapshots used by `tests/` so the suite can run without hitting BigQuery. Two kinds of files:

- `mozaic_parts.raw.<source>.<platform>.<metric>.parquet` — pre-forecast BQ aggregates for each `(source, platform, metric)` combination. Eight files cover the cross-product the pipeline iterates over.
- `mozaic_daily_forecast.<date>.parquet` — a baked forecast output used as the "golden" parquet for end-to-end validation tests.

## Regenerating

These files are produced by running `python scripts/run_main.py --checkpoints` against a real BigQuery training set and copying the resulting checkpoint parquets here. Don't regenerate casually — tests are pinned to the values inside, so a regen requires updating any test that compares against absolute numbers. The forecast date in the filename (`2026-03-03`) is the pinned test reference.
