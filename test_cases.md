# Manual Test Cases for Metaflow Backfill Feature

These tests should be executed manually on Outerbounds infrastructure to verify the implementation.

## Prerequisites

- Activate virtual environment: `source .venv/bin/activate`
- Ensure you have access to `moz-fx-mfouterbounds-prod-f98d` project
- Ensure Docker image is available: `brwells78094/mozaic-daily:v0.0.7_test_amd64`

## Test 1: Local Run

**Command:**
```bash
python scripts/run_flow.py local
```

**Expected Result:**
- Flow runs locally with today's date
- Uses default behavior (forecast_start_date = yesterday)
- Completes successfully and uploads to v2 table

**Verification:**
- Check output for "Running flow locally..."
- Check that forecast_start_date matches yesterday
- Verify data written to `mart_mozaic_daily_forecast_v2`

---

## Test 2: Single Backfill

**Command:**
```bash
python scripts/run_flow.py backfill 2024-06-15 2024-06-15
```

**Expected Result:**
- Runs single historical forecast for June 15, 2024
- Creates log file in `logs/backfill_2024-06-15.log`
- Shows progress: `[1/1] Processing 2024-06-15...`
- Completes with success message

**Verification:**
- Check log file exists and contains run output
- Check BigQuery for data with forecast_start_date = 2024-06-15
- Verify training_end_date = 2024-06-14
- Verify forecast_end_date = 2025-12-31

---

## Test 3: Sequential Backfill (3 dates)

**Command:**
```bash
python scripts/run_flow.py backfill 2024-06-01 2024-06-03
```

**Expected Result:**
- Processes 3 dates sequentially
- Creates 3 log files in `logs/` directory
- Shows progress for each date: `[1/3]`, `[2/3]`, `[3/3]`
- Prints summary with success count

**Verification:**
- Check that 3 log files exist
- Check that dates were processed in order
- Verify BigQuery contains data for all 3 dates

---

## Test 4: Parallel Backfill (2 workers)

**Command:**
```bash
python scripts/run_flow.py backfill 2024-06-01 2024-06-03 --parallel 2
```

**Expected Result:**
- Processes 3 dates with 2 concurrent workers
- Creates 3 log files
- Progress may appear out of order (due to parallelism)
- Summary shows 3 succeeded

**Verification:**
- Check that processes ran in parallel (timestamps in logs)
- Verify all 3 log files exist
- Verify BigQuery contains data for all 3 dates

---

## Test 5: Deploy Scheduled Job

**Command:**
```bash
python scripts/run_flow.py deploy
```

**Expected Result:**
- Runs `python mozaic_daily_flow.py --with retry argo-workflows create`
- Creates or updates scheduled job on Argo Workflows
- Job scheduled for 7 AM daily (cron: `0 7 * * ? *`)
- Uses v2 table as destination

**Verification:**
- Check Argo Workflows UI for updated schedule
- Verify job configuration uses correct Docker image
- Verify job uses `mart_mozaic_daily_forecast_v2` table

---

## Test 6: Future Date Rejection

**Command:**
```bash
python scripts/run_flow.py backfill 2099-01-01 2099-01-02
```

**Expected Result:**
- Flow execution fails with ValueError
- Error message: "forecast_start_date_override (2099-01-01) cannot be in the future"
- Log file captures the error

**Verification:**
- Check that no data written to BigQuery
- Verify error message in log file
- Backfill summary shows failed dates

---

## Test 7: Date Range Validation

**Command:**
```bash
python scripts/run_flow.py backfill 2024-06-30 2024-06-01
```

**Expected Result:**
- Script fails immediately with error
- Error message: "Start date 2024-06-30 is after end date 2024-06-01"
- No flow execution attempted

**Verification:**
- Check that error appears before any flow runs
- No log files created

---

## Test 8: Large Parallel Backfill (1 month, 4 workers)

**Command:**
```bash
python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4
```

**Expected Result:**
- Processes 30 dates with 4 concurrent workers
- Creates 30 log files in `logs/` directory
- Shows progress with ✓/✗ markers as jobs complete
- Prints final summary

**Verification:**
- Check that ~4 processes run concurrently (check system/Kubernetes)
- Verify all 30 log files exist
- Check BigQuery for 30 dates of forecast data
- Verify completion time is ~7-8x faster than sequential

---

## Test 9: Recovery After Failure

**Scenario:** Simulate a failure mid-backfill and verify continuation

**Command:**
```bash
# This requires manual intervention - kill the process after a few dates complete
python scripts/run_flow.py backfill 2024-06-01 2024-06-10 --parallel 2
# (press Ctrl+C after 3-4 dates complete)

# Then re-run the full range
python scripts/run_flow.py backfill 2024-06-01 2024-06-10 --parallel 2
```

**Expected Result:**
- First run: Partial completion, some log files created
- Second run: All dates processed, even those already completed
- BigQuery may have duplicate data for dates run twice (WRITE_APPEND mode)

**Verification:**
- Check that second run completes all dates
- Note: This is expected behavior - no automatic deduplication

---

## Test 10: Verify Table Destination (v2)

**After any successful run:**

**Command:**
```sql
SELECT DISTINCT table_name
FROM `moz-fx-data-shared-prod.forecasts_derived.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'mart_mozaic_daily_forecast%'
```

**Expected Result:**
- Both v1 and v2 tables exist
- v2 table has recent data from test runs
- v1 table has older data (if still in production)

**Verification:**
- Confirm data is written to v2, not v1
- Check row counts and date ranges in both tables

---

## Notes for Testers

1. **Logs Directory**: All run logs are saved to `logs/backfill_YYYY-MM-DD.log` for debugging
2. **Parallel Safety**: Each worker runs independently - safe for parallel execution
3. **Checkpoint Conflicts**: The `--no-checkpoints` flag is not used in flow mode (checkpointing is disabled by default in Metaflow)
4. **Duration Tracking**: Each run logs start/end times - check logs for performance data
5. **Failure Handling**: Failed runs continue processing remaining dates - check summary for failures
