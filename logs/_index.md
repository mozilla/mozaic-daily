# `logs/` — backfill run logs and state files

Gitignored (`logs/` in `.gitignore`). Contains:

- `backfill_YYYY-MM-DD.log` — per-date backfill run output; reruns get `.run2.log`, `.run3.log` suffixes
- `backfill_state_{start}_{end}[_{weekday}].json` — state file tracking completed + failed dates for `--resume`

## Producers

- `scripts/run_flow.py backfill ...` writes both the log and the state file
- `scripts/check_logs.py` consumes the logs to find successes / failures / ambiguous runs

## Retention

These are local-only debugging artifacts. There's no strict retention rule; clear when the dir gets unwieldy. The state files are needed only while a backfill is mid-flight (for `--resume`); once a backfill completes successfully its state file is informational only.
