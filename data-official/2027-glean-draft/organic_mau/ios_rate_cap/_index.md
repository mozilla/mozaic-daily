# ios_rate_cap/ — India + Indonesia organic Fenix MAU capped at their own Firefox iOS growth (provisional)

Read `IOS_RATE_CAP.md` first: what the correction is, the evidence, which builds carry it, and the one-flag rollback.

| file | what |
|---|---|
| `IOS_RATE_CAP.md` | the record + REVERT instructions |
| `ios_rate_cap.json` | spec: countries, from date, smoothing days, data file + sha1, runner flag |
| `ios_rate_cap.ID_IN.2026-04-01.parquet` | the series per day × country: organic, iOS, smoothed iOS ratio, cap level, capped, removed |
| `ios_rate_cap.ID_IN.2026-04-01.meta.json` | inputs (raw pull + split, sha1s), the numbers on the last day, evidence pointer |
| `plots/ios_rate_cap.ID_IN.2026-04-01.png` | organic vs capped vs cap path vs iOS indexed, per country, full range + zoom |

Producer `scripts/build_ios_rate_cap.py`; applier `scripts/run_mobile_mau_forecast.py --ios-rate-cap <parquet>` (pure logic
in `src/mozaic_daily/ios_rate_cap.py`). A changed rule (countries, date, smoothing) is a new `ios_rate_cap.<TAG>.<from>.*`
triple beside these, never an edit in place; the spec points at the live one.
