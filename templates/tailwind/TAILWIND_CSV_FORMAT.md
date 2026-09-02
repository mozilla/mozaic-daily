# Daily tailwind CSV — format

Send us one CSV with one row per calendar day. Each row tells us how much incremental Firefox DAU
your effect adds on that day, and whether you measured that number or modelled it. The file next to
this document, `example_daily_tailwind.mozillaonline_2026-07.csv`, is a real one.

## Shape

```
submission_date,type,dau
2026-06-01,actuals,0
2026-06-02,actuals,21000
2026-06-03,actuals,200830
...
2026-06-28,actuals,526710
2026-06-29,forecast,802771
...
2026-12-31,forecast,569898
```

| column | type | meaning |
|---|---|---|
| `submission_date` | `YYYY-MM-DD` | The calendar day, in UTC. |
| `type` | `actuals` or `forecast` | Write `actuals` if telemetry measured this day's `dau`; write `forecast` if your model produced it. |
| `dau` | number | The **incremental** DAU your effect adds on this day, world total. Positive values raise the forecast; negative values lower it. |

## Need to have

- **Use these three column names.** You may add other columns; we drop them.
- **Write `type` as exactly `actuals` or `forecast`, lower case, and put every `forecast` row after
  every `actuals` row.** We read the measured-to-modelled boundary from this column, and we rely on
  it more than on anything else in the file.
- **Report a daily level.** Each row answers "how many more (or fewer) DAU did the effect produce
  that day?" Do not send a moving average, a weekly total, a cumulative sum, or a day-over-day
  change.
- **Count DAU on the KPI's own population**, Firefox Desktop DAU or Firefox mobile DAU. Do not
  count clients, installs, or sessions.
- **Cover every day through 31 December of the current forecast year.**

## Nice to have

- Send only the three columns, in the order shown, with no index column.
- Sort the rows by date, ascending.
- Include every day. If you skip a day we fill it, and we don't promise how.
- Write whole numbers, without thousands separators or quotes.
- Keep one sign per file. We accept mixed signs, but separate files for a tailwind and a headwind
  are easier for us to handle.
- Extend coverage through 31 December of the *following* year, where the forecast horizon ends.

## The example

`example_daily_tailwind.mozillaonline_2026-07.csv` covers 2026-06-01 to 2026-12-31, 214 rows.
The series starts at 0, ramps as MozillaOnline users migrate onto mainline Firefox, peaks near
985K on 2026-06-25, and decays to about 570K by year end. The `actuals` rows run from 2026-06-01
to 2026-06-28; every later row is `forecast`.

## For agents producing or checking one of these files

- Treat each "need to have" item as an assertion and each "nice to have" item as a warning.
- Label a day `actuals` only when telemetry produced the number. A model fit to the measured
  period is still `forecast`.
- Never write a moving average into `dau`. If the source produces only a smoothed series, say so
  rather than pass it off as daily.
- If the series ends before 31 December of the following year, deliver what the model produced
  and state the end date. Do not extrapolate unless asked.
