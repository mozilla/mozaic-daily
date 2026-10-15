# organic_mau/ — the ORGANIC side of the 2027 mobile MAU draft

Brendan, 2026-10-15: "we only forecast the organic part of mobile. We allow the marketing team to forecast paid." Everything
here feeds or checks the organic Fenix MAU series the mobile MAU runner trains on (`scripts/run_mobile_mau_forecast.py --organic-split`).

| file / dir | what |
|---|---|
| `fenix_paid_organic_mau.2026-10-09.parquet` + `.meta.json` | the measured Fenix paid/organic **MAU** split by day × country (`scripts/sql/fenix_paid_organic_mau_split.sql` → `scripts/build_fenix_organic_mau_split.py --start 2024-10-01`; paid = release + Play Store + gclid, as `p`, counted on 28-day actives). From 2024-10-01 because `new_profile_clients` retains only clients first seen since 2024-09; the runner holds the first day's share flat backwards. World paid share 1.0% → 21.3% (9.0M of 42.2M) at the edge |
| `plots/fenix_paid_mau_share.png` | paid share over time, world + the five most-paid countries |
| `paid_organic_correlation.html` + `paid_organic_*.csv` + `plots/paid_organic_*.png`, `plots/paid_vs_excess_over_ios.png` | the sanity check (`scripts/analyze_paid_organic_correlation.py`): is organic growth correlated with paid? Sections A–C: nothing systematic in the demeaned monthly panel. Section D: India (+15 pt) and Indonesia (+7 pt) above their own Firefox iOS over the high spend period as paid share passed 30%; handed to the marketing DS team |
| `paid_organic_quarterly.html` + `.csv`, `plots/paid_vs_excess_over_ios_quarterly*.png` | section D by calendar quarter, Q4 2024 → Q3 2026 (`scripts/analyze_paid_organic_quarterly.py`), for the marketing DS conversation |
| `ios_rate_cap/` | the **provisional data correction** that follows from section D: IN + ID organic Fenix capped at their own iOS growth from 2026-04-01, excess removed with no add-back. `IOS_RATE_CAP.md` is the rollback note; read it before touching any mobile build |

Not here: the raw country × app pull (`../raw_glean_mobile_countryxapp_2026-10-09/`), the probe builds (`../probes_mobile_mau/`),
the DAU split `p` uses in production cycles (`data-official/{YYYY-MM}/organic/`). A new organic-side input (another split
vintage, another correction) gets its own file or subdirectory here with a meta and a line in this table.
