# paid_mau_2027/ — the marketing DS team's paid Fenix MAU, bridged from our data edge through 2027 (checkpoint, 2026-10-15)

Brendan, 2026-10-15: the mobile handoff must carry **organic + paid**; marketing forecasts paid. Their 2027 plan (ahe, STMO query
128941 → `mozdata.analysis.ahe_gmio_plan2027_mau_daily_20261014`; quick site `ahe-2027-planning-first-scenario`) is daily paid MAU
for 2027 = weekly paid DAU (UAC + Meta Android, world, **attributed** basis) × 4.792 × a day-of-week factor, two scenarios. Decisions:
scenario **`opt_dec15_mau`** ("Optimized for Dec-15 MAU", the optimal spend model); bridge 2026-10-10..2026-12-31 from the same
run's weekly "total" paid DAU by the same rule, as delivered, unscaled; per-country allocation by OUR measured split's trailing-28-day
paid share; the 90% paid band is carried.

| file | what |
|---|---|
| `paid_mau_2027.opt_dec15_mau.pull2026-10-15.parquet` / `.csv` | daily world paid Fenix MAU 2026-10-10..2027-12-31 with 90% band; `segment` = `bridge_2026` (83 days) or `plan_2027`; produced by `scripts/pull_paid_mau_2027.py` |
| `paid_mau_2027.opt_dec15_mau.pull2026-10-15.meta.json` | provenance (sha1 of every source), the construction rule, the numbers, the source's own caveats |
| `paid_country_shares.2026-10-09.csv` | each country's share of measured paid Fenix MAU over 2026-09-12..2026-10-09 (IN 32%, ROW 28%, ID 7.6%, US 7.5%, BR 5.7%; IR 0) |
| `plots/paid_mau_2027.opt_dec15_mau.png` | measured → bridge → plan, with the band |
| `source_data/` | verbatim: the three `bq_query.py` JSON results + their SQL (`scripts/sql/paid_mau_*.sql`), the STMO query JSON (`query_128941.json`), and `quick_site/` (the dashboard HTML copied from `gs://moz-fx-quick-prod-sites/ahe-2027-planning-first-scenario/`, plus its embedded `DATA.json`) |

Numbers: last measured paid MAU 9,005,217 (2026-10-09) → bridge opens 8,984,550 (−0.23%) → 2026-12-15 9,698,450 → 2026-12-31
9,905,808 → 2027-01-01 9,949,400 (+0.44%) → **2027-12-15 14,341,804** [13.81M, 14.94M]; the flat-budget scenario would be 13,327,527.

Read before quoting (the source's own caveats): hypothetical plan with no seasonality and no cross-channel interaction (an upper
bound); MAU is a global multiplier (4.792 × DAU), not a model; retention extrapolated past the fitted range by late 2027; interval
calibration unverified. The weekly-step × day-factor construction gives the daily series a small sawtooth.

Not here: the organic forecast (`../mobile_organic_mau_2026-10-10/`) and the composition of the two (next step). A re-pull is a new
pull-date suffix beside these, never an overwrite; a different scenario is a different file name.
