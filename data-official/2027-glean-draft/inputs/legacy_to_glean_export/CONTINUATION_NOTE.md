# Continuation of the fc15 series past 2026-09-08 (D-118, run 2026-10-12)

The four `*_fc15_continuation.*` files extend `glean_equivalent_{dau,mau}_history_cells_fc15.*` from 2026-09-09 through
**DATA_END = 2026-10-09** on the delivered definitions (decisions through D-117; D-116 column names; reference Glean
state Fx 148–153; the fc15 cut's 53 series and columns). They were built by `v4/steps_topup.py` and `mau/steps_topup.py`
from fresh reads of the source aggregates, not by re-running the history build, and they differ from the delivered
history in exactly these ways: the published Legacy rows of 2026-08-10..DATA_END were read straight from
`firefox_desktop.active_users_aggregates` (no scratch table) and translated with the delivered lookups
(`v4/results/lookup_v4_{factor_source}.csv`, reference state; `mau/results/lookup_mau_*.csv`, the FR / CN hybrids under
the cell's name), each cell keeping its delivered `factor_source` and `pool_a_fail`; `glean_only_steady` is each cell's
delivered constant share of the Legacy input (checked constant to 1e-16 over 2026); `glean_only_temporary` follows the
delivered rules on the windows of record only, with every window that was **open on 2026-09-08 extended to DATA_END**
(DAU: 88 Linux cells from `windows.csv` + `windows_members.csv`; MAU: 232 Linux cells through their pools' windows, plus
Windows × FR / CN on every day per D-114) and **no new window detected**; MAU `duplicate_id_inflation` = e(OS, d) ×
published with **e carried forward at its 2026-09-08 value** (Windows 0.709 %, Linux 0.253 %, Mac 0) because the 1 %
ping sample it is measured on (`l2g_stage0_pings_glean_1pct_v1`) ends 2026-09-08, so September / October could not be
re-measured without a new Stage 0 extract; every band column is carried forward as the cell's delivered last-day ratio
to its basis (DAU: the 28-day mean of the headline; MAU: the headline), so the percentage bands are unchanged and the
absolute bands scale with the series; roll-up rows are re-summed from all 724 translated cells (roll-up published Glean
summed over every country of the OS, as Step 8 did), and ROW rows come from `tooling/export_cells_fc15.row_rows`.
DATA_END is the latest `submission_date` present in both aggregates on 2026-10-12 (Legacy 2026-10-10, Glean 2026-10-11)
minus one day for landing. **Fx 155 was not re-measured**: the factors are the Fx 148–153 reference-state factors
applied to every row regardless of the version mix on the new days, so any Fx 155 behaviour change in Glean is not in
the translated series (it would show up as a gap against published Glean, see the verification's section d). On the
reproduction window 2026-08-10..2026-09-08 the method reproduces the delivered tables bit-for-bit (max relative gap
≤ 9e-14 on every component, both metrics).

## Files (sha1)

| file | rows | sha1 |
|---|---|---|
| `glean_equivalent_dau_history_cells_fc15_continuation.parquet` | 1,643 (53 series × 31 days) | `22125c0d9b3f5027ebf7a8d1aa8fa50254dce1c5` |
| `glean_equivalent_dau_history_cells_fc15_continuation.csv` | 1,643 | `cfe4fe1eecc626abcaca57db05b31bb90031b04d` |
| `glean_equivalent_mau_history_cells_fc15_continuation.parquet` | 1,643 | `eae5b71cec95f262cf3284edd465f834d4768460` |
| `glean_equivalent_mau_history_cells_fc15_continuation.csv` | 1,643 | `90c172763b371cabf62155991d33716a8c0156f4` |

All-cell frames (every cell and roll-up, 2026-08-10..DATA_END, the verification's input): `v4/results/topup_dau_allcells.parquet`,
`mau/results/topup_mau_allcells.parquet` (44,409 rows each). Verification: `python3 tooling/verify_continuation.py` (exit 1 on failure).

## BigQuery jobs and spend

| job | bytes | log |
|---|---|---|
| max submission_date of both aggregates and the ping sample (hand-run, `bq_query.py`) | 23.4 GB | `v4/RUN_LOG.md` |
| `topup_legacy_dau_2026-10-09` (published Legacy DAU rows, 2026-08-10..10-09) | 246.6 GB | `v4/RUN_LOG.md` |
| `topup_glean_dau_2026-10-09` (published Glean DAU by OS × country) | 46.2 GB | `v4/RUN_LOG.md` |
| `topup_legacy_mau_2026-10-09` (published Legacy MAU rows) | 219.7 GB | `mau/RUN_LOG.md` |
| `topup_glean_mau_2026-10-09` (published Glean MAU by OS × country) | 46.2 GB | `mau/RUN_LOG.md` |

Total **0.582 TB** of the run's 1.0 TB cap; no single pull above 0.25 TB (stop rule 0.5 TB never reached). Against the
budgets of record: DAU 12.30 → 12.616 TB of 13; MAU 13.518 → 13.784 TB of 15 (`mau/results/spend.json`). Nothing was
uploaded; the delivered tables and lookups were not touched.

## Decisions in force

D-116 / D-117 names; D-094, D-097, D-098 (DAU Glean-only columns and windows of record); D-110, D-111 (duplicate-id
inflation, Mac e = 0); D-113, D-114 (FR / CN); D-105, D-107 (factor sources as delivered); D-087 / D-094 seams unchanged;
D-118 (this top-up). Reference state Fx 148–153 (D-067) unchanged.

## What the forecast consumer must know

- The seam between 2026-09-08 and 2026-09-09 is in the data, not the method: the Legacy input steps +0.35 % (MAU, global)
  and published Glean +0.49 % on the same day; the DAU seam step is within every series' usual daily range.
- Over 2026-09-09..10-09 the reconstruction (headline + temporary + duplicate-id) sits below published Glean more than in
  August: DAU global −0.65 % (Aug −0.17 %), MAU global −0.98 % (Aug +0.03 %), driven by Linux (DAU −2.6 %, MAU −6.0 %, where
  the headline alone is −5.7 % / −22 %: the Linux Glean excursion widened and only cells with a recorded window carry the
  temporary term). Windows / Mac: DAU −0.55 % / −0.07 %, MAU −0.33 % / −0.53 %.
- Open windows are extended, not re-detected; e is frozen at 0.709 % (Windows) / 0.253 % (Linux); bands are carried, not
  re-estimated. Fx 155 is not in the factors. A re-run after the Fx 155 reference re-measurement (late Oct) supersedes this.
