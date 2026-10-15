# `data-official/2027-glean-draft/` — 2027 desktop forecast on Glean-equivalent history

**Not a monthly cycle.** A separate PRE-DRAFT forecast of desktop **MAU** (headline: raw daily MAU on 2027-12-15) through 2027-12-31.
Since **2026-10-14 the headline build is the real-Glean one** (17 months of real Glean telemetry, model re-tuned for a short history,
Linux bot-like traffic measured and held flat; `LOCK.md`); **build 5** on the legacy-to-glean project's *Glean-equivalent* history
(locked 2026-10-12, MAU + DAU) is the **validation build**. Branch `glean-2027-forecast` (off `october-forecast`). Nothing here feeds
the KPI sheet, the BigQuery output table, or `main`.

Start with `LOCK.md` (what is locked and why), then `PROCEDURE.html` (every decision and build, the full record).
`REPORT.html` is the external copy; `handoff/` holds the per-metric packages for coworkers.

## Current state (2026-10-13)

| | |
|---|---|
| headline build | `comparison_glean_real_mau_additive_b_2026-10-14/` — real Glean 2025-05-22 → 2026-10-09, additive regime, cps 0.005 / 3 changepoints / range 0.8, outage mask off, Linux bot overlay `b` held flat at 4.2M, China correction `o`; MAU only |
| headline | **world MAU 2027-12-15 = 161,546,764** (2026-12-15 166,290,298, −2.9%); minus the Linux add-back 157.3M = the validation build's 157.2M |
| validation build | `desktop_glean_history_cpr087_o_2026-10-10/` (build 5) — g01 lock with changepoint range 0.87, Iran fill, 2025 outage calendar, China correction on DAU + MAU; world MAU 2027-12-15 157,222,571; DAU 28d MA 52,731,308 |
| uncertainty | backtest of the locked config at 8 seams (≥ 4 y history): MAU 431-day \|error\| p50 6.7% / p90 9.9%, forecasts ran low ~3%; translation band ±~1.3% |
| validation | same model on Legacy telemetry = this forecast ÷ the translation factor (MAU 1.030, DAU 1.040), Dec→Dec equal |
| comparisons | `COMPARISONS.html` (2026-10-13): Legacy = the same forecast ÷ the translation factor; real Glean with no simulated history = unusable at 17 months (world MAU 169M → 229M), already +15% at one month so a shorter horizon does not help; the run also masked Aug–Dec 2025 with the Legacy-outage calendar, which real Glean does not need |
| real Glean | re-tuned for 17 months (additive regime, cps 0.005, 3 changepoints, range 0.8, outage mask off) and adopted temporarily 2026-10-13: world MAU 2027-12-15 158,436,071, −3.0% Dec→Dec |
| mobile MAU companion | `mobile_mau_2026-10-10/` (2026-10-14, PROCEDURE §15): production mobile model (October lock) on real Glean mobile MAU from 2020-12-31, Iran rolling-28 MAU fill, no `p`; world MAU 2027-12-15 59,424,593 (+10.7% Dec→Dec) but the forecast opens **−7.2% below the last actual** — superseded as the approach: **since 2026-10-15 mobile is forecast as ORGANIC MAU** (marketing forecasts paid; `organic_mau/`, PROCEDURE §16). Round 1 of changepoint probes on the uncorrected organic series (`probes_mobile_mau/round1.html`); the India + Indonesia iOS-rate cap (provisional, `organic_mau/ios_rate_cap/IOS_RATE_CAP.md`); round 2 and round 3 on the capped series (`probes_mobile_mau/round2.html`, `round3.html`): a smooth seam-vs-slope frontier; prior 0.0065 / range 0.87 (seam −1.5%, Dec→Dec +7.3%, 51.5M) recommended — awaiting Brendan's pick |
| next | the duplicate-id-corrected real-Glean build as a SECONDARY comparison (headline stays uncorrected); `profile_group_id` does not isolate the term, route open (PROCEDURE §14); the mobile organic build choice after round 2, then the mobile curves/handoff re-exported as organic; the marketing DS team's read on the India/Indonesia attribution (decides whether the cap stays) — awaiting Brendan |

## What's here

| path | what |
|---|---|
| `LOCK.md`, `PROCEDURE.html`, `REPORT.html`, `REPORT_build5_glean_equivalent.html` | lock record (headline + validation rows); full record (draft 17); the headline's external PRE-DRAFT copy (`scripts/render_glean_real_report.py`); the validation build's (`scripts/render_glean_history_report.py`) |
| `COMPARISONS.html` | the two formal comparisons as one PRE-DRAFT page (`scripts/render_glean_history_comparisons.py`; plots `plots/comparisons/` from `scripts/plot_glean_history_comparisons.py`) |
| `handoff/` | the MAU package for coworkers on the headline build: `glean2027_desktop_mau_handoff/` + `.zip` (page, forecast CSVs, plots, README, `HANDOFF.md` for the recipient's AI agent; every name says desktop); `scripts/render_glean_history_handoff.py --metric mau [--build real_glean|glean_equivalent]`; the DAU package (build 5) was dropped 2026-10-14 |
| `handoff/glean2027_mobile_mau_handoff/` + `.zip`, `handoff/glean2027_mau_desktop_and_mobile.csv` | the mobile MAU package (`scripts/render_mobile_mau_handoff.py`; red callout while the seam is not closed) and the desktop + mobile world MAU curves side by side (not summed) |
| `raw_glean_mobile_countryxapp_2026-10-09/`, `iran_fill_mobile/`, `mobile_mau_2026-10-10/` | the mobile MAU companion: country × app frames from `scripts/sql/glean_mobile_mau_country_app.sql` (`build_comparison_raw_frames.py --layout mobile_app`; parquets + 17 MB source pull untracked, meta tracked), the Iran rolling-28 MAU fill (`generate_mobile_mau_iran_fill.py`, scale 1.0, seam diagnostics + plot), the first build (`run_mobile_mau_forecast.py`) with its `csv/`, `plots/`, `plots/canonical/` |
| `organic_mau/` | the ORGANIC side of the mobile draft (`_index.md`): the measured Fenix paid/organic MAU split (from 2024-10-01; 21.3% paid at the edge), the paid-vs-organic correlation check (`paid_organic_correlation.html`), and `ios_rate_cap/` — the **provisional** India + Indonesia correction (organic Fenix capped at its own iOS growth from 2026-04-01; 440,081 MAU removed on 2026-10-09; `IOS_RATE_CAP.md` = what, why, which builds, rollback) |
| `probes_mobile_mau/` | organic mobile MAU changepoint probes (`scripts/run_mobile_mau_probes.py`): round 1 `<slug>/` on the uncorrected organic series + `round1.html`; rounds 2–3 `round2_ioscap/`, `round3_ioscap/` on the capped series + `round2.html`, `round3.html` (with the seam-vs-slope scatter) (`scripts/render_mobile_mau_probes_page.py`); `scores_*.csv`, `plots/` |
| `inputs/legacy_to_glean_export/` | the delivered export, **tracked**: fc15 per-cell parquets (DAU, MAU) + D-118 continuation to 2026-10-09, roll-ups, cell summary, gap/seam CSVs, the export project's notes. Source: `~/work/experiments/legacy-to-glean/.claude/worktrees/export-rollups/exports/` @ 22e01e2 |
| `raw_glean_history_2026-09-08/`, `raw_glean_history_2026-10-09/` | adapter output (`mozaic_daily.glean_history`), regenerable; parquets untracked, `glean_history_inputs.meta.json` tracked |
| `raw_legacy_osxcountry_2026-10-09/`, `raw_glean_real_osxcountry_2026-10-09/` | comparison inputs (Legacy; real Glean from 2025-05-22), country × OS, from `scripts/sql/glean_history_comparison_*.sql` via `build_comparison_raw_frames.py` |
| `iran_fill/`, `iran_fill_legacy/` | regenerated Iran counterfactual for this layout (Glean-equivalent units; Legacy units for the comparison), scale 1.0 / 1.0 |
| `mozillaonline/` | the `o` curve as a **data correction**: spec `mozillaonline.glean_history.json` (flag `windows`, windows/mac split, DAU + derived MAU curve), `MAU_OFFSET_APPROACH.md`, measurements |
| `adjustments/` | combinatorics **candidates, not applied**: `d` duplicate-id inflation (MAU), `x` Linux Glean-only temporary (DAU + MAU); `candidate_only: true` |
| `desktop_glean_history_cpr087_o_2026-10-10/` | **build 5, LOCKED** |
| `desktop_glean_history_g01_*`, `build*_seam*/` | earlier builds and their exported outputs (build 1 seam 09-09; build 2 plain; build 3 o on DAU only; build 4 o on both, g01) |
| `probes_mau_cpr/`, `probes_cps/` | parameter probes (changepoint range; changepoint prior scale); `probes_mau_cpr/cpr*/` ran without the China correction (gate bug, fixed) — use `fixed_cpr*/` |
| `backtest/`, `backtest_round1/` | backtest of the locked config: round 2 (≥ 4 y seams, inputs cut at seam) is the stated one; round 1 kept for the record |
| `comparison_legacy_cpr087_2026-10-10/`, `comparison_glean_real_cpr087_2026-10-10/` | the same model on Legacy telemetry (validates the translation factor) and on real Glean from 2025-05-22 (first run, locked knobs: unusable, kept for the record) |
| `probes_glean_real_mau/`, `comparison_glean_real_mau_additive_2026-10-13/` | real-Glean MAU parameter probes (rounds 1–3, `scripts/run_glean_real_mau_probes.py`) and the round-3 centre without the Linux overlay (158,436,071; README) |
| `linux_bot/`, `comparison_glean_real_mau_additive_b_2026-10-14/` | the Linux bot-ISP measurement from real Glean client data (CSVs, `_index.md`), the `b` overlay curves + spec; **the headline build** with its `csv/` and `plots/canonical/` |
| `csv/`, `plots/`, `plots/canonical/`, `plots/comparisons/` | the locked build's curves, Dec-15 summary, build comparison, Legacy comparison table, real-Glean comparison by horizon, real-units restatement, Easter alignment; canonical-format plots (purple PRE-DRAFT); comparison plots |
| `intervals/` | build-1 DAU Prophet bands (kept; the Prophet band is not usable at this horizon) |
| `_internal_plots/`, `_shakedown_nofill_*`, `_superseded_float_inputs_*` | internal diagnostics and records; not for circulation |

## Rules for this directory

- Headline column is `glean_equivalent`; tiles are OS × country (`windows` / `mac` / `linux`), not the production
  Windows-version segments. Do not mix the two layouts in one artifact.
- Every forecast artifact follows the repo convention: `.raw.` / `.adj-*.` marker + sidecar `.meta.json`, loaded via
  `mozaic_daily.adjustments.load_forecast`.
- Inputs are never edited in place; a refreshed export goes in beside the old one with its source commit recorded.
- External outputs (REPORT, handoff, canonical plots) carry the purple **PRE-DRAFT** stamp and never show the raw model.
- Unlocking means a new build directory and a new row in `LOCK.md`.
