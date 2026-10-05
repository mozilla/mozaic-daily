# `data-official/2026-09/` — September 2026 forecast cycle

Cycle branch `september-forecast`, off `clean-slate` @ `a59d04f` (which carries every August tooling
change). Opened 2026-09-04 by the button-down skill; **closed 2026-10-05 by the same skill.**

## Status: CLOSED — buttoned down 2026-10-05 · published 2026-09-15 (c-suite update) · seam 2026-09-09

October 2026 is the live cycle from 2026-10-05 (`october-forecast`). Nothing here changes any more; the
numbers below are the ones that were delivered. The long section after the "Present vs Archived" block is
the cycle log as it was written while September was live (newest entry first) and is kept verbatim.

### Current usable working set

| what | path / value |
|---|---|
| Canonical desktop build | `desktop_g01_2026-09-09/cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/mozaic_daily_forecast.2026-09-09.ld-D.adj-ijlo.parquet` + `.meta.json` + `parameters.json`. Config **g01**, overlays `i j l o` baked in, `h` applied by the notebook (display layer). Trained through 2026-09-08. |
| Canonical mobile build | `mobile_cpr0725_paid0915med_2026-09-09/cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-09-09.gm-D.adj-p.parquet` + sidecar + `parameters.json`. **cpr 0.725**, `p` on the delivered workbook's **Med** paid scenario; `t` and `u` applied by the notebook. |
| Published curves | `csv/september_canonical_curves.csv` (world 28d MA, both platforms + ALL, with the August prior columns), `csv/september_dec15_summary.csv`, `csv/september_canonical_curves.DESKTOP_ONLY.DAILY.csv` (desktop daily twin, `exact` post-anchor rule), `csv/september_desktop_waterfall_steps.csv`. Inventory and the daily file's ledger: `csv/README.md`. |
| Producer notebook | `september_canonical_v2026-09-04.ipynb` — last executed 2026-09-15 after the c-suite update; plots in `plots/`. |
| **Dec-15 28d-MA, published** | desktop **49,332,443** · mobile **18,214,594** · ALL **67,547,037** — vs August 48,703,443 / 17,924,562 / 66,628,005 = **+629,000 / +290,032 / +919,032**. ALL is +511,085 vs the Baseline target and +32,034 vs Stretch. |
| Display-layer specs (`adjustments/`, live by presence, all ramps 2026-09-09 → 2026-12-15) | `headwind.json` **`h`** desktop **−1,017,277**, `clamp_at_anchor` (flat after Dec-15) · `tailwind.json` **`t`** mobile **+299,000** · `tou_mobile_headwind.json` **`u`** mobile **−27,162**. Rationale dirs: `headwinds/`, `tailwind/`, `tou_mobile_headwind/`. |
| Per-tile desktop overlays (all gated `applies_to_forecast_start = 2026-09-09`) | **`l`** `launch_at_login_new_users/launch_at_login_new_users.json` → `lol_tailwind.2026-07-29.cap200k.parquet` (200K ceiling, carried from August) · **`o`** `mozillaonline/mozillaonline.json` → `mozillaonline_migration.2026-08-31.parquet` (Dec-15 28d-MA 668,839) · **`j`** `japan_bot/japan_bot.json` → `japan_bot.2026-09-07.parquet` (PEAK plateau 43,813) · **`i`** `india_excess/india_excess.json` → `india_excess.2026-09-06.parquet` (PROPORTIONAL, 50,994 at Dec-15) · **`e`** `launch_at_login_existing_users/launch_at_login_existing_users.json` → `launch_at_login_existing_users.2026-09-08.parquet`, **WITHHELD** (`"withheld": true`, never applied this cycle). |
| Mobile paid split **`p`** | `organic/organic.json` → measured split `organic/fenix_paid_organic.2026-09-09.parquet`; paid level `marketing/marketing_lift_model.gmio_uac_meta_total_med.2026-09-09.pull2026-09-15.parquet` (workbook Med scenario, Dec-15 daily level 1,833,753; the Low, ci90lo, 09-09 and 09-04 pulls are siblings in `marketing/`). |
| Raw BigQuery pulls | `desktop_rawpull_2026-09-09/`, `mobile_rawpull_2026-09-09/` (the canonical builds read these); `*_rawpull_2026-09-02/` feed the 09-02-seam revert builds and the stale ladder runs. |
| Adjustment-effects record | `adjustment_combinatorics/adjustment_effects.csv` + `adjustment_subsets.csv`, `adjustment_curves_28ma.csv`, `adjustment_dec15_by_country.csv`, `adjustment_effects_vs_2026-08.csv`, `adjustment_effects.meta.json`. Re-checked **current** against both canonical sidecars and re-exported on 2026-10-05 (byte-identical CSVs). The October record is compared with `scripts/compare_adjustment_effects.py --prior 2026-09 --current 2026-10`. |
| KPI workbook row set | `kpi_sheet/official_forecast_data.2026-09-17.csv` (September as `CURRENT`, uploaded by hand). |
| Prediction intervals (raw model, **not canonical**) | `desktop_raw_ci_2026-09-09/`, `mobile_raw_ci_2026-09-09/`, `september_raw_intervals.ipynb`. |

**Revert targets, now closed.** Desktop `desktop_g01_2026-09-02/` (old seam); mobile
`mobile_cpr0725_paid0910low_2026-09-09/` (Low paid) with `mobile_cpr0725_paid0910ci90lo_2026-09-09/`,
`mobile_cpr0725_paid0910ci90lo_2026-09-02/`, `mobile_cpr0725_paid0909_2026-09-02/`,
`mobile_cpr0725_2026-09-02/` behind it; overlays `japan_bot_REVERT_2026-09-0{4,9}/`,
`india_excess_REVERT_2026-09-0{4,9}/`. Their revert window closed when October became the live cycle:
parquets, sidecars and REVERT docs stay on disk so a revert stays legible, only the pickles would need
pulling back from GCS.

### Present vs Archived (button-down 2026-10-05)

Archive prefix: `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/` (`README.md`
at the prefix root; the full directory under `data-official/2026-09/`, plus
`research/forecast-vs-summer-actuals/data/pkl/` for the one research pickle that was not in any prefix).
The whole tree also remains in the `september-forecast` branch. A full snapshot of this directory was
uploaded mid-cycle on 2026-09-17 (476 local / 479 remote, 43 pickles, see "Archived mid-cycle" in the log
below); the button-down topped it up with `gcloud storage rsync` and re-verified. Verified counts are
recorded in the table at the end of this section once Phase 2 of the button-down has run.

- **Present (on disk through the 3-month retention window, i.e. until the December 2026 roll-forward):**
  every forecast and raw-pull `.parquet` of every build above — canonical, revert targets, the two
  `*_raw_ci_*` interval builds and all 35 `adjustment_ladder/<codes>.<key>/` runs — with their
  `.meta.json` / `parameters.json` / `run.log`; `csv/`, `plots/`, `kpi_sheet/` (incl. CSVs and
  `source_data/`), every spec directory with its curves and `source_data/`, the `*_REVERT_*` directories'
  specs + parquets + `REVERT.md`, the notebooks, and `adjustment_combinatorics/` (tracked CSVs, manifest,
  `index.html`).
- **Archived to GCS and removed from disk (Phase 4):** all **43** `mozaic_objects.*.pkl` (≈24 GB — the
  canonical desktop/mobile fits, every revert-target build, the two interval builds and the 33 ladder
  runs that were really forecast; the two `--reuse-run` ladder rungs never had one) and `.DS_Store` files.
  Nothing else leaves: this cycle produced no handoff zip, no `_backup_*` snapshot and no staging dir.

| directory | local objects (files + symlinks) | remote objects | pickles | verified |
|---|--:|--:|--:|---|
| `data-official/2026-09/` | _filled at Phase 2_ | | 43 | |
| `research/forecast-vs-summer-actuals/data/pkl/` | 1 | | 1 | |

---

## Cycle log — written while September was live (newest first; originally headed "DRAFT BUILDS at the 2026-09-02 seam — not locked · `h` re-anchored to −1,089,347 on 2026-09-08")

**2026-09-08:** the Win10 headwind `h` anchor moved −726,000 → **−1,089,347** (the `h_for_plus479k` counterfactual from
`adjustment_combinatorics/counterfactuals.csv`: all four overlays kept, all-in desktop Dec-15 = August +479,000 exactly). A
calibration choice between Brad's model value and August's −1,315,000, not a measurement. Display layer, so no rerun: desktop
Dec-15 28d-MA is now **49,182,443** (+479,000 vs August, −330,714 vs Baseline), ALL **67,440,453**; mobile unchanged. Canonical
notebook, CSVs, plots and the waterfalls were rerun the same day. The numbers in the paragraph below are the 2026-09-04
values at −726,000. **Stale at the old anchor:** `adjustment_combinatorics/` (manifest `display_effects_dec15.h` and
`index.html`) — its `h_for_plus479k` row IS the adopted number, so re-render only if the report is to be circulated.


**SEAM REFRESH 2026-09-10 — seam moved 2026-09-02 → 2026-09-09 (training through 2026-09-08, every landed day).** Fresh raw
pulls (`desktop_rawpull_2026-09-09/`, `mobile_rawpull_2026-09-09/`, pre-flight PASS for all tables), the `p` split rebuilt for
the new window (`organic/fenix_paid_organic.2026-09-09.parquet`, 294 GB scan, four checks PASS), every September spec re-gated
(`applies_to_forecast_start` on `i j l o p`, ramp `start_date` on `h t u`; curves and anchors unchanged), and both platforms
rebuilt with the same locked configs → `desktop_g01_2026-09-09/` (`.adj-ijlo.`) and `mobile_cpr0725_paid0910ci90lo_2026-09-09/`
(`.adj-p.`, lower-bound paid curve). The 2026-09-02-seam builds stay on disk as revert targets. Raw-model Dec-15 28d-MA moved
desktop **+77,930** (50,271,790 → 50,349,720) and mobile **+11,013** (17,914,827 → 17,925,840); the seven new desktop training
days ran ~890K/day above what the 09-02-seam forecast had for them (mobile ~76K/day). Paid seam step at 2026-09-08: +66,911
(+4.15% of paid). Published Dec-15 28d-MA at the *old* −1,089,347 anchor, notebook re-executed 2026-09-10: desktop 49,260,373
(+556,930 vs August), mobile **18,197,678** (+273,116), ALL 67,458,052 (+830,046; −78,905 vs the 2025 Dec-15 flat line).
**`h` re-anchored the same day (Brendan's decision 2026-09-10): −1,089,347 → −1,167,277**, i.e. the +77,930 the new seam added at
Dec-15 is taken back out, so published desktop stays **49,182,443** (= August +479,000, the value the old anchor was calibrated
to) and ALL is **67,380,122** (−156,835 vs the 2025 flat line). Same calibration logic as 2026-09-08, re-applied at the new seam;
display layer, exact, no rerun. Still a DRAFT. ~~Still open: `adjustment_ladder/` and `adjustment_combinatorics/` are keyed to the 2026-09-02 seam and are stale~~ — **both rebuilt 2026-09-17 at the 2026-09-09 seam** (see the entry below); the notebook's `[plot-desktop-ladder]` can assert again.

**Raw-model prediction intervals (2026-09-10)** — `desktop_raw_ci_2026-09-09/` and `mobile_raw_ci_2026-09-09/`: the live
configs re-run at the 2026-09-09 seam on the same raw pulls with **every adjustment off** (desktop `i j l o` disabled, `h` not
applied; mobile `p` off, so a total-DAU fit, `t`/`u` not applied), then Prophet predictive bands as quantiles of the 28d
trailing mean across the 1,000 stored paths (`scripts/compute_forecast_intervals.py`, ported from August). Not canonical.
Dec-15 28d-MA: desktop raw **50,326,587**, 90% 48,477,503 – 52,425,191 (±1,973,844; August's was ±4,811,094 at a 38-day
longer horizon); mobile raw **17,706,153**, 90% 17,486,488 – 17,966,102 (±239,807, 1.35% of level — the mobile model's own
narrowness, not calibrated). Review notebook `september_raw_intervals.ipynb` (both platforms; ends with the raw-point-forecast / bounds / full-width table), chart
`plots/raw_intervals_28ma_bands.png`. Copies in `research/forecast-intervals/september-2026-{desktop,mobile}/`.

**Adjustment-effects record + ladder/combinatorics rebuilt, 2026-09-17** — with Brendan's approval, 14 desktop
subsets were forecast at the 2026-09-09 seam (g01, ~2 min each; `raw` adopted from `desktop_raw_ci_2026-09-09/`, the
all-in `i+j+l+o` from the canonical build, both sidecar-verified) and the mobile `p` on/off pair recorded from the
canonical Med build and `mobile_raw_ci_2026-09-09/`. The all-in rows reproduce the published Dec-15 exactly (desktop
49,332,443 with `h`; mobile 18,214,594 with `t`+`u`). New **tracked** files in `adjustment_combinatorics/`:
`adjustment_subsets.csv`, `adjustment_effects.csv`, `adjustment_curves_28ma.csv`, `adjustment_dec15_by_country.csv`,
`adjustment_effects.meta.json`, `adjustment_effects_vs_2026-08.csv` (`scripts/export_adjustment_effects.py`,
`compare_adjustment_effects.py`). Shapley Dec-15 attribution: `l` +61,009 (of a 200,000 curve), `j` +15,547 (of
43,813), `i` −8,585 (of 50,994), `o` −44,837 (of 668,839), `p` +236,603 (against a 1,804,995 paid level; the `p`-off
run is a total-DAU fit, so this is marketing's paid minus the paid the model implies). Four overlays whose curves sum to
963,646 net **+23,134** on the raw model at Dec-15. Ladder at the new seam: raw 50,326,587 → `h` −1,017,277 → `l`
+96,645 → `i` −25,079 → `j` +44,797 → `o` −93,229 = 49,332,443. `index.html` re-rendered. Versus August (Shapley): `o`
−63,680 (curve +3,363, pass-through −67,043), `l` −3,231, `p` +161,446 (paid level +12,090, pass-through +149,357).
August's own record was built the same day, retroactively (`../2026-08/adjustment_combinatorics/`, canonical
reproduced exactly by today's code). The 2026-09-02-seam runs remain in `adjustment_ladder/` as exhaust (~10 GB).
Detail: `adjustment_combinatorics/_index.md`.

**Archived mid-cycle, 2026-09-17 — do not re-upload at button-down.** At Brendan's request the non-destructive half of
the archive job ran the same day: all of `data-official/2026-09/` as it stood at commit `92b3f55` (25 GB, 476 files
incl. symlinks, 43 pickles) is in
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/`, uploaded with
`gcloud storage rsync -r --no-ignore-symlinks` and verified: **476 local / 479 remote** (the 3 extras are
`japan_bot/alternates/*.2026-08-30.csv` from the pre-cycle handoff upload, since removed from disk), **sorted pickle-size
list identical** (md5 `5309e860…`), remote 26.72 GB. A **PROVISIONAL** `september-2026/README.md` says it is a snapshot;
Phase 2 of the button-down replaces it. Nothing was deleted or pruned. Anything produced after 2026-09-17 (new builds,
notebook re-executions, spec edits) is *not* archived yet — the button-down tops the prefix up with `rsync`
(idempotent), which is why the skill's Phase 2 now checks for this note before copying. Operational note: the first
`rsync` pass lost authentication after ~1 h (539 "Anonymous caller" 401s on parallel composite upload components; small
files landed, large ones did not); the retry with `CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=False` completed
with zero errors in 56 min. August's four new directories went to `august-2026/` the same day (see
`../2026-08/_index.md` § "Archived mid-cycle").

**Desktop DAILY (unsmoothed) export, 2026-09-17** — `csv/september_canonical_curves.DESKTOP_ONLY.DAILY.csv`, the
September counterpart of August's daily file, by `scripts/export_desktop_daily_csv.py` repointed to this cycle.
Dec-15 daily reads **55,802,968** (a Tuesday) against the published 28d-MA 49,332,443; the August column reproduces
August's export (55,077,204). New this cycle: `h` is **clamped flat after Dec-15**, so no smooth daily headwind
re-smooths to the published curve on both sides of the kink; Brendan chose the `exact` rule (rolling(28) of the file
IS the published curve through Dec-31; the daily headwind sawtooths after Dec-15, −1,158,857 → −875,697 on Dec-16).
Ledger, the two rejected rules and their numbers in `csv/README.md`; plot `plots/desktop_daily_export_vs_published_ma.png`.

Canonical builds made 2026-09-04 with August's locked configs (desktop g01 → `desktop_g01_2026-09-02/`, `.adj-ijlo.`;
mobile cpr 0.725 → `mobile_cpr0725_2026-09-02/`, `.adj-p.`; **mobile rerun 2026-09-09** with the re-pulled GMIO paid curve → `mobile_cpr0725_paid0909_2026-09-02/`, same config and raw pull, Dec-15 raw-model 28d-MA −7,072 vs the 09-04 build, training rows identical; **mobile rerun again 2026-09-10** with the marketing team's **lower-bound** paid curve (p5 of the 90% CI on forecast weeks, `_ci90_20260909` feed, pulled as variant `ci90lo`) → `mobile_cpr0725_paid0910ci90lo_2026-09-02/`, same config and raw pull, Dec-15 raw-model 28d-MA −64,273 vs the 09-09 build (daily −57,014 = the paid-curve delta exactly), training rows identical, paid seam step +71,978 (+4.52% of paid; was +103,544); the 09-09 build is the revert target, the 09-04 build stays beside it). Dec-15 28d-MA, display layer applied (`h` −1,089,347 draft, adopted 2026-09-08;
`t` +299,000, `u` −27,162), notebook re-executed 2026-09-10: desktop **49,182,443** (+479,000 vs August, by construction of `h`), mobile **18,186,665** (+262,103; was 18,250,938 on the point-estimate paid curve and 18,258,010 before the 09-09 re-pull), ALL
**67,369,108** (+741,103). Desktop sits −330,714 below the Baseline target; ALL sits +333,156 above it. Review notebook
`september_canonical_v2026-09-04.ipynb`; CSVs in `csv/`; plots (DRAFT-watermarked) in `plots/`. (The 18,186,665 mobile figure
above was the ci90lo build's; the Low rebuild later on 2026-09-10 published **18,194,860** / ALL **67,377,303**.)

**C-suite update 2026-09-15 (two changes, both planning decisions, not measurements):** (1) **desktop `h` eased −1,167,277 → −1,017,277**
(+150,000, display layer, exact, no rerun); (2) **`p` paid level switched from the workbook's Low to its `Med Forecast` scenario**
(variant `med`, the sheet's 90% CI lower end — leadership's "marketing midpoint"; Dec-15 daily 1,833,753, +19,144 vs Low), mobile rerun
→ `mobile_cpr0725_paid0915med_2026-09-09/` (same config and raw pull; training rows identical; raw-model Dec-15 28d-MA +19,734
exactly; paid seam step +67,389 = +4.18% of paid, up from +45,773 because Med sits 23,532 above Low at the seam). Notebook and
`research/executive-plot/` re-executed: **desktop 49,332,443** (+629,000 vs August), **mobile 18,214,594** (+290,032), **ALL
67,547,037** (+919,032; +511,085 vs the Baseline target, +32,034 vs Stretch). Leadership's stated expectation was 49,332,443 /
18,214,884 / 67,547,327 — the 290 DAU gap on mobile is the workbook's `Dec15 DAU` footer convention (Med − Low there is
20,024) versus the interpolated Monday series (+19,734 on the 28d MA). The Low build is the mobile revert target. **Re-executed 2026-09-15** (earlier that day) after the 2026-09-14 `active_users_aggregates` fix: the fix is
**mobile-only** (`glean_telemetry` view; desktop identical on every day 2023–2026), so only the `[bq-actuals]` mobile
line moved — `mobile_actuals`/`all_actuals` in `csv/september_canonical_curves.csv` shift by ≤8,750 DAU (28d MA) over
2026-01-01..2026-07-03 and gain four landed days; every forecast column and every Dec-15 figure is byte-identical. The
**mobile training rows in every September build are pre-fix**: the corrected table is +0.43% above them across 2025
(+2.2–2.4% in 2023–24, tapering to zero by 2026-06-07), so the notebook's 2025 mobile reference line (read from training
rows) and the waterfall notebook's 2025 tiles still carry the old series. Only a mobile re-pull and rebuild would move them.

**Desktop weekly seasonality** (`september_desktop_weekly_seasonality.ipynb`, added 2026-09-11): the g01 2026-09-09 model's
recent weekly component assembled to world DAU from the 48 tiles' fitted Prophet frames, day-of-week means over the 16 whole
weeks 2026-09-09..2026-12-29, plot `plots/desktop_weekly_seasonality_2026-09-09.png`. Zero-mean in log space exactly (asserted);
in DAU the model's own decomposition (with-weekly minus without-weekly) averages **+1,082,412 (+2.19% of level) — Jensen, not a
bug**: exp of a zero-mean term has a positive mean, so the trend line sits ~2.2% under the weekly mean of the DAU it produces.
The deviation-from-centered-7-day-mean version is zero-mean and matches the last eight weeks of actuals to within ~0.6pp per
day (Sun −29.6% model vs −29.1% actual).

**Mobile weekly seasonality** (`september_mobile_weekly_seasonality.ipynb`, added 2026-09-11, twin of the desktop notebook):
the cpr-0.725 2026-09-09 model's recent weekly component to world DAU from the 64 tiles (63 multiplicative, ROW Fenix additive;
raw-DAU fits, so no Jensen gap — the model decomposition averages +2 DAU), plot `plots/mobile_weekly_seasonality_2026-09-09.png`;
model side is **organic** mobile (15.9M level, paid level not included). Actuals-only twin over the 8 weeks to 2026-09-05 in
`plots/mobile_weekly_seasonality_actuals_2026-09-05.png`: Sun −2.8% actual vs −2.9% model, Tue +1.5% vs +1.6%. Desktop's
actuals-only plot is `plots/desktop_weekly_seasonality_actuals_2026-09-05.png`.

**Mobile paid/organic split plots** (same notebook, cells `[plot-mobile-organic-paid]`, `[plot-mobile-organic-only]`,
`[plot-mobile-paid-only]`): `plots/mobile_organic_paid_decomposition.png` stacks organic + paid + display layer to the
published total; `plots/mobile_organic_only.png` is total − paid − display layer (mozaic's organic forecast alone);
`plots/mobile_paid_only.png` (added 2026-09-11) is the complement — measured paid before the seam, marketing's Low
scenario after, with the +299,000 mobile tailwind `t` drawn **explicitly** as a second dashed line (paid + t) and `u`
left out, for both September and the August prior. Dec-15 28d-MA: Sep paid 1,785,261 (+t 2,084,261), Aug delivered
paid 1,554,879 (+t 1,853,879); paid delta +230,382.

**Desktop adjustment ladder** (`adjustment_ladder/ladder_manifest.json`, 7 cached isolation runs, built 2026-09-04): raw model
50,209,027 → +h −726,000 → +o −100,781 → +j +144,187 → +l −20,577 → +i +39,934 = 49,545,790. Single-overlay effects vs
raw were o −100,781, j +71,355, l +64,265, i −48,263: for `o` and `i` the training-row subtraction lowers Prophet's trend by
more than the curve adds back, so wiring them LOWERS Dec-15 despite positive curves; cumulative steps differ from single
effects because overlays interact through the fit. Rebuilding the ladder needs explicit approval (the script prompts).

**Desktop adjustment combinatorics** (`adjustment_combinatorics/`, built 2026-09-08): every subset of the four droppable
overlays `i`/`j`/`l`/`o` forecast with `h` always applied — 16 real desktop runs sharing the ladder cache — scored at Dec-15
against August's 48,703,443, the all-in September build and the targets, with the canonical desktop chart per combination in a
self-contained `index.html`. Built because not every adjustment can ship this cycle; the numbers are inputs to that choice, and no
combination has been selected. See its `_index.md`.

**Desktop Dec-15 waterfalls** (`september_desktop_waterfalls.ipynb`, built 2026-09-04): 2025 actual 51,846,238 → 2026 published
49,545,790 (−2,300,447) decomposed three ways from the canonical parquet's tiles, bars by absolute magnitude, closing exactly.
By OS: modern Windows −1,611,048 (holds the whole `h` −726,000), older Windows −1,125,336, Mac + Linux + other +435,937 (the
forecast has no Mac/Linux split). By market: ROW −988,740, CN +920,370 (MozillaOnline), US −485,799, DE −432,890 … IR +6,678;
`h` allocated to countries by 2026 modern-Windows share, no separate `h` bar. By market group (member-labelled, deliberately
NOT called regions because ROW is a third of DAU): ROW −988,740, DE + FR + IT −865,165, CN + JP + ID +710,295, US + CA −528,427,
PL + RU −364,306, BR + MX + AR −167,374, IN −103,410, IR +6,678. Plots `plots/desktop_waterfall_{os,country,country_groups}.png`
(DRAFT-watermarked), table `csv/september_desktop_waterfall_steps.csv`. **Added 2026-09-08:** the same three decompositions
for August delivered 48,703,443 → September 49,545,790 (+842,347), files `*_aug_vs_sep.png`, each build's `h` placed by its
own modern-Windows shares (so the +589,000 re-anchor is spread across every country bar, not shown separately). By OS: modern
Windows +772,842, Mac + Linux + other +51,516, older Windows +17,989. By market: ROW +230,959, US +98,193, JP +82,588 (`j`),
CN +73,794 (`o` refresh), FR +72,510, IN +58,399 (`i`) … AR +4,271; every market is up.

## Previous status: EMPTY CYCLE — `../2026-08/` remains authoritative until this branch produces output

Published August numbers (Dec-15 28d-MA, `h` + `t` applied): desktop **48,703,443** · mobile
**17,924,562** · ALL **66,628,005**, at the 2026-08-02 seam. Those are the N-1 comparison series for
September; they come from `../2026-08/csv/august_canonical_curves.csv`.

## KPI workbook updated 2026-09-17 (`kpi_sheet/`)

September promoted to `CURRENT` in the "Official Forecast Data" tab via the new `/update-kpi-sheet` skill
(`scripts/build_kpi_sheet_update.py`, first run). Label mapping on existing rows: the hand-mislabelled July block
`AUG *` → `JUL *`; August's `CURRENT *` → `AUG *`. New lines from `csv/september_canonical_curves.csv`, publish date
2026-09-17: `CURRENT forecast` 2026-09-09 → 12-31 (Dec-15 desktop 49,332,443 / mobile 18,214,594, locked) and
`CURRENT prior forecasts` Jan 1 → Sep 8 with 2026-08-01 blanked as the new handoff. Output
`kpi_sheet/official_forecast_data.2026-09-17.csv` (7,120 rows), upload by hand pending. Details in `kpi_sheet/_index.md`.

## What is already here (pre-work, produced before August was locked)

Two **desktop overlays**, both `desktop_overlay`-style components on `legacy_desktop`, both produced by a
different agent in `product-data-science-core/scratch/brwells/regional-story/`, and **both wired on 2026-09-04**
through the registry (rerun pending). Each directory has a `HANDOFF.md` — read it first — an
`_index.md`, a spec JSON, and scenario curves as `.parquet` (what the pipeline would load; tracked) with
`.csv` twins (read-only, gitignored) and `.meta.json` sidecars.

| dir | code | what | spec points at | data edge |
|---|---|---|---|---|
| `japan_bot/` | `j` | Japan's non-organic automated desktop traffic since late June 2026, to subtract before training and add back after. A masking effect, not growth. **Wired 2026-09-04** via `/ingest-adjustment` (registered, spec rebuilt, MIDDLE kept, LOW/HIGH archived); **re-exported 2026-09-09** on the PEAK plateau (43,813, the highest measured day) at the 2026-09-07 edge, MIDDLE build in `japan_bot_REVERT_2026-09-09/`; **rerun pending** | PEAK | 2026-09-07 |
| `launch_at_login_existing_users/` | `e` | Launch at Login for *existing* users desktop DAU tailwind (the new-users half is `l`): measured 2026-07-30..2026-09-08, producer forecast with **planned** rollout steps 2026-10-13/10-15 to ~736K/day, Dec-15 28d-MA 736,935. Fixed shares = en-locale Win10/11 DAU by country, IR excluded. **Ingested 2026-09-10 via `/ingest-adjustment`, WITHHELD** (`"withheld": true` in the spec; not applied, no marker) — delete the key + rerun to turn on | delivered curve | 2026-09-08 |
| `india_excess/` | `i` | India desktop DAU running above the 2022–25 typical curve since late May 2026, carried forward as real. **Already net of `l`** — do not net again. **Wired 2026-09-04** (registered; spec switched SETTLE → PROPORTIONAL and rebuilt via `/ingest-adjustment`; four alternates kept on disk); **rerun pending** | PROPORTIONAL | 2026-08-29 |

Wiring either is now registry-only (since 2026-09-04, `src/mozaic_daily/overlays.py`): add the code to
`../adjustment_codes.yaml` with `applier: per_tile_overlay` and a `spec_glob`, then a model re-run — a
spec-only change moves nothing because the curve is subtracted from training rows. No `main.py` edits.
Change one overlay per run so the Dec-15 delta stays interpretable. The `/ingest-adjustment` skill does the
registration and bookkeeping.

- **`STALE_REFERENCES_from_august_button_down.md`** — every script/notebook that still hardcodes an
  August path whose blobs were archived, plus the six cycle-scoped scripts whose constants
  (`FORECAST_START`, `DEFAULT_HEADWIND`, `TARGET_DEC15`, CSV dirs, raw-cache dirs) still say August or
  July. **Repoint before running any of them.**

## Inherited from August (`../2026-08/_index.md` § Next up)

- ~~Re-measure and swap the **`o` MozillaOnline curve**~~ **Done 2026-09-04**: rebuilt in `mozillaonline/` from the 2026-09-02 official export (Dec-15 28d-MA 668,839 vs the stale 567,549). Rerun pending.
- ~~The **Win10 headwind `h` anchor** sits at −1,315,000~~ **Replaced 2026-09-04** by the Dec-15 value of Brad's model curve: −726,000, applied as a linear ramp from the September seam, flat after Dec-15 (`headwinds/`). A DRAFT; the producer may revise.
- Decide how the **summer trough is scored** now that it fell inside the `display_ma` splice zone.
- Re-check the **data-refresh sign** (−64,769 then +100,840 on consecutive refreshes).
- **Summer-trough overlay** go/no-go (`research/param-scans/aug22-retune/`, target shape
  `research/summer-slump/`). Not implemented; nothing tuned toward the trough.
- Mobile: the **`t` tailwind** (+299,000, the August calibration tailwind — not terms-of-use, which is `u`) **carried forward unchanged 2026-09-04** (`tailwind/`); revisit now that the rebuilt `p` has been run (paid level +266,691 at Dec-15 vs August on the lower-bound curve), since it may cover part of what `t` was sized for. The paid curve `p` reads was re-pulled 2026-09-09 (point estimate), switched to the lower-bound twin 2026-09-10, then to the marketing team's delivered workbook's Low scenario later on 2026-09-10 (`marketing/`; rerun done, published mobile 18,194,860).
- Start `TODO_factors.md` as a diff against `../2026-07/TODO_factors.md`.
- **`../2026-06/` is retained under the 3-month rule until the October roll-forward.** `_archive/` and
  `research/ma-seam-turbulence/` import its frozen `export_canonical_curves.py`, and July code reads its
  marketing parquet and `june_delivered_mo_tailwind.json`. Before June leaves the window, the current
  cycle must own copies of whatever it still needs.

## Attribution ledger (desktop, Dec-15 28d-MA) — pending rerun

Starts from August's delivered desktop figure. Each row differences two builds that differ in exactly one
input; **no realised step exists until the September build runs**. Expected add-backs are the curve's own
Dec-15 28d-MA and are what the rerun is checked against (pass-through in a 0.5–1.5× band via
`scripts/verify_overlay.py`).

| step | expected | realised | running |
|---|--:|--:|--:|
| August delivered (`h` + `l` + `o`) | | | 48,703,443 |
| September data refresh + re-gated `l`/`o`/`h` | — | pending rerun | |
| `l` launch at login (new users) carried forward unchanged (200K ceiling) | 0 vs August | pending rerun (config-dependent) | |
| `j` japan_bot PEAK add-back | +43,813 | pending rerun | re-exported 2026-09-09, edge 2026-09-07 (was MIDDLE +67,094 at edge 2026-08-30) |
| `i` india_excess PROPORTIONAL add-back | +50,994 | pending rerun | re-exported 2026-09-09, edge 2026-09-06 (was +41,945 at edge 2026-08-29) |
| `o` MozillaOnline curve refreshed (Dec-15 28d-MA 567,549 → 668,839) | curve +101,290; realised effect empirical | pending rerun | |
| `e` launch at login (existing users) add-back — **WITHHELD, not in any build** | +736,935 if turned on | not applied | ingested 2026-09-10; flip `withheld` in the spec + rerun to include |
| `h` Win10 headwind: August ramp −1,315,000 → Brad's Dec-15 value −726,000, ramped from the seam, flat after (display layer, exact) | +589,000 | +589,000 (no rerun needed) | |

Mobile (Dec-15 28d-MA), from August's delivered 17,924,562:

| step | expected | realised | running |
|---|--:|--:|--:|
| August delivered (`h` mobile −27,162 + `t` +299,000 + `p`) | | | 17,924,562 |
| `u` tou_mobile_headwind: the −27,162 mobile leg moved out of `headwind.json`, anchor unchanged | 0 | 0 (exact) | |
| `t` mobile calibration tailwind carried forward unchanged (+299,000) | 0 vs August | 0 (exact) | |
| `p` paid level: August curve 1,559,477 → GMIO curve 1,883,182 at Dec-15 (anchor 808,398; the 2026-09-09 re-pull of feed `_20260909`, replacing the 09-04 pull's 1,891,002); split rebuilt + wired; **mobile rerun done 2026-09-09** (`mobile_cpr0725_paid0909_2026-09-02/`) | +323,705 | realised Dec-15 raw-model 28d-MA −7,072 vs the 09-04 build (paid ramp −7,820 daily at Dec-15); published mobile 18,250,938 | 18,250,938 |
| `p` paid level switched to the **lower-bound** curve (p5, `ci90lo`, pulled 2026-09-10): 1,883,182 → 1,826,168 at Dec-15; **mobile rerun done 2026-09-10** (`mobile_cpr0725_paid0910ci90lo_2026-09-02/`). A planning choice by Brendan, not a measurement change: the point estimate stays on disk as the revert target | −57,014 | −64,273 on the Dec-15 28d-MA (daily −57,014, exact); training rows identical | 18,186,665 |
| Seam refresh 2026-09-02 → 2026-09-09 (2026-09-10): fresh pulls, split rebuilt, same config and paid curve → `mobile_cpr0725_paid0910ci90lo_2026-09-09/` | data-driven, no expectation | +11,013 raw-model Dec-15 28d-MA; paid seam step +66,911 | **18,197,678** |
| `p` paid level switched to the marketing team's **delivered workbook, Low scenario** (point estimate − 3.3% backtest error, variant `low`, imported 2026-09-10 via `pull_paid_dau_curve.py --from-xlsx`): 1,826,168 → 1,814,609 at Dec-15. A planning choice by Brendan, not a measurement change (the workbook's own `Dec15 DAU` footer, 1,774,241, uses a non-standard computation and is wrong — Brendan, 2026-09-10; the Monday series interpolated is used). **Mobile rerun done 2026-09-10** → `mobile_cpr0725_paid0910low_2026-09-09/`, same config and raw pull; the ci90lo build is the revert target | −11,559 daily at Dec-15 (exact; additive post-mozaic) | −2,818 on the Dec-15 28d-MA (the Low curve sits above ci90lo at the seam and below it by December, so the trailing window nets less than the point delta); training rows identical; paid seam step +45,773 (+2.84% of paid; was +66,911) | 18,194,860 |
| `p` paid level switched to the workbook's **Med scenario** (the sheet's 90% CI lower end, variant `med`, imported 2026-09-15 via `pull_paid_dau_curve.py --from-xlsx`) **at the c-suite's request** ("marketing midpoint"): 1,814,609 → 1,833,753 at Dec-15. **Mobile rerun done 2026-09-15** → `mobile_cpr0725_paid0915med_2026-09-09/`, same config and raw pull; the Low build is the revert target | +19,144 daily at Dec-15 (exact; additive post-mozaic) | +19,734 on the Dec-15 28d-MA (exact); training rows identical; paid seam step +67,389 (+4.18% of paid; was +45,773) | **18,214,594** |

## Read this before quoting the headline

| lever | change | Dec-15 effect | basis |
|---|---|---|---|
| `e` launch at login (existing users) | withheld — registered, not applied | 0 while withheld; +736,935 curve value if turned on (realised effect needs the rerun) | producer's curve; the ~736K plateau is a **planned rollout** on 2026-10-15, not an observation, and the en-locale Win10/11 country split was measured once |
| `h` Win10 headwind | anchor −1,315,000 → −726,000, ramp re-anchored at the September seam, flat after Dec-15 | +589,000 on desktop vs August | producer's draft model curve, Dec-15 value only; the shape is our convention |

## Expected layout (populate as the cycle progresses)

```
2026-09/
  september_canonical_v2026-09-04.ipynb  # present — plots + numeric tables only (targets restored, ex-Iran kept, ladder cell added;
                                         #   caveats/benchmark prose dropped). [setup] raises until DESKTOP_FORECAST_PATH / MOBILE_FORECAST_PATH are set
  adjustment_ladder/                 # ladder_manifest.json + cached per-rung desktop runs from scripts/build_adjustment_ladder.py (parquet/pkl gitignored)
  desktop_<config>_<seam>/           # canonical desktop build: .adj-ijlo. parquet + sidecar + parameters.json + pkl (gitignored) — live: desktop_g01_2026-09-09/; revert target: desktop_g01_2026-09-02/
  mobile_<config>_<seam>/            # canonical mobile build: .adj-p. parquet + sidecar + parameters.json + pkl — live: mobile_cpr0725_paid0915med_2026-09-09/ (seam 2026-09-09, workbook Med paid scenario, c-suite 2026-09-15); revert targets: mobile_cpr0725_paid0910low_2026-09-09/ (Low paid), mobile_cpr0725_paid0910ci90lo_2026-09-09/ (ci90lo paid), mobile_cpr0725_paid0910ci90lo_2026-09-02/ (old seam), mobile_cpr0725_paid0909_2026-09-02/ (point-estimate paid)
  mobile_rawpull_2026-09-02/         # present — raw BQ mobile pull (fetch_raw_pull.py, 2026-09-04); reuse via --raw-cache-dir
  mobile_rawpull_2026-09-09/         # present — raw BQ mobile pull at the refreshed seam (2026-09-10); the live mobile build reads it
  desktop_rawpull_2026-09-09/        # present — raw BQ legacy_desktop pull at the refreshed seam (2026-09-10); the live desktop build reads it
  desktop_rawpull_2026-09-02/        # present — raw BQ legacy_desktop pull (2026-09-04); the ladder and scans reuse it
  desktop_raw_ci_2026-09-09/         # present — raw-model desktop (i j l o off) + prediction intervals, 2026-09-10; NOT canonical
  mobile_raw_ci_2026-09-09/          # present — raw-model mobile (p off = total-DAU fit) + prediction intervals, 2026-09-10; NOT canonical
  adjustments/headwind.json          # present — h re-anchored 2026-09-04: linear ramp 0 at seam → −726,000 (Brad's Dec-15) flat after; desktop only; DRAFT
  headwinds/                         # present — h delivered file + value-read meta + plot + rationale
  adjustments/tou_mobile_headwind.json  # present — u (display layer): the mobile -27,162 leg split out of headwind.json 2026-09-04
  tou_mobile_headwind/               # present — u rationale
  adjustments/tailwind.json          # present — t carried forward unchanged 2026-09-04 (+299,000 mobile at Dec-15, ramp from the seam)
  tailwind/                          # present — t September rationale record
  marketing/                         # present — paid-DAU curves for `p`: 09-04 build, 09-09 point-estimate pull, 09-10 lower-bound query pull (ci90lo), 09-10 delivered-workbook Low scenario (variant low), 09-15 delivered-workbook Med scenario (variant med, WIRED, c-suite; source_data/delivered.*.xlsx)
  organic/                           # present — p REBUILT 2026-09-04 (split through 2026-09-01, four checks PASS); repointed 2026-09-09 at marketing/…pull2026-09-09, then 2026-09-10 at marketing/…total_ci90lo…pull2026-09-10 (lower bound), then 2026-09-10 at marketing/…total_low… (workbook Low), then 2026-09-15 at marketing/…total_med.2026-09-09.pull2026-09-15 (workbook Med scenario, c-suite, no anchor; mobile rerun done → mobile_cpr0725_paid0915med_2026-09-09/; Low build was mobile_cpr0725_paid0910low_2026-09-09/)
  launch_at_login_new_users/         # present — `l` re-gated 2026-09-04, renamed 'Launch at Login for new users' (dir, spec, registry name); 200K curve carried unchanged
  mozillaonline/                     # present — `o` REBUILT 2026-09-04 from the 2026-09-02 official export via /ingest-adjustment; rerun pending
  japan_bot/                         # present — `j` WIRED 2026-09-04 (registry + spec + curve + source_data/); rerun pending
  japan_bot_REVERT_2026-09-04/       # present — the handoff's original spec/parquet; revert target, keep while cycle is live
  japan_bot_REVERT_2026-09-09/       # present — the 2026-09-04 MIDDLE build (edge 2026-08-30); revert target, keep while cycle is live
  india_excess_REVERT_2026-09-09/    # present — the 2026-09-04 india_excess build (edge 2026-08-29) and its alternates; revert target
  launch_at_login_existing_users/    # present — `e` INGESTED 2026-09-10 (registry + spec + curve + source_data/ + share SQL); WITHHELD (`withheld: true`), not applied
  india_excess/                      # present — `i` WIRED 2026-09-04 (PROPORTIONAL; hold/linger/settle/fade alternates kept); rerun pending
  india_excess_REVERT_2026-09-04/    # present — pre-ingest spec/parquet; revert target, keep while cycle is live
  september_raw_intervals.ipynb          # present — raw-model desktop + mobile prediction intervals (both *_raw_ci_2026-09-09/ builds), 2026-09-10
  september_desktop_waterfalls.ipynb     # present — desktop waterfalls, 2025 actual→Sep 2026 and Aug→Sep forecast (OS / market / market group); `h` in modern Windows
  csv/september_canonical_curves.csv # + september_dec15_summary.csv + september_desktop_waterfall_steps.csv (add .gitignore exceptions)
  csv/september_canonical_curves.DESKTOP_ONLY.DAILY.csv  # present — desktop daily (unsmoothed) twin, `h` advanced 13.5d, `exact` post-anchor rule, 2026-09-17; csv/README.md
  csv/README.md                      # present — file inventory + the DAILY file's ledger and clamp-rule decision
  plots/  kpi_sheet/  handoff/
  TODO_factors.md
```

Every forecast artifact carries a `.raw.` / `.adj-<codes>.` state marker and a sidecar `.meta.json`;
load only through `mozaic_daily.adjustments.load_forecast()`. Per-cycle inputs the pipeline consumes get
their own subdirectory with a spec gated by `applies_to_forecast_start`.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs, parquets, canonical
CSVs) live here. Cross-month or topic-anchored work goes to `research/{topic}/`. Each new subdirectory
gets an `_index.md`. At the end of the cycle run `/cycle-button-down`.
