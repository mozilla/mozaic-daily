# Handoff: rebuild the launch-on-login (`l`) curve with a 165K cap

**Task owner decision (do not re-litigate):** the LOL curve is allowed to **rise to 165,000 DAU/day
before flattening**, replacing the current flat-at-125,000 clamp.

Everything below is context you need to execute that correctly. Read it all before writing code —
several of the traps here silently produce a plausible-but-wrong number.

---

## 1. What launch-on-login is

Firefox launches at OS login for new modern-Windows installs. Experiment
`long-term-holdback-2026-growth-desktop`, 100% rollout **2026-05-08**, permanent. It adds incremental
desktop DAU, measured as excess vs the experiment's holdback control.

It is wired into the forecast as adjustment code **`l`**, a **bidirectional overlay** on
`legacy_desktop` DAU, `modern_windows` segment only:

1. The curve is **subtracted** from `modern_windows` training rows before mozaic (allocated across
   countries by trailing-28d `modern_windows` DAU share) so Prophet learns the *no-LOL* dynamic.
2. The curve is **added back** to the per-country forecast afterwards.

Machinery: `src/mozaic_daily/adjustments.py` (`load_overlay_spec`, `subtract_lift_from_training`,
`add_lift_to_forecast`), generic and shared with `m` (marketing) and `o` (MozillaOnline). **You should
not need to touch the appliers at all** — this is a curve + spec change.

## 2. Current state of the curve

`data-official/2026-08/launch_on_login/lol_tailwind.2026-06-29.parquet` — DatetimeIndex `target_date`
2026-01-01 → 2027-12-31, columns `lol_lift_daily` (the one the spec reads) and `lol_lift_ma`.

Shape today:

| date | `lol_lift_daily` |
|---|--:|
| ≤ 2026-05-07 | 0 (pre-rollout) |
| 2026-05-10 | 8,451 |
| 2026-06-06 | 88,975 |
| 2026-06-18 | 124,931 |
| **2026-06-19 → 2027-12-31** | **125,000 (hard clamp, single distinct value)** |

It rises tracking the measured 7d-MA of the holdback excess, then clamps flat at 125,000 from
**2026-06-19** onward forever.

Provenance is in `lol_tailwind.2026-06-29.model_meta.json`. The load-bearing fields:

```
cap_dau_daily                    125000
first_date_at_cap                2026-06-19
clean_window_last_date           2026-06-23
contamination_onset              2026-06-24
measured_daily_excess_at_last_clean  138375.6
smooth_window_days               7
ff152_transient_excluded         [2026-06-17, 2026-06-18]
rollout_date                     2026-05-08
conservatism_note                "~125K/day, still rising ~19K/wk; 7d-MA ~145K"
```

## 3. The three hard constraints

**(a) You cannot re-measure past 2026-06-24. Do not try.**
Contamination onset is 2026-06-24 because the holdback *control group received the feature* — the
counterfactual is permanently gone. There is no fresh telemetry that extends the clean window, and
querying recent data will show the excess "collapsing," which is an artifact, not a decay. **Any rise
beyond 2026-06-23 is extrapolation by construction.** If you find yourself writing a BigQuery query to
measure LOL excess in July, stop.

**(b) 165,000 is a human judgement, not a measurement. Do not re-derive or "improve" it.**
It sits deliberately between two known anchors:
- **125,000** — the previous deliberately-conservative flat cap.
- **~220,000** — an independent convolution model in `~/work/launch-on-login/`. July recorded the
  conservatism gap as ≈96K (see `../../2026-07/launch_on_login/plots/`).

165K is a middle position chosen by the task owner. Implement it; don't optimise it.

**(c) The 125K clamp truncated real measurement, so un-clamping is part of the job.**
`measured_daily_excess_at_last_clean = 138,375.6` at 2026-06-23, but the curve reads 125,000 there.
The old cap was biting *below* the last clean measurement. With a 165K ceiling, 2026-06-19 → 2026-06-23
should follow measurement rather than the clamp.

## 4. Construction to implement

Rebuild the curve as: **measured rise through the clean window → linear extrapolation to 165,000 →
flat at 165,000 through 2027-12-31.**

Proposed arithmetic, using the recorded ~19,000/wk (= ~2,714/day) slope at the clean cutoff:

- From 138,376 at 2026-06-23: `(165,000 − 138,376) / 2,714 ≈ 9.8 days` → hits 165K about **2026-07-03**.
- From the 7d-MA ~145,000 instead: `(165,000 − 145,000) / 2,714 ≈ 7.4 days` → about **2026-07-01**.

Either way the curve reaches its ceiling in **early July 2026** and is flat from there. Since the
forecast trains through 2026-07-27 and the KPI is Dec-15, the exact day within that window is a
second-order effect — but **you must state which you chose and why in the meta**, not leave it implicit.

**Reconcile this inconsistency before you build.** `model_meta.json` says three different things about
the level at the clean cutoff: `measured_daily_excess_at_last_clean = 138,375.6`, and a
`conservatism_note` saying "~125K/day" *and* "7d-MA ~145K". These cannot all be the quantity being
capped. Read `~/work/launch-on-login/build_lol_tailwind.py` and determine **which series the cap is
applied to** (raw daily excess vs its 7d-MA) before choosing your ramp anchor. Getting this wrong
shifts the whole curve by ~7K/day.

## 5. Producer

`~/work/launch-on-login/build_lol_tailwind.py`, reading cached inputs
`~/work/launch-on-login/tmp/obs_dau.parquet` and `tmp/obs_enr.parquet` (both present, dated
2026-06-30 — they are the clean-window snapshot, which is exactly what you want; **do not refresh
them**, see constraint (a)).

⚠️ **`~/work/launch-on-login/` is NOT a git repository.** There is no version control and no undo.
Copy `build_lol_tailwind.py` to a new filename (or otherwise preserve the original) before editing,
and prefer adding a `--cap` parameter over hardcoding 165000 in place, so the 125K result stays
reproducible.

## 6. Deliverables

### In `~/work/launch-on-login/`
- Parameterised producer (`--cap`, defaulting to something explicit) that can emit both the 125K and
  165K curves.

### In `data-official/2026-08/launch_on_login/`
- `lol_tailwind.<YYYY-MM-DD>.parquet` — new curve, same schema (`target_date` index,
  `lol_lift_daily` + `lol_lift_ma`), same 2026-01-01 → 2027-12-31 span. **New date in the filename;
  do not overwrite the 2026-06-29 files** — the 125K curve must stay on disk for comparison.
- `lol_tailwind.<YYYY-MM-DD>.model_meta.json` — same field set, with `cap_dau_daily: 165000`, a
  `first_date_at_cap` reflecting the new ramp, the extrapolation method and slope stated explicitly,
  and a note that everything after 2026-06-23 is extrapolated.
- `lol.json` — update `data_file`, `model_meta_file`, and the `notes` (the notes currently describe the
  125K clamp and say "expected to be re-measured and swapped this cycle"). **Leave
  `applies_to_forecast_start: "2026-07-28"` alone** unless you are also re-anchoring the forecast.
- `_index.md` — update the cap facts and drop the "STALE / carried forward" framing for this overlay.

### Rebuild the desktop forecast
```bash
source .venv/bin/activate
python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/desktop_lol165_2026-07-28 \
    --changepoint-prior-scale 0.08983 --changepoint-range 0.65 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6
```
⚠️ **Use a NEW `--results-dir`** (as above). The output filename is
`mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — **identical** to the existing baseline's,
because the adjustment codes haven't changed. Reusing `desktop_baseline_2026-07-28/` would silently
clobber the 125K baseline and destroy the comparison.

Reuse the cached BQ pull instead of re-querying:
`--raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825`

Runtime ~15 min for desktop DAU. **Do not poll the log or re-run mid-flight** — wait for it.

### Report
Desktop Dec-15 2026 28d-MA **post-headwind**, versus these two reference points:

| build | Dec-15 desktop 28d-MA |
|---|--:|
| July delivered (125K LOL) | 48,585,483 |
| August baseline (125K LOL, 2026-07-28 anchor) | 48,520,714 |
| **your 165K build** | ? |

## 7. Expect the net effect to be much less than +40K

The overlay is **bidirectional**: raising the cap raises both the amount subtracted from training rows
*and* the amount added back. July measured the net Dec-15 effect of the whole 125K curve at only
**+102K**, not +125K, because Prophet had already extrapolated ~23K of the rise from the raw data. The
same partial-offset applies here. **Do not assume 125K → 165K buys +40K at Dec-15.** Measure it, and if
the delta looks like a clean +40,000, treat that as a signal something is wrong (e.g. the curve was
added back but not subtracted from training).

`scripts/verify_lol_overlay.py` produces the three-curve isolation (raw / subtract-only / full) that
decomposes this. Note it hardcodes `FORECAST_START = "2026-06-29"` and a July output path, and its plot
labels say "125K" — parameterise or update it rather than reading stale output.

## 8. Docs and tests to update

- `CLAUDE.md` line ~392 — the adjustment-code table describes `l` as "the capped/flat curve (125K)".
- `data-official/2026-08/_index.md` — the status table, the Dec-15 numbers, and the "Next up" bullet
  saying the overlay curves are stale (LOL will no longer be).
- `data-official/2026-08/august_canonical_v2026-07-28.ipynb` — the `[baseline-caveats]` markdown cell
  has a per-overlay table whose `l` row describes the flat 125K and its bias direction. **Edit notebook
  cells with `python3 /Users/brendanwells/work/nb_cells/nb_cells.py edit <nb> <cell-name> --file <f>`,
  never by hand-editing the JSON and never with NotebookEdit.** Re-execute the notebook after swapping
  the curve so its numbers and plots regenerate.
- `tests/` — 286 currently pass; run the full suite. `tests/test_adjustments.py` covers the overlay
  appliers. If you add a `--cap` parameter, the loader/spec path is unchanged, so no new applier tests
  should be needed — but confirm rather than assume.

## 9. Out of scope — do not do these

- **Do not touch the `o` (MozillaOnline) or `m` (marketing) curves.** They are also stale and also
  slated for refresh, but they are separate tasks. Changing two overlays at once makes the Dec-15
  delta uninterpretable.
- **Do not revisit the Win10 headwind anchor** (−1,345,000). Open, but a separate decision.
- **Do not implement the summer-trough overlay.** It is a recommendation awaiting go/no-go.
- **Do not modify `~/work/mozaic-forecasting-official/`** (the mozaic package checkout). It has 9
  unpushed commits and is off-limits.
- **Do not `git rm` anything from a cycle branch.** Pruning happens only on `clean-slate`.
- Do not create CSV exports or a `kpi_sheet/` — August is still a baseline, not a delivery.

## 10. Environment notes

- `source .venv/bin/activate` before any Python.
- Branch is `august-forecast`. Working tree was clean at handoff (commit `140412e`).
- Every forecast artifact needs its `.raw.`/`.adj-{codes}.` filename marker **and** a sidecar
  `.meta.json`; load through `mozaic_daily.adjustments.load_forecast`, never bare `pd.read_parquet`.
  The param-scan runner handles both automatically.
- Overlay specs are matched by **exact string equality** on `applies_to_forecast_start`. If you change
  that field and it no longer equals the run date, the overlay silently does not apply and the output
  is stamped `.raw.` instead of `.adj-lo.`. The canonical notebook guards against this with
  `load_forecast(..., require_state=["l","o"])`.
- Scratch files go in `./tmp/`, not `/tmp/`. Reusable scripts never go in `tmp/`.
- Plots only inside notebook cells, saved to `data-official/2026-08/plots/` — never to `tmp/`, and
  always cite the path when referring to one.
