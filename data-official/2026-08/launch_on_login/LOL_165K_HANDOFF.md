# Handoff: rebuild the launch-on-login (`l`) curve with a 165K cap

**Scope: build the curve artifact. Nothing else.** Do not run a forecast, do not touch the notebook,
do not sweep docs or tests. Whoever handed you this is re-running the August desktop forecast
themselves once your curve lands.

**Task owner decision (do not re-litigate):** the LOL curve is allowed to **rise to 165,000 DAU/day
before flattening**, replacing the current flat-at-125,000 clamp. 125K was July's number; August's is
165K.

---

## 1. What you're changing

Launch-on-login: Firefox launches at OS login for new modern-Windows installs. Experiment
`long-term-holdback-2026-growth-desktop`, 100% rollout **2026-05-08**, permanent. Incremental desktop
DAU, measured as excess vs the experiment's holdback control.

It feeds the forecast as adjustment code `l`, a bidirectional overlay on `legacy_desktop` DAU
(`modern_windows` segment). You do **not** need to understand or modify the applier machinery — this is
purely a change to the daily-lift curve it reads.

## 2. Current curve

`data-official/2026-08/launch_on_login/lol_tailwind.2026-06-29.parquet` — DatetimeIndex `target_date`
2026-01-01 → 2027-12-31, columns `lol_lift_daily` (the one the spec reads) and `lol_lift_ma`.

| date | `lol_lift_daily` |
|---|--:|
| ≤ 2026-05-07 | 0 (pre-rollout) |
| 2026-05-10 | 8,451 |
| 2026-06-06 | 88,975 |
| 2026-06-18 | 124,931 |
| **2026-06-19 → 2027-12-31** | **125,000 (hard clamp, one distinct value)** |

Provenance: `lol_tailwind.2026-06-29.model_meta.json`. Load-bearing fields:

```
cap_dau_daily                        125000
first_date_at_cap                    2026-06-19
clean_window_last_date               2026-06-23
contamination_onset                  2026-06-24
measured_daily_excess_at_last_clean  138375.6
smooth_window_days                   7
ff152_transient_excluded             [2026-06-17, 2026-06-18]
rollout_date                         2026-05-08
conservatism_note                    "~125K/day, still rising ~19K/wk; 7d-MA ~145K"
```

## 3. Three hard constraints

**(a) You cannot re-measure past 2026-06-24. Do not try.**
Contamination onset is 2026-06-24 because the holdback *control group received the feature* — the
counterfactual is permanently gone. No fresh telemetry extends the clean window; querying recent data
shows the excess "collapsing," which is an artifact, not a decay. **Any rise beyond 2026-06-23 is
extrapolation by construction.** If you start writing a BigQuery query to measure LOL excess in July,
stop.

**(b) 165,000 is a human judgement, not a measurement. Implement it; don't optimise it.**
It sits deliberately between two known anchors: **125,000** (the previous conservative flat cap) and
**~220,000** (an independent convolution model in `~/work/launch-on-login/`; July recorded the
conservatism gap as ≈96K, see `../../2026-07/launch_on_login/plots/`).

**(c) The 125K clamp truncated real measurement, so un-clamping is part of the job.**
`measured_daily_excess_at_last_clean = 138,375.6` at 2026-06-23, but the curve reads 125,000 there —
the old cap was biting *below* the last clean measurement. With a 165K ceiling, 2026-06-19 → 2026-06-23
should follow measurement rather than the clamp.

## 4. Construction

**Measured rise through the clean window → linear extrapolation to 165,000 → flat at 165,000 through
2027-12-31.**

Using the recorded ~19,000/wk (≈2,714/day) slope at the clean cutoff:

- From 138,376 at 2026-06-23: `(165,000 − 138,376) / 2,714 ≈ 9.8 days` → reaches 165K ≈ **2026-07-03**.
- From the 7d-MA ~145,000 instead: `(165,000 − 145,000) / 2,714 ≈ 7.4 days` → ≈ **2026-07-01**.

Either way it tops out in **early July 2026** and is flat thereafter. State which anchor and slope you
used in the meta — don't leave it implicit.

**Reconcile this before you build.** `model_meta.json` gives three different levels at the clean
cutoff: `measured_daily_excess_at_last_clean = 138,375.6`, and a `conservatism_note` saying both
"~125K/day" and "7d-MA ~145K". These cannot all be the quantity being capped. Read
`build_lol_tailwind.py` and determine **which series the cap applies to** (raw daily excess vs its
7d-MA) before picking your ramp anchor. Getting it wrong shifts the curve by ~7K/day.

## 5. Producer

`~/work/launch-on-login/build_lol_tailwind.py`, reading cached inputs
`~/work/launch-on-login/tmp/obs_dau.parquet` and `tmp/obs_enr.parquet` — both present, dated
2026-06-30. They are the clean-window snapshot, which is exactly what you want; **do not refresh
them** (see constraint (a)).

⚠️ **`~/work/launch-on-login/` is NOT a git repository.** No version control, no undo. Preserve the
original `build_lol_tailwind.py` before editing, and prefer adding a `--cap` argument over hardcoding
165000, so the 125K curve stays reproducible.

## 6. Deliverables — exactly these

**In `~/work/launch-on-login/`**
- `build_lol_tailwind.py` parameterised on the cap (`--cap`), able to emit both the 125K and 165K
  curves. Original preserved.

**In `data-official/2026-08/launch_on_login/`**
1. `lol_tailwind.2026-07-29.parquet` — the new curve. Same schema (`target_date` DatetimeIndex,
   `lol_lift_daily` + `lol_lift_ma` columns), same 2026-01-01 → 2027-12-31 span. Use today's date in
   the filename — it records when the curve was built, and must not claim 2026-06-29.
2. `lol_tailwind.2026-07-29.model_meta.json` — same field set as the existing meta, with
   `cap_dau_daily: 165000`, a `first_date_at_cap` matching your new ramp, your extrapolation method
   and slope stated explicitly, and a field or note making clear that everything after 2026-06-23 is
   extrapolated rather than measured.
3. `lol.json` — repoint `data_file` and `model_meta_file` at the new files, and rewrite `notes`: it
   currently describes the 125K clamp and says the curve is a stale carry-forward "expected to be
   re-measured and swapped this cycle." That is now done, so the carry-forward framing must go.
   **Leave `applies_to_forecast_start: "2026-07-28"` exactly as it is.**
4. `_index.md` — update the cap facts and drop the "Carried forward from July — STALE" framing.

**Leave the `lol_tailwind.2026-06-29.*` files on disk.** They are superseded, not deleted: the
already-committed August baseline sidecar records their sha1, so removing them would break that
artifact's verifiability. Note them as superseded in `_index.md`. (Also: never `git rm` from a cycle
branch — pruning happens only on `clean-slate`.)

## 7. Verify before handing back

- The curve loads and resolves through the real code path:
  ```python
  from mozaic_daily.adjustments import load_overlay_spec, load_lift_series
  from pathlib import Path
  p = "data-official/2026-08/launch_on_login/lol.json"
  spec = load_overlay_spec(p)
  s = load_lift_series(spec, Path(p).parent)
  print(s.index.min(), s.index.max(), s.max(), s.loc["2026-07-27"], s.loc["2026-12-15"])
  ```
  Expect: span 2026-01-01 → 2027-12-31, max exactly 165000, and 165000 at both 2026-07-27 and
  2026-12-15.
- The spec is still found for the August anchor:
  ```python
  from mozaic_daily.main import _find_launch_on_login_spec_for_forecast
  print(_find_launch_on_login_spec_for_forecast("2026-07-28"))
  ```
  Must print the `data-official/2026-08/...` path. If it prints `None`, `applies_to_forecast_start` got
  broken and the overlay would silently not apply.
- No zeros or NaNs anywhere in 2026-05-08 → 2027-12-31, and the curve is monotonically
  non-decreasing.
- `python3 -m pytest tests/ -q` still reports **286 passed** (a curve swap should not move it).

## 8. Out of scope — do not do these

- **Do not run a forecast** (`run_param_scan.py`, `run_main.py`). That's being handled separately.
- **Do not touch the canonical notebook** `august_canonical_v2026-07-28.ipynb`, `CLAUDE.md`,
  `data-official/2026-08/_index.md`, or `scripts/verify_lol_overlay.py`.
- **Do not touch the `o` (MozillaOnline) or `m` (marketing) curves.** Also stale, also slated for
  refresh, but separate tasks.
- **Do not revisit the Win10 headwind anchor** (−1,345,000) or implement the summer-trough overlay.
- **Do not modify `~/work/mozaic-forecasting-official/`** — 9 unpushed commits, off-limits.

## 9. Environment

- `source .venv/bin/activate` before any Python. Branch `august-forecast`.
- Scratch files in `./tmp/`, never `/tmp/`. Reusable scripts never under `tmp/`.
- If you make a diagnostic plot of the old-vs-new curve, put it in a notebook cell and save it to
  `data-official/2026-08/plots/` — never `tmp/` — and cite the path. A plot is optional here.
- Commit your work with a message stating the cap change and the extrapolation choice you made.
