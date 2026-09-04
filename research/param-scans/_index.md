# `research/param-scans/` — Prophet parameter sensitivity exploration

Sweeps over `changepoint_prior_scale`, `recent_weeks`, `holiday_threshold`, `changepoint_range`, and
`clip` to understand how the model responds. Desktop sweeps used to live at this level
(`desktop_gradient_round{1..4}.ipynb` — **moved to `_archive/` on 2026-07-29**, see below); the July
**mobile** grid search lives in `mobile-july/`.

## Present vs Archived

- **Present (on disk):** the analysis notebooks (`param_scan_exploration.ipynb`),
  `mobile-july/` notebooks + `_index.md` + `plots/` + the
  per-config `parameters.json`/`*.meta.json` sidecars (the record of what each config produced).
- **Archived to GCS (`gs://…/july-2026/param-scans/`), removed from disk:** the multi-GB sweep
  **`results/`** and `mobile-july/results/` forecast-output blobs. Regenerable via the scan drivers
  below, but re-running is compute-expensive — pull from GCS to reuse. Sidecar `.meta.json` +
  `parameters.json` for each config remain on disk so the search is still legible without the blobs.
- **August searches archived to GCS (`gs://…/august-2026/param-scans/`), blobs removed from disk
  2026-09-04:** `summer-trough-v2/` (426 objects, 68 probe pkls), `mobile-aug/` (230 objects, 33 probe
  pkls) and `aug25-gap/` (2,003 objects, 390 probes) — probe pickles **and** probe parquets are in GCS,
  verified by object count and pickle size list; every `parameters.json` / `*.meta.json` / scores CSV /
  `FINDINGS.md` / notebook remains on disk. Scan symlinks to the shared raw pull were resolved on
  upload, so each archived probe directory is self-contained. Regenerable report HTML was deleted.
- **`aug22-retune/` is slimmed here:** only its summary artifacts (`_index.md`, `FINDINGS.md`,
  `*_scores.csv`, best-fit curve + notebook + plot, generators) are on this branch. Its ~197
  per-probe `run.log` / `parameters.json` / `*.meta.json` sidecars live on the `july-forecast`
  branch, and its ~40G of forecast blobs are archived to GCS under `july-2026/`.

Most sweeps here target the **Dec-15 far horizon**. Two exceptions target the **Aug-2026 summer
trough**, and they pull in *opposite directions* — do not read one's results into the other:

- `aug22-retune/` and `summer-trough-v2/` moved the trough **up**, toward a 45M–46M band.
- `aug25-gap/` moves it **down**, to close 10% of the August-vs-July gap.

`scripts/score_near_horizon.py` is shared by all three and still prints a `target band : 45M-46M`
line belonging to the *upward* objective. It is meaningless for `aug25-gap/`.

Note that `score_near_horizon.py` is **desktop-only**. The mobile searches use
`scripts/mobile_scoring.py`, which is not interchangeable with it: it reads `mobile_dau` from the
headwind spec (~45× smaller than `desktop_dau`) and selects on mobile's `"{}"` segment rather than
`'{"os": "ALL"}'`. Also: desktop scores are **not comparable across the 2026-07-29 `Fix A` boundary**,
because that scorer's window overlaps the seam transition.

## Standing policy: holiday knobs are excluded from every search

**All four holiday parameters** (`holiday_threshold`, `holiday_max_radius`, `holiday_min_radius`,
`holiday_effect_floor`) are permanently excluded from parameter searches, on principle: they govern
**strictly local** effects, and a local effect must never be used to move a whole-season quantity like
the Dec-15 headline or the summer trough. Do not re-propose them on the grounds that their measured
slope is large — a large slope on a whole-season KPI is precisely the symptom that makes them
inadmissible. `scripts/run_trend_only_grid.py` refuses holiday overrides by design, and the canonical
notebook asserts desktop's four holiday knobs sit at package defaults.

One documented exception, which is a *pin* rather than a tuning: mobile carries
`holiday_threshold = −0.055` (off-default, inherited from July's `grad_moderate` search, which predates
this policy). It is held fixed so the search centre equals the build actually in production; resetting
it to the −0.032 default was considered and declined on 2026-07-31.

## Files

| File | Purpose |
|---|---|
| `param_scan_exploration.ipynb` | Desktop analysis notebook |
| `summer-trough-v2/` | August desktop **s01** retune (later superseded by **g01** — see `aug25-gap/`). `s01_canonical_desktop.ipynb` was **repointed at `mozaic_daily.seam_ma`** on 2026-07-29, so its early-horizon curves differ from when it was first run. Its like-for-like s01 measurement is deliberately made on a LOL curve that is no longer active — that is *what isolates the config* — so do not repoint it at the canonical build. |
| `mobile-july/` | July mobile grid search (notebooks + sidecars present; `results/` blobs archived) |
| `mobile-aug/` | **August mobile search** (`glean_mobile` DAU): calibrating the new `p` paid/organic build back toward July's delivered Dec-15. 33 probes across three `seasonality_regime` values. Concluded **parameters cannot close the gap** — the whole envelope spans 63,539 against a 322,714 gap — and established the structural reason: **mozaic reconciles top-down, so the mobile world headline is effectively one Prophet fit on the aggregate and per-tile knobs largely cancel.** Outcome: base locked at `cpr 0.725`, remainder carried by the explicit `t` overlay. Start here for any future mobile parameter work. |
| `aug25-gap/` | **Downward** near-horizon search (opened 2026-07-30): close 10% of the Aug-25 August-vs-July gap (−196,183 to 45,027,066) on Prophet params only, holding Dec-15 within ±50,000. Side-folder work; `data-official/2026-08/desktop_locked/` stays canonical. See its `_index.md` and `PLAN.md`. |
| `aug22-retune/` | Near-horizon (Aug-22 trough) desktop retune — 3 rounds + LHS sampling. Concluded params **can't** hit Aug without breaking Dec; recommends a summer-trough overlay. Summary artifacts present; per-probe sidecars on `july-forecast` only. See its `_index.md`. |
| `results/`, `pinned/` | Output of `scripts/run_param_scan.py` / `run_pinned_scan.py`. **Archived to GCS; regenerable.** |

## Regenerating

`results/` and `pinned/` are gitignored because they're a few GB each and the producer is deterministic given the inputs:

```bash
source .venv/bin/activate

# Wide sweep (~45 configs)
python scripts/run_param_scan.py \
    --forecast-start-date 2026-05-13 \
    --raw-cache-dir data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6

# Curated pinned subset
python scripts/run_pinned_scan.py \
    --forecast-start-date 2026-05-17 \
    --raw-cache-dir data-official/2026-06/desktop_cps0.15983_thresh032_recent13_clip0.6_cap426 \
    --april-pkl data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_objects.legacy_desktop.2026-04-01.pkl
```

Pinning the April changepoints (the second command) is the key trick — it isolates the impact of parameter changes from the impact of refit changepoint placement.

## Related

- The mechanism investigation that motivated these scans: `april-vs-june-mechanism/` — **archived to GCS
  and no longer on disk** (`gs://…/research-superseded/`, or the `july-forecast` branch history)
- Production-validated param sets feed `data-official/{YYYY-MM}/desktop_*/parameters.json`
