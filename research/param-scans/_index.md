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

## Files

| File | Purpose |
|---|---|
| `param_scan_exploration.ipynb` | Desktop analysis notebook |
| `summer-trough-v2/` | August desktop s01 lock. `s01_canonical_desktop.ipynb` was **repointed at `mozaic_daily.seam_ma`** on 2026-07-29, so its early-horizon curves differ from when it was first run. |
| `mobile-july/` | July mobile grid search (notebooks + sidecars present; `results/` blobs archived) |
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

- The mechanism investigation that motivated these scans: `../april-vs-june-mechanism/`
- Production-validated param sets feed `data-official/{YYYY-MM}/desktop_*/parameters.json`
