# `research/param-scans/` — Prophet parameter sensitivity exploration

Sweeps over `changepoint_prior_scale`, `recent_weeks`, `holiday_threshold`, `changepoint_range`, and
`clip` to understand how the model responds. Desktop sweeps live at this level
(`desktop_gradient_round{1..4}.ipynb`); the July **mobile** grid search lives in `mobile-july/`.

## Present vs Archived

- **Present (on disk):** the analysis notebooks (`param_scan_exploration.ipynb`,
  `desktop_gradient_round{1..4}.ipynb`), `mobile-july/` notebooks + `_index.md` + `plots/` + the
  per-config `parameters.json`/`*.meta.json` sidecars (the record of what each config produced).
- **Archived to GCS (`gs://…/july-2026/param-scans/`), removed from disk:** the multi-GB sweep
  **`results/`** and `mobile-july/results/` forecast-output blobs. Regenerable via the scan drivers
  below, but re-running is compute-expensive — pull from GCS to reuse. Sidecar `.meta.json` +
  `parameters.json` for each config remain on disk so the search is still legible without the blobs.

## Files

| File | Purpose |
|---|---|
| `param_scan_exploration.ipynb`, `desktop_gradient_round{1..4}.ipynb` | Desktop analysis notebooks |
| `mobile-july/` | July mobile grid search (notebooks + sidecars present; `results/` blobs archived) |
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
