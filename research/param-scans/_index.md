# `research/param-scans/` — Prophet parameter sensitivity exploration

Sweeps over `changepoint_prior_scale`, `recent_weeks`, `holiday_threshold`, and `clip` to understand how the model responds. Desktop-only (mobile's parameter search lives elsewhere — see scope discussion in CLAUDE.md).

## Files

| File | Purpose |
|---|---|
| `param_scan_exploration.ipynb` | Analysis notebook; reads from `results/` and `pinned/` |
| `results/` | Output of `scripts/run_param_scan.py` — unpinned wide sweep. **Gitignored, regenerable.** |
| `pinned/` | Output of `scripts/run_pinned_scan.py` — curated subset reusing the April changepoints pkl. **Gitignored, regenerable.** |

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
