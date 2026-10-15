# `iran_fill_mobile/` — Iran counterfactual for mobile rolling-28 MAU

Produced by `scripts/generate_mobile_mau_iran_fill.py` (2026-10-14) for the 2027 mobile MAU build (`../mobile_mau_2026-10-10/`): the
packaged mozaic `glean_mobile` fill has no `MAU` metric. Same method as every other Iran fill: mobile model (October 2026 lock) fitted on IR
before 2026-02-28, harvested 2026-02-28..2026-06-21, weekly amplitude restored.

| file | what |
|---|---|
| `iran_fill.glean_mobile.MAU.parquet` + `.meta.json` | the finalized fill (tracked), scale **1.0** — passed to the runner as `--iran-fill` |
| `seam_diagnostics.csv`, `seam_plots/seam.glean_mobile.M.png` | fill vs real at the right edge: 7-day means 1,524,153 vs 1,964,222 (+28.9%); the real series overshoots as the rolling window refills, so no scaling |
| `seam_scale_spec.json` | the approved scale (1.0) |
| `_draft/` | smooth and unscaled drafts (untracked) |

Not here: the DAU/NP/EE Iran fills (packaged in mozaic) and the desktop OS × country fill (`../iran_fill/`).
