# `research/param-scans/summer-trough-v2/` — summer-trough search on the August baseline

Desktop parameter search to lift the 2026 summer trough while holding Dec-15. Continuation of
`../aug22-retune/` (which concluded negative on July data) against the August build, after the headwind
seam fix moved the starting point up ~0.67M.

**Status: complete — negative result, awaiting a human decision.** With Dec-15 held to **±50,000**, the
best achievable trough gain across 26 builds is **+16,875 (+0.04%)**, i.e. nothing. The +1.3M lift is real
but indivisible: outcomes are **bimodal** with an empty 1,281,347 gap, because the whole step is
`ROW/modern_windows` (27% of desktop weight) flipping regime atomically at corr −0.1465. Cheapest exit
costs +252,452 on Dec-15 = 5.0× the budget.

Start with **`grid/FINDINGS.md`** and the chart in **`grid_report.html`**. `HANDOFF.md` is superseded as a
plan (its phased approach was abandoned) but is still accurate on the objective, the tooling and the traps.

| file | purpose |
|---|---|
| `grid/FINDINGS.md` | **The result.** Bimodality, the single-tile explanation, what each knob did, the four options |
| `grid_report.html` | Self-contained charts: frontier, the corr dial, seam derivative, parallel coordinates. **Gitignored** (~4.5MB of inline plotly.js so it opens offline); rebuild with `build_grid_report.py` |
| `grid_scores.csv` | Every build scored — Aug-15, Aug-25, trough min, Dec-15, seam slopes, full config. Tracked via a `.gitignore` exception, so the result survives without the blobs |
| `grid/<slug>/` | Per-probe parquet, sidecar, pkl, `parameters.json` (blobs gitignored) |
| `grid/logs/` | Per-probe run logs (gitignored) |
| `phase1/FINDINGS.md` | The earlier single multiplicative probe + the three scorer defects fixed in phase 0 |
| `HANDOFF.md` | Original brief. Plan superseded; objective/tooling/traps still valid |

Producers: `scripts/run_summer_trough_grid.py` (driver), `build_grid_report.py` (report),
`scripts/tile_corr_distribution.py` (the per-tile corr distribution that placed the grid points).

**What isn't here:** the July-data search (`../aug22-retune/`), the Dec-15 searches
(`../desktop_gradient_round{1..4}.ipynb`), and the seam/overlay diagnostic that preceded this
(`data-official/2026-08/desktop_adjustment_ladder.ipynb`).

**Where new code goes:** probe outputs under this directory (never into
`data-official/2026-08/desktop_baseline_2026-07-28/` — a probe would overwrite the canonical build, see
`HANDOFF.md` §9), round `FINDINGS.md` per round, and analysis notebooks with their plots in `plots/`.
