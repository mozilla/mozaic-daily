# `research/param-scans/summer-trough-v2/` — summer-trough search on the August baseline

Desktop parameter search to lift the 2026 summer trough while holding Dec-15. Continuation of
`../aug22-retune/` (which concluded negative on July data) against the August build, after the headwind
seam fix moved the starting point up ~0.67M.

**Status: phases 0–1 done, awaiting a decision on Dec-15 drift.** `regime=multiplicative` alone put the
trough **in band** at 45,140,569 (from 43,833,674) for +252,550 on Dec-15 — a 5.17:1 trade, and it cut the
seam slope kink 68% as a side effect. `HANDOFF.md` is still the brief for what remains; read
`phase1/FINDINGS.md` for what has been measured.

| file | purpose |
|---|---|
| `HANDOFF.md` | The brief: goal, measured slopes, the hypothesis to test first, tooling repointing, traps |
| `phase1/FINDINGS.md` | Center re-measure + the multiplicative probe; the three scorer defects fixed in phase 0 |
| `phase1/<slug>/` | The probe's parquet, sidecar, pkl, `parameters.json` (blobs gitignored) |

**What isn't here:** the July-data search (`../aug22-retune/`), the Dec-15 searches
(`../desktop_gradient_round{1..4}.ipynb`), and the seam/overlay diagnostic that preceded this
(`data-official/2026-08/desktop_adjustment_ladder.ipynb`).

**Where new code goes:** probe outputs under this directory (never into
`data-official/2026-08/desktop_baseline_2026-07-28/` — a probe would overwrite the canonical build, see
`HANDOFF.md` §9), round `FINDINGS.md` per round, and analysis notebooks with their plots in `plots/`.
