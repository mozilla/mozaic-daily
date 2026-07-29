# `research/param-scans/summer-trough-v2/` — summer-trough search on the August baseline

Desktop parameter search to lift the 2026 summer trough while holding Dec-15. Continuation of
`../aug22-retune/` (which concluded negative on July data) against the August build, after the headwind
seam fix moved the starting point up ~0.67M.

**Status: not started.** `HANDOFF.md` is the brief — read it first. Nothing has been run.

| file | purpose |
|---|---|
| `HANDOFF.md` | The brief: goal, measured slopes, the hypothesis to test first, tooling repointing, traps |

**What isn't here:** the July-data search (`../aug22-retune/`), the Dec-15 searches
(`../desktop_gradient_round{1..4}.ipynb`), and the seam/overlay diagnostic that preceded this
(`data-official/2026-08/desktop_adjustment_ladder.ipynb`).

**Where new code goes:** probe outputs under this directory (never into
`data-official/2026-08/desktop_baseline_2026-07-28/` — a probe would overwrite the canonical build, see
`HANDOFF.md` §9), round `FINDINGS.md` per round, and analysis notebooks with their plots in `plots/`.
