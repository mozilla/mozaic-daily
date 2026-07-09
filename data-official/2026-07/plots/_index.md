# `data-official/2026-07/plots/` — July canonical review plots

Rendered figures for the July cycle review (produced by the canonical + review notebooks).

- `global_legacy_desktop.{dau,new_profiles}.png`, `global_glean_mobile.{dau,new_profiles}.png` —
  global (ALL-level) forecast curves.
- `legacy_desktop_grid.{dau,new_profiles}.png`, `glean_mobile_grid.{dau,new_profiles}.png` —
  per-segment grids.
- `headwind_tailwind_breakdown.png` — the headwind (`h`) vs tailwind (`l`+`o`) decomposition
  (companion to `../headwind_tailwind_breakdown.ipynb`).
- `canonical_review_2026-07-06/` — the dated review-plot set for the 2026-07-06 lock.

**Where new files go:** plots for this cycle only (project rule: plots are never `tmp/` throwaways).
Regenerate from `../july_canonical_v2026-06-29.ipynb` / `../canonical_review_2026-07-06.ipynb`.
