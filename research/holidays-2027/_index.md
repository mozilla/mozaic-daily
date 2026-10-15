# holidays-2027 — how holidays will distort 2027 DAU/MAU

Read-only review of the locked 2027 Glean-history desktop build (build 5, `data-official/2027-glean-draft/LOCK.md`):
which 2027 moving holidays collide with each other or with fixed ones, how mozaic's holiday machinery will
handle them, and a holdout test (fit before 2025-12-01, scored on Dec 2025 → Oct 2026) of how well that
machinery works. Deliverable: `index.html`. No forecast is rerun; every effect comes from mozaic's own
`Mozaic._fit_holiday_effects` / `_predict_holiday_effects`.

| File | What |
|---|---|
| `extract_holiday_effects.py` | Pickle → `data/` CSVs: tile 2027 effects, country fitted effects, history, calendar, Prophet yearly term |
| `holiday_calendar.py` | Pure: label explode/tagging (moving, native vs mozaic add-on, same-day duplicates), 2027 clusters, novelty vs 2021–2026 |
| `run_counterfactual_fits.py` | 2027 effects under calendar variants (as built / no duplicates / no outage label / both); asserts as-built reproduces the build |
| `run_holiday_backtest.py` | Same variants, fit before the holdout and scored on it |
| `review_metrics.py` | Pure: world impacts, KPI-window effect, model-free raw dips, cluster scoring and tier rule |
| `build_holiday_review.py` | All numbers → `data/facts.json`, `data/clusters_2027.csv`; all plots → `plots/` |
| `render_holiday_calendar.py` | Same inputs → `calendar.html`: vertical day-by-day 2027 calendar, holidays with country chips, model's world-DAU effect per day, watch-list notes |
| `render_holiday_review.py` | `facts.json` → `index.html` (judgement calls on tiers live in its text) |

Run order: the first three from the repo root, the last two from this directory.

Not here: anything that changes the model or calendar. A fix to mozaic's holiday code (duplicate labels,
the ineffective day-of-week cap, ROW's missing Islamic holidays) belongs in `mozaic-forecasting`, and
needs its own holdout before adoption: this review found that removing duplicates does not reduce DAU error.
