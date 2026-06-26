# `data-official/2026-06/update_scenarios/` — June forecast scenario plots

A month-scoped, self-contained scenario built **on top of** the June 2026 canonical
ALL-level curves. Two stakeholder charts in the KPI Looker-replica style (gray 2025
actuals, orange current actuals, purple/blue June forecasts (+ pink MozillaOnline on desktop), gold Dec-15 markers,
red Dec-15 line, legend below the axes):

- **Desktop** — adds a solid pink (purple-red) `June Forecast + MozillaOnline` line (the solid purple June
  line is the +Iran forecast, solid blue is ex-Iran): the June +Iran forecast
  with a MozillaOnline (China distribution partner) onboarding modeled as a **+500k
  daily-DAU step on 2026-06-02**. Because the curves are 28-day moving averages, that
  daily step renders as a 28-day linear ramp in MA space, then parallels the base
  curve (+500k, Dec-15 28dMA 47.8M → 48.3M).
- **Mobile** — adds the prior purple-dashed `April Forecast` line (the April +Iran
  forecast), clipped to start **2026-04-01** (the April forecast spans the full year;
  only the Apr-onward portion is drawn).
- **Mobile (alternate)** — a second mobile chart that swaps the prior April line for the
  **April ex-Iran** forecast (blue dashed, matching June ex-Iran's color), and omits the
  plain April +Iran line.

## What's here
| File | What it is |
|---|---|
| `build_augmented_csv.py` | Reads the pristine `../csv/june_canonical_curves.csv`, adds the MozillaOnline ramp columns and last-year (2025) 28dMA actuals (pulled once from BigQuery, aligned onto the 2026 axis), writes `augmented_curves.csv`. **Only step that needs BQ.** |
| `augmented_curves.csv` | The canonical 13 columns + `{desktop,mobile}_mozillaonline_plus_iran` + `{desktop,mobile}_actuals_2025`. The single input the plot script reads. |
| `plot_scenario.py` | Portable plotter — reads only `augmented_curves.csv` (no BQ, no model). Writes the three PNGs. |
| `desktop_mozillaonline.png`, `mobile_with_april.png` | The main output charts. |
| `mobile_with_april_exiran.png` | Alternate mobile chart: April **ex-Iran** (blue dashed) instead of the plain April +Iran line. |

## What isn't here
- The canonical curves themselves and their data card live in `../csv/` and are left
  **untouched** — this folder only derives from them. To refresh after a canonical
  rebuild, re-run `build_augmented_csv.py` then `plot_scenario.py`.
- The seam-smoothing logic, model parquets, and adjustment specs — see `../` and
  `research/ma-seam-turbulence/`.

## Where new code goes
Scenario variants on the June ALL-level curves (different partner deltas, additional
overlay lines) belong here. Cross-month or mechanism work belongs under `research/`.

## Rebuilding
```bash
source .venv/bin/activate
python3 data-official/2026-06/update_scenarios/build_augmented_csv.py   # needs BQ auth
python3 data-official/2026-06/update_scenarios/plot_scenario.py
```
