# `research/executive-plot/` — executive-style KPI forecast output plot

A new stakeholder-facing chart format for the KPI forecast (desktop + mobile DAU), aimed at an
executive audience rather than the diagnostic canonical plots (`global_<platform>.png`). Cross-cycle
by design: the format is fixed once and then re-rendered every cycle from that cycle's canonical
curves, so it lives here rather than under `data-official/{YYYY-MM}/`.

## Status

**Prototype built 2026-09-09, under review.** Style decisions taken so far are recorded in the notebook
header and `[setup]` constants (locked colors, allowed tick steps 1M / 0.5M / 0.1M, labels at line ends,
reference labels in the right margin, DRAFT watermark on until the flag is flipped).

## Files

| Path | Purpose |
|---|---|
| `executive_plots.ipynb` | **The renderer.** Reads the cycle's canonical CSV (asserting it is newer than its source parquets and agrees with the Dec-15 summary CSV), draws Desktop / Mobile / Combined in the executive style, saves to `plots/`. Cycle-scoped constants in `[setup]`; product bets are a hand-edited list there, drawn only on the combined chart as forecast + linear ramp to a Dec-15 increment. Kernel `mozaic-daily-venv`. |
| `examples/` | Reference screenshots the format is aimed at (August 2026 cycle, delivered by hand). Inputs only, never regenerated. |
| `plots/` | Rendered output: `executive_{desktop,mobile,combined}.png`. Never write plots to `tmp/`. |

### `examples/` inventory

| File | Source / what to take from it |
|---|---|
| `Screenshot 2026-09-09 at 2.56.19 PM.png` | Desktop DAU, Aug vs Jun forecast. Low/Base/Stretch as faint lines labelled at right; prior label flips below its line. |
| `Screenshot 2026-09-09 at 2.56.34 PM.png` | Mobile DAU. 0.5M tick step; `18M` / `17.5M` mixed-precision labels. |
| `Screenshot 2026-09-09 at 2.56.57 PM.png` | Combined DAU. Single "Combined Flat" line (last year's Dec-15); product bet as a green line shaded to the forecast, kept separate from the forecast tail. |

## What is not here

- The data the plot reads: cycle canonical CSVs in `data-official/{YYYY-MM}/` (e.g. `*_canonical_curves.csv`).
- The seam/MA logic: `mozaic_daily.seam_ma.display_ma` — this directory only renders, never recomputes.
- The diagnostic plot set: `scripts/plot_forecast_set.py`.

## Open review items (2026-09-09)

- Product bets are **hidden** (`SHOW_PRODUCT_BETS = False`) until Product delivers a number; the entry in `[setup]` is an illustrative +500,000 placeholder. When shown, bet labels stack above the current-forecast label.
- Labels drawn above a line end take the free slot nearest their default that clears every reference line
  (`place_label_above`, decided 2026-09-15 from rendered variants): boxed if the gap fits the white backing box,
  bare text if it only fits the glyphs (Sep 2026 desktop sits between Base and Stretch), never below the default
  so the label cannot cover its own curve. The default stays anchored to the Dec-15 end, but the label's bottom
  edge must also clear the curve's MAXIMUM over the days the label spans (text width via `label_span_days`),
  because the 2026-09-15 combined curve peaks a week before Dec-15 and ran under the text. Anchoring the default
  to that peak instead was tried and reverted the same day: it pushed desktop's label above Stretch. The prior
  (below) label keeps a fixed offset from its line end.
- Scale matched to the examples on 2026-09-09 by drawing on a 16x8 in canvas at 150 dpi (same 2400x1200 px, point-sized elements 25% smaller). Label convention: current above its line end, prior below, white backing box.

## Where new code goes

Exploratory rendering → a notebook here. Once the format is settled, the reusable renderer moves to
`src/mozaic_daily/` with a thin `scripts/` entry point and tests, and this directory keeps the spec,
examples, and design notes.
