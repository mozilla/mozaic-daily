# `research/headwinds/` — headwind-ramp profile exploration

Explores linear-ramp shapes for the `h` (headwinds) adjustment — anchor date, slope, target value, MA28 alignment math.

## Files

| File | Purpose |
|---|---|
| `headwind_options.ipynb` | Compares ramp profiles; covers the Order-1 vs. Order-2 alignment trick (apply to MA28, not daily series) |

## Production usage

The chosen ramp lives in `data-official/{YYYY-MM}/adjustments/headwind.json` and is applied via the composite-style applier registered in `src/mozaic_daily/adjustments.py`. The applier subtracts the full anchor at the anchor date, not the window-averaged ramp — see `feedback_headwind_ma28_alignment` memory.

The April-vs-June mechanism cluster (`../april-vs-june-mechanism/`) is the consumer: the headwind ramp is one of the levers explored there to close the Dec-15 MA28 gap.
