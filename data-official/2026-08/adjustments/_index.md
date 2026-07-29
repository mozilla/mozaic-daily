# `data-official/2026-08/adjustments/` — display-layer adjustment specs (`h`)

`headwind.json` — the Windows 10 migration headwind. A `linear_ramp` from 2026-04-01 reaching
**desktop −1,345,000 / mobile −27,162** at the 2026-12-15 anchor.

**Carried forward from July verbatim.** Every field is byte-identical to
`../../2026-07/adjustments/headwind.json`.

Unlike `l`/`o`/`m`, this is a **display-layer** adjustment: it is *not* baked into the forecast
parquets. It is applied to the 28-day MA in the canonical notebook (`[compute-series]`) via
`load_adjustments` + `apply_net_adjustment`. The forecast parquets in `../desktop_baseline_*` and
`../mobile_baseline_*` are **pre-headwind**.

The spec has no `applies_to_forecast_start` key — it is picked up by directory, so anything reading
`ADJUSTMENTS_DIR` gets it regardless of anchor date. That also means an empty directory would silently
yield a pre-headwind number, so `load_adjustments` raises on an empty glob rather than returning zeros.

**Open for this cycle:** the anchor was deliberately not revisited for the baseline. July softened it
from −1,420,000 → −1,345,000 on the reasoning that Prophet had partly learned the Win10 decline from
data available then; with five more weeks of actuals it plausibly should attenuate further. See the
parent `_index.md` "Next up".

**Where new files go:** additional display-layer adjustment specs for this cycle. They are summed, so
each file must describe a distinct effect — do not add a second file that restates the same headwind.
