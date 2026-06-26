# `data-official/2026-07/mozillaonline/` — MozillaOnline migration overlay

Inputs for the `o` adjustment (planned): the MozillaOnline → Firefox desktop
migration modeled as a bidirectional overlay (subtract from training before
mozaic, add back after), same pattern as marketing-lift `m`. >90% China; ~10%
tail (likely VPN) across other countries.

## What's here

- **`PLACEHOLDER_MODEL_HANDOFF.md`** — instructions for a fresh Claude to build a
  conservative **placeholder** migration model (daily migration-DAU series parquet
  + `mozillaonline.json` spec + meta + plot + generator script), designed so Brad's
  official model drops in as a clean swap. **Start here** until the official model arrives.

## What lands here once the placeholder/official model is built

`mozillaonline_migration_model.*.parquet` (+ `.meta.json`), `mozillaonline.json`,
`README.md`, `placeholder_curve.png`, `build_placeholder_model.py`.
