# `data-official/2026-07/adjustments/` — July headwind spec

Holds the July **headwind (`h`)** adjustment spec.

- `headwind.json` — linear-ramp Win10-migration headwind. Desktop anchor **−1,345,000** at the Dec-15
  anchor (softened from June's −1,420,000 as Prophet learned part of the decline from fresh data;
  see `TODO_factors.md` §D3). Mobile anchor **−27,162** (unchanged). Applied as the composite-style
  post-forecast `h` applier in `src/mozaic_daily/adjustments.py`.

The other three July adjustments live in their own dirs: `l` → `../launch_on_login/`,
`o` → `../mozillaonline/`, `m` → `../marketing/`. Codes are registered in
`../../adjustment_codes.yaml`.

**Where new files go:** only headwind-type specs for this cycle. New adjustment *types* get their own
sibling dir + a code in `adjustment_codes.yaml`.
