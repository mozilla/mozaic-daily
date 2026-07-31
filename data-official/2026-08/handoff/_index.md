# `data-official/2026-08/handoff/` — stakeholder handoff snapshots

Point-in-time, shareable **packaged bundles** of the August 2026 canonical forecast for
sending to stakeholders. Dated by the forecast-start (seam) date — current: **2026-07-28**.

## What's here

- **`august_canonical_handoff_2026-07-28.zip`** (~1.7 MB, 21 files) — self-contained:
  - `README.md` — the boss-facing note: headline table, the "read this before quoting
    +119,215" framing, a **hand-verification checklist** with checkpoint values, and the
    list of what's baked in.
  - `august_canonical_curves.csv` — 365 × 10, full daily 2026 curves (28d-MA DAU).
  - `august_dec15_summary.csv` — 3 rows, Dec-15 headline + summer trough per platform.
  - `CSV_REFERENCE.md` — copy of `../csv/README.md`; column reference, provenance,
    plotting recipe.
  - `plots/` — the 9 canonical charts (4 headline, 4 `*_with_2025` year-over-year
    references, 1 mobile ex-Iran).
  - `diagnostics/` — 5 optional charts explaining the curve's shape: the adjustment
    ladder (raw → overlays → headwind → published) and the seam-fix before/after.

  Large → gitignored, archived to GCS with the rest of the cycle.

- **`august_canonical_handoff/`** — the unzipped staging dir the zip was built from.
  Gitignored (`data-official/*/handoff/*_handoff/`); it duplicates tracked sources.

## What isn't here (source of truth — do NOT duplicate, regenerate instead)

The bundle is a **packaged copy**. The live, tracked sources are:

- CSVs + `CSV_REFERENCE.md` → `../csv/` (both CSVs have explicit `.gitignore` exceptions
  and are git-tracked; `CSV_REFERENCE.md` is `../csv/README.md`).
- `plots/` and `diagnostics/` PNGs → `../plots/` (see `../plots/_index.md` for which
  notebook cell produces each).
- The headline numbers, attribution ledger, and caveats → `../_index.md`.

**Per-country CSVs are not in this bundle.** June shipped 15 of them; July and August do
not. If a stakeholder asks, the June pattern is `../../2026-06/export_canonical_curves.py
--per-country`, but note that script is **frozen** and must not be edited — an August
per-country export needs its own producer.

## Rebuilding a snapshot

Everything in the bundle comes from one notebook run:

```bash
source .venv/bin/activate
# Rebuilds ../csv/*.csv and all 9 canonical PNGs in ../plots/. Needs BigQuery access.
jupyter nbconvert --to notebook --execute --inplace \
  data-official/2026-08/august_canonical_v2026-07-28.ipynb

# The 5 diagnostics/ PNGs come from two other cycle notebooks (re-run only if their
# inputs changed — they are not affected by a data refresh):
#   ladder_*.png              <- ../desktop_adjustment_ladder.ipynb
#   seam_fix_before_after_ma.png <- ../seam_fix_before_after.ipynb
```

Then stage into `august_canonical_handoff/` and re-zip, dated by the seam date:

```bash
cd data-official/2026-08/handoff
zip -r -q august_canonical_handoff_2026-07-28.zip august_canonical_handoff -x "*.DS_Store"
```

## Status caveat

This bundle reflects the build as of 2026-07-30. The cycle is **not finalized**: the `o`
(MozillaOnline) and `m` (marketing lift) overlays are still ~4–5 week-stale carry-forwards
from July, and the Win10 headwind anchor has been attenuated four cycles running without
data-side validation. The bundle's `README.md` states both prominently. Re-cut the zip when
those land.
