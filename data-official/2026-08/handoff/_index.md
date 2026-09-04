# `data-official/2026-08/handoff/` — stakeholder handoff snapshots

Point-in-time, shareable **packaged bundles** of the August 2026 canonical forecast for
sending to stakeholders. Dated by the forecast-start (seam) date — current: **2026-07-28**.

## What's here

- **`august_canonical_handoff_2026-07-28.zip`** (~1.7 MB, 21 files) — self-contained.
  **⛔ Stale as of 2026-08-03 — see the status section below before using.**
  - `README.md` — the boss-facing note: headline table, a "read this before quoting the
    ALL delta" framing, a **hand-verification checklist** with checkpoint values, and the
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

## ⛔ STATUS: THIS BUNDLE IS STALE — DO NOT SEND IT

**The on-disk zip and staging dir reflect the build as of 2026-07-30 and were never re-cut
after the 2026-08-03 mobile changes.** Every headline number in them is superseded:

| series | in the bundle | current canonical | drift |
|---|--:|--:|--:|
| Desktop Dec-15 | 48,703,960 | **48,703,443** | −517 |
| Mobile Dec-15 | 17,924,607 | **17,924,562** | −45 |
| ALL Dec-15 | 66,628,567 | **66,628,005** | −562 |
| ALL vs July | +119,215 | **+118,653** | −562 |
| Desktop trough | 45,223,249 (08-25) | **45,220,838** (08-24) | −2,411 |
| Mobile trough | 17,015,132 (08-16) | **17,063,631** (08-16) | +48,499 |
| seam | 2026-07-28 | **2026-08-02** | +5 days |

Five things landed after it was built: the **g01 desktop retune** (2026-07-30, −6,357 at Dec-15), the
**mobile re-lock** to `cpr 0.725` (+23,907), the **`t` mobile tailwind** (+299,000), a **full data
refresh to the 2026-08-02 seam** (desktop +100,840, mobile +500), and the **ramp start moving with the
seam**, and the **headwind anchor moved to −1,295,000** (2026-08-03, −75,000) and then **−1,315,000**
(2026-08-04, a further −19,000 and −1,000).
⚠️ **The bundle's headline figures now sit within a few hundred DAU of the current ones** — desktop
within 517, ALL within 562, mobile within 45 — purely by coincidence, after three headwind reversals
almost exactly cancelled the data refresh. **This makes the stale bundle far more dangerous, not less:** it
looks current and is not. Its seam, model config, mobile methodology, paid split and both display
specs all differ. Do not read those small drifts as the
bundle being current; the seam, the model, the split and two specs all differ. The bundle's framing is also
substantively wrong now, not just numerically: it describes mobile as "essentially unchanged
(+738)" when mobile in fact underwent a methodology swap, a re-lock and a large discretionary
overlay that nearly cancel, and it says nothing about `t` at all.

Its `CSV_REFERENCE.md` is likewise an outdated copy of `../csv/README.md`, which has since been
corrected.

**Re-cut before any stakeholder contact**, per "Rebuilding a snapshot" above. The bundle is a
gitignored build artifact, so it was deliberately *not* hand-edited during the 2026-08-03
documentation pass — a bundle should only ever be a faithful package of one notebook run, never
a hand-patched one.

The cycle is also still **not finalized** on its own terms: `o` (MozillaOnline) remains a ~4–5
week-stale carry-forward from July (`m` is retired, superseded by `p`), the Win10 headwind anchor
has now been attenuated **five** times running and its validation
(`research/headwinds/WIN10_ANCHOR_FINDINGS.md`) found the elapsed portion contradicted, and mobile
carries the discretionary `t` overlay. Re-cut the zip when the `o` swap lands, not before.
