# `data-official/2026-09/adjustment_ladder/` — cached desktop isolation runs (ladder + combinatorics)

Every directory `<codes>.<key>/` is one real desktop forecast (g01 config) with exactly the overlays named
in `<codes>` applied, cached by `scripts/build_adjustment_ladder.py` and reused by
`scripts/build_adjustment_combinatorics.py`. `<key>` fingerprints seam + config + the specs and curves of
*only* the overlays in that run, so editing one curve re-runs only the rungs containing its code.
`ladder_manifest.json` is what the canonical notebook's `[plot-desktop-ladder]` reads; the combinatorics
manifest lives in `../adjustment_combinatorics/`.

**Two seams are present, 35 runs in all:**

| seam | runs | what they are |
|---|--:|---|
| **2026-09-09** (live) | 16 | the full 2^4 subset lattice of `i j l o` rebuilt 2026-09-17 with Brendan's approval. 14 were forecast (~2 min each); `raw.0e49d0455cdadb38/` and `i+j+l+o.f482e79f98674fd6/` were adopted with `--reuse-run` from `../desktop_raw_ci_2026-09-09/` and the canonical build, so they hold the parquet + sidecar only (no pickle). The all-in rung reproduces the published 49,332,443 exactly. |
| 2026-09-02 (stale) | 19 | the 2026-09-04 ladder (7 runs) and the 2026-09-08 combinatorics at the first seam, with three pairs duplicated by spec and curve edits between the 09-04 ladder and the 09-08 combinatorics (`l` ×2, `j+l+o` ×2, `i+j+l+o` ×2). Exhaust: nothing reads them, kept because the branch record cites their Dec-15 numbers. |

Ladder at the live seam, Dec-15 28d-MA: raw 50,326,587 → `h` −1,017,277 → `l` +96,645 → `i` −25,079 →
`j` +44,797 → `o` −93,229 = **49,332,443**. The per-code single / marginal / Shapley effects derived from
these runs are the tracked `../adjustment_combinatorics/adjustment_effects.csv`.

**Rebuilding needs explicit approval** — both scripts prompt before every model run; `--yes` only after
the user has approved that specific rebuild.

**Present vs Archived.** Each run's forecast parquet + `.meta.json` + `parameters.json` + `run.log` stay on
disk (35 runs, ~50 MB). The 33 pickles (~18 GB, 558–566 MB each) are archived to
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/adjustment_ladder/`
(verified 2026-09-17 and at the 2026-10-05 button-down) and removed from disk at the October roll-forward.
Every run's `mozaic_parts.raw.legacy.desktop.DAU.parquet` is a symlink to `../desktop_rawpull_<seam>/`.
