# `adjustment_combinatorics/` — every subset of the September desktop adjustments, and what each one added

Two things live here, both **rebuilt 2026-09-17 at the canonical 2026-09-09 seam** (the 2026-09-04/08
build at the 2026-09-02 seam and `h` −726,000 is superseded; its runs are still in
`../adjustment_ladder/` as `<codes>.<old key>/` and leave with the September archive):

1. **The planning report** (`index.html`): every subset of the four droppable desktop overlays (`i` India
   excess, `j` Japan bot, `l` launch at login for new users, `o` MozillaOnline) scored at Dec-15 with the
   Win10 headwind `h` applied in every case, against August's 48,703,443, the all-in build and the targets,
   with the canonical desktop chart per combination. Built because not every adjustment can ship in one
   cycle; no combination was selected — September shipped all four.
2. **The adjustment-effects record** (`adjustment_*.csv`, tracked): the same runs, plus the mobile `p` on/off
   pair, reduced to what each code added at Dec-15 so the next cycle can say "the impact of X moved by Y".
   Written by `scripts/export_adjustment_effects.py`; the August twin is `../../2026-08/adjustment_combinatorics/`.

Each subset is a real desktop forecast (g01 config, 2026-09-09 seam, cached raw pull) with exactly that subset
subtracted from training rows and added back after, so the numbers include the overlays' interactions through
the Prophet fit — they are not sums of single effects. `raw` was adopted from `../desktop_raw_ci_2026-09-09/`
and the all-in `i+j+l+o` from the canonical `../desktop_g01_2026-09-09/` (sidecar-verified, same
`artifact_sha1`); the other 14 were forecast on 2026-09-17 (~2 min each). Mobile: canonical
`../mobile_cpr0725_paid0915med_2026-09-09/` (`p` on) vs `../mobile_raw_ci_2026-09-09/` (`--no-organic-split`).

## Results (Dec-15 28d-MA, display layer `h` −1,017,277 / `t` +299,000 / `u` −27,162 exact)

| platform | code | nominal (curve's own value) | single (added to raw) | marginal (removed from all-in) | Shapley | pass-through |
|---|---|---:|---:|---:|---:|---:|
| desktop | `i` | 50,994 | −55,437 | +2,915 | −8,585 | −0.17 |
| desktop | `j` | 43,813 | +33,659 | +69,125 | +15,547 | 0.36 |
| desktop | `l` | 200,000 | +96,645 | +112,497 | +61,009 | 0.31 |
| desktop | `o` | 668,839 | +15,570 | −93,229 | −44,837 | −0.07 |
| mobile | `p` | 1,804,995 | +236,603 | +236,603 | +236,603 | 0.13 |

Raw desktop 50,326,587 → all-in 50,349,720 (+23,134 net from four overlays whose curves sum to 963,646) → with
`h` **49,332,443**, the published September desktop. Raw mobile (total-DAU fit) 17,706,153 → with `p`
17,942,756 → with `t` and `u` **18,214,594**, the published September mobile.

The single and marginal views disagree in sign for `i` and `o`: alone on the raw model each lowers Dec-15
(the training-row subtraction pulls Prophet's trend down by more than the curve adds back), but removing `o`
from the all-in build costs 93K because the other three overlays are already holding the trend. Shapley is the
order-free reconciliation and is the headline column. For `p`, the "raw" run is a **total**-DAU fit that
already contains paid, so +236,603 reads as marketing's paid level minus the paid the total fit implies, not
as an absorption ratio.

**Against August** (`adjustment_effects_vs_2026-08.csv`, Shapley view): `l` −3,231 (same 200K curve, slightly
lower pass-through); `o` −63,680, of which +3,363 is the refreshed curve (567,549 → 668,839) and −67,043 is
pass-through (0.03 → −0.07); `p` +161,446, of which +12,090 is the higher paid level and +149,357 is the
model implying less paid at the new seam; `h` +297,723 by anchor (−1,315,000 → −1,017,277); `i`, `j`, `u`
added; August's mobile `h` leg (−27,162) is September's `u`.

## Files

| file | what |
|---|---|
| `combinatorics_manifest.json` | one entry per subset: overlays enabled, run parquet, plain Dec-15, `artifact_sha1`; model config, overlay fingerprints, `h` Dec-15 value; `mobile_runs` block + mobile config. `scripts/build_adjustment_combinatorics.py` |
| `index.html` | the planning report, self-contained. `scripts/render_adjustment_combinatorics.py` (**cycle-scoped**; its `h_for_plus479k` counterfactual now re-solves `h` for August +479,000 at the current seam and is informational only — `h` was set by the 2026-09-15 c-suite decision) |
| `dec15_by_combination.csv`, `counterfactuals.csv` | the report's tables as data |
| `adjustment_subsets.csv` | the fact table: Dec-15 per subset per platform, without / with the display layer, run parquet |
| `adjustment_effects.csv` | per code: nominal, single / marginal / Shapley, pass-through, fingerprint |
| `adjustment_curves_28ma.csv` | world 28d-MA per subset from the seam to 2027-12-31, without / with the display layer |
| `adjustment_dec15_by_country.csv` | per-country Dec-15 per subset, no display layer |
| `adjustment_effects.meta.json` | configs, spec sha1s, nominal sources, the canonical parquets the currency check ran against |
| `adjustment_effects_vs_2026-08.csv` | the comparison above. `scripts/compare_adjustment_effects.py --prior 2026-08 --current 2026-09` |
| `plots/` | one chart per combination, the overview, the counterfactual |
| `desktop_actuals_daily.parquet` | cached BigQuery desktop actuals for the charts (gitignored) |

## What isn't here

- No withheld codes: `e` (launch at login, existing users) is not in the subsets by decision (2026-09-17).
- No decision, and no re-measurement of any curve.
- No cross-platform ALL totals.

## Reproducing / refreshing

```bash
source .venv/bin/activate
DS=cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative
MS=cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1
DCAN=data-official/2026-09/desktop_g01_2026-09-09/$DS/mozaic_daily_forecast.2026-09-09.ld-D.adj-ijlo.parquet
MCAN=data-official/2026-09/mobile_cpr0725_paid0915med_2026-09-09/$MS/mozaic_daily_forecast.2026-09-09.gm-D.adj-p.parquet
python scripts/export_adjustment_effects.py --cycle 2026-09 --check-current --desktop-canonical $DCAN --mobile-canonical $MCAN
# exit 0: re-export (covers an h/t/u edit); exit 2: stale, show the run list and get a go-ahead, then:
python scripts/build_adjustment_combinatorics.py --cycle 2026-09 --forecast-start-date 2026-09-09 \
    --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-09 --config-from $DCAN.meta.json \
    --reuse-run raw=data-official/2026-09/desktop_raw_ci_2026-09-09/$DS/mozaic_daily_forecast.2026-09-09.ld-D.raw.parquet \
    --reuse-run i+j+l+o=$DCAN --mobile-run p=$MCAN \
    --mobile-run raw=data-official/2026-09/mobile_raw_ci_2026-09-09/$MS/mozaic_daily_forecast.2026-09-09.gm-D.raw.parquet
python scripts/export_adjustment_effects.py --cycle 2026-09 --desktop-canonical $DCAN --mobile-canonical $MCAN
python scripts/compare_adjustment_effects.py --prior 2026-08 --current 2026-09
python scripts/render_adjustment_combinatorics.py --cycle 2026-09
```

The build prompts before any model run (`--yes` only with explicit approval). A spec or curve edit changes
that overlay's fingerprint and re-runs only the subsets containing it; a seam or config change re-runs all.
