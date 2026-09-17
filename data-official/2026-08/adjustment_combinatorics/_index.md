# `adjustment_combinatorics/` — what each August adjustment added at Dec-15 (built retroactively)

The August 2026 cycle's **adjustment-effects record**: every subset of the model-run adjustments
forecast at August's locked configs and seam, reduced to tracked CSVs so a later cycle can say
"the impact of X moved by Y since August". Built on **2026-09-17**, six weeks after the cycle
closed, with the tooling introduced that day (`scripts/build_adjustment_combinatorics.py`
`--reuse-run` / `--mobile-run`, `scripts/export_adjustment_effects.py`). August's canonical
parquets were **not touched**; new isolation runs were added beside them.

**Comparability check.** The canonical builds date from 2026-08-03. Before trusting fresh runs
against them, the all-in desktop (`l+o`) and mobile (`p`) builds were re-run once with the
2026-09-17 code (`../adjustment_reproduction_2026-09-17/`): both reproduce the canonical parquets
**exactly, zero difference on every row** (198,696 desktop rows, 191,019 mobile rows). The fresh
`raw`, `l`, `o` and mobile `raw` runs are therefore on the same footing as the adopted canonical.

## Setup

| | desktop | mobile |
|---|---|---|
| seam | 2026-08-02 | 2026-08-02 |
| config | g01 (`cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825, multiplicative`) | `cps 0.035, cpr 0.725, ncp 25, recent 13, sps 0.1, auto, holiday −0.055` |
| model-run codes | `l` (200K ceiling), `o` (July's curve, Dec-15 567,549) | `p` (paid level, Dec-15 28d-MA 1,554,879) |
| display layer | `h` −1,315,000 | `t` +299,000, `h` mobile leg −27,162 (the leg became code `u` in September) |
| all-in run | canonical `../desktop_g01_2026-08-02/…adj-lo.parquet` (adopted) | canonical `../mobile_cpr0725_2026-08-02/…adj-p.parquet` |
| other runs | `raw`, `l`, `o` in `../adjustment_ladder/<codes>.<key>/` (fresh) | `raw` = `../mobile_raw_noorganic_2026-08-02/` (`--no-organic-split`, fresh) |

## Results (Dec-15 28d-MA)

| platform | code | nominal (curve's own value) | single (added to raw) | marginal (removed from all-in) | Shapley | pass-through |
|---|---|---:|---:|---:|---:|---:|
| desktop | `l` | 200,000 | +59,733 | +68,748 | +64,241 | 0.32 |
| desktop | `o` | 567,549 | +14,335 | +23,351 | +18,843 | 0.03 |
| desktop | `h` | −1,315,000 | exact | exact | exact | 1 |
| mobile | `p` | 1,554,879 | +75,157 | +75,157 | +75,157 | 0.05 |
| mobile | `t` | +299,000 | exact | exact | exact | 1 |
| mobile | `h` (mobile leg) | −27,162 | exact | exact | exact | 1 |

Raw desktop 49,935,359 → all-in 50,018,443 (+83,084 from `l`+`o`, interaction +9,015) → with `h`
**48,703,443**, the published August desktop. Raw mobile (total-DAU fit) 17,577,567 → with `p`
17,652,724 → with `t` and the headwind leg **17,924,562**, the published August mobile.

Reading `o`: the curve adds 567,549 at Dec-15 but the training-row subtraction lowers Prophet's
trend by almost as much, so wiring it moved the headline only +14K to +23K. Reading `p`: its
"raw" run is a **total**-DAU fit that already contains paid, so +75K is marketing's paid level
minus the paid the total fit implies, not an absorption ratio.

## Files

| file | what |
|---|---|
| `combinatorics_manifest.json` | the runs: parquet paths, plain Dec-15, `artifact_sha1`, overlay fingerprints, `mobile_runs` block |
| `adjustment_subsets.csv` | the fact table above: Dec-15 per subset per platform, without / with the display layer |
| `adjustment_effects.csv` | the per-code table above (single / marginal / Shapley, nominal, pass-through, fingerprint) |
| `adjustment_curves_28ma.csv` | world 28d-MA per subset from the seam to 2027-12-31, without / with the display layer |
| `adjustment_dec15_by_country.csv` | per-country Dec-15 per subset, no display layer |
| `adjustment_effects.meta.json` | configs, spec sha1s, nominal sources, the canonical parquets the check ran against |

Not here: an HTML report (September's renderer is cycle-scoped to 2026-09 and August never had
one), and no decision — August shipped `l`, `o`, `p`, `h`, `t` as published.

## Reproducing

```bash
source .venv/bin/activate
python scripts/export_adjustment_effects.py --cycle 2026-08 --check-current \
    --desktop-canonical data-official/2026-08/desktop_g01_2026-08-02/<slug>/mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet \
    --mobile-canonical  data-official/2026-08/mobile_cpr0725_2026-08-02/<slug>/mozaic_daily_forecast.2026-08-02.gm-D.adj-p.parquet
```

The runs are gitignored (parquet + pickle under `../adjustment_ladder/`); the CSVs and JSON are
tracked. Launch script used: `tmp/effects_august_chain.sh` (throwaway; the commands are in
`../_index.md`).
