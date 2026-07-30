# `_archive/` — code frozen against past forecast cycles

Everything here **imports the seam-MA implementation from a frozen cycle directory**
(`data-official/2026-06/export_canonical_curves.py`) rather than from the package
(`src/mozaic_daily/seam_ma.py`).

## Why this directory exists

We do not modify past forecasts, even where they are known to be wrong. The seam-MA logic used to
live under `data-official/2026-06/`, and the June, July and August cycles all imported it from
there. On **2026-07-29** a defect was fixed in the trend estimator (`research/ma-seam-turbulence/`
Fix A: the deseasonalizing window at the seam was day-of-week unbalanced, inflating the first
forecast day by up to ~10% of the weekday/weekend swing and stepping the published August desktop
curve **+102,595** at the seam).

Fixing it in place would have silently changed June's and July's delivered curves. Instead the
fixed implementation went into `src/mozaic_daily/seam_ma.py`, the 2026-06 file was left byte-for-byte
untouched, and everything still bound to that file was moved here.

**The freeze is therefore structural, not a promise.** Past cycles' curves cannot move, because the
code they call cannot change.

## What is here

| path | why frozen |
|---|---|
| `research/param-scans/desktop_gradient_round{1,2,3,4}.ipynb` | June/July-era desktop parameter gradients; their curves are only meaningful against the estimator in force when they were run |
| `research/param-scans/mobile-july/seam_smoothing.ipynb` | the July mobile seam-kink work (`display_ma` `continuous_splice` / `slope_match` tuning). Its conclusions are specific to the pre-fix estimator |
| `research/param-scans/mobile-july/mobile_july_sensitivity.ipynb` | July mobile round-1 sensitivity. Bound the frozen copy via a path list (`REPO / "data-official/2026-06"`), which a narrower grep missed on the first pass; also imports the archived `mobile_sensitivity.py` |
| `research/param-scans/aug22-retune/{desktop_bestfit_vs_july.ipynb,export_bestfit_curve.py,make_notebook.py}` | the Aug-22 retune exporter and report; scores are pre-fix |
| `scripts/mobile_sensitivity.py` + `tests/test_mobile_sensitivity.py` | July-cycle tooling by construction — hardcodes `FORECAST_START = 2026-06-29` and June's delivered baseline constant, so it is not a general-purpose tool to repoint |
| `tests/test_export_canonical_curves.py` | tested the frozen 2026-06 file. Nothing in that file can regress, so this is documentation of frozen behaviour; superseded by `tests/test_seam_ma.py` |

## What is NOT here, and why

- **`data-official/2026-06/` and `data-official/2026-07/`** — the cycle directories *are* the
  archive. A frozen cycle importing its own frozen exporter is self-consistent; those notebooks
  were left exactly as delivered.
- **`research/param-scans/param_scan_exploration.ipynb`** — never imported the exporter, so the fix
  does not reach it. (`mobile_july_sensitivity.ipynb` was initially judged the same way and is in fact
  archived above — it binds the frozen copy through a path list rather than a literal string.)
- **`research/ma-seam-turbulence/`** — the diagnosis harness for this very defect. Several of its
  scripts load the frozen 2026-06 implementation *deliberately*, as the "before" reference for
  before/after comparison. It stays put.

## Running anything in here

`_archive` is in `norecursedirs` in `pyproject.toml`, so the live `pytest` run does not collect
the two test modules — they exercise frozen code, and reporting on code that is not allowed to
change would be noise. Run them explicitly to confirm frozen behaviour still holds:

```bash
pytest _archive/tests/ -q      # 12 passed as of 2026-07-29
```

**Two files carry an on-archiving edit, and only one kind.** `scripts/mobile_sensitivity.py` and
`tests/test_mobile_sensitivity.py` each derived the git root from `__file__` by counting parent
directories, which the move invalidated. Their root resolution was corrected and nothing else;
both are commented at the change site. No behaviour was altered — the 12 tests above pass against
the same frozen implementation as before the move.

## Where new code goes

**Nowhere.** Do not add to this directory and do not "update" what is in it — an edit here defeats
the point. New display/seam work goes in `src/mozaic_daily/seam_ma.py` with tests in
`tests/test_seam_ma.py`. If something here is still genuinely useful, copy the parts you need into
fresh code that imports the package, and leave the original untouched.

Related: `src/mozaic_daily/_index.md`, `research/ma-seam-turbulence/_index.md` (Fix A),
`data-official/_index.md`.
