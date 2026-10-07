# `data-official/2026-10/csv/` — published October curves (DRAFT, first build 2026-10-06)

| file | written by | content |
|---|---|---|
| `october_canonical_curves.csv` | `october_canonical.ipynb` `[csv-export]` | daily 2026 rows: `{desktop,mobile,all}_{actuals,prior_september,current_october}` 28d-MA, display layer applied |
| `october_dec15_summary.csv` | same | Dec-15 per platform vs September delivered and the June targets, near-horizon min |
| `*.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv` | `scripts/export_desktop_no_headwind_csv.py` | desktop with `h` stripped (pure arithmetic on the published CSV) |
| `*.DESKTOP_ONLY.EX_IR_CN.csv`, `*.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv` | `scripts/export_desktop_ex_ir_cn_csv.py` | desktop minus Iran and China, from the parquets, `h` applied / removed |

Dec-15 2026 (28d-MA): desktop **50,189,791** (+857,348 vs September) · mobile **18,232,063** (+17,469) · ALL
**68,421,854** (+874,817). Ex-IR/CN desktop 47,162,371 (+829,139). All four small CSVs are tracked via `.gitignore`
exceptions. The daily (unsmoothed) twin from `export_desktop_daily_csv.py` has **not** been produced for October (the
script is still pinned to September by its test).
