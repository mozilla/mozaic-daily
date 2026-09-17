# `adjustment_reproduction_2026-09-17/` — do today's code and August's canonical builds agree?

One desktop (`l+o`, g01) and one mobile (`p`, cpr 0.725) forecast at the 2026-08-02 seam, made on
2026-09-17 with the September-branch code, to check that fresh isolation runs for the retroactive
adjustment-effects record (`../adjustment_combinatorics/`) are comparable with the 2026-08-03
canonical parquets that were adopted as the all-in runs.

**Result: exact.** Desktop 198,696 rows and mobile 191,019 rows, maximum absolute DAU difference 0.000;
Dec-15 28d-MA 50,018,442.9 (desktop) and 17,652,723.9 (mobile) on both sides.

Diagnostics only — nothing here is canonical or adopted anywhere. `desktop/` and `mobile/` hold
the standard `<slug>/` layout (parquet + sidecar + `parameters.json` + pickle; pickle and parquets
gitignored). Delete after the September button-down archives them, or keep as the precedent for
the check.
