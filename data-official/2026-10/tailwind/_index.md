# `t` — mobile calibration tailwind, cycle 2026-09 (carried forward)

Rationale record for September. The spec is `../adjustments/tailwind.json` (display layer, live by presence).

**Carried forward unchanged on 2026-09-04**: +299,000 mobile DAU at 2026-12-15, ramped from 0 at this cycle's seam
(2026-09-02). Nothing about it was re-measured or re-sized; only the ramp start moved with the seam, as every
display-layer ramp does each cycle. Desktop 0. Unclamped past Dec-15, exactly as August published it (the Win10
headwind `h` became clamped this cycle; `t` was deliberately left a pure carry-forward).

**What it is, and is not.** It is the August *calibration* tailwind: the remainder of the 322,714 that the
2026-07-31 `m` → `p` swap removed from mobile Dec-15, carried as an explicit overlay after a 33-probe parameter
search found no exposed non-holiday configuration recovers it. As adopted: ~47% (+141,637) the measured excess of
an independent mobile implementation, ~45% a planning decision, ~8% calibration so August landed within 1,000 DAU
of July's published mobile figure. **It is not the terms-of-use headwind** — that is the −27,162 mobile leg that
lived in `headwind.json` through August and is now its own code, `u`. Full evidence split and adoption record:
`../../2026-08/tailwind/_index.md`.

| | |
|---|---|
| spec | `../adjustments/tailwind.json` |
| Dec-15 mobile effect | +299,000 (exact; display layer) |
| change vs August | 0 |
| net September mobile display layer at Dec-15 | +299,000 (`t`) − 27,162 (`u`) = **+271,838**, the same net August carried |

## Where new files go

A re-sized anchor is an edit to `mobile_dau` in the spec plus a dated line here recording the old value and the
reason. If the September `p` rebuild (new paid curve, +331,525 at Dec-15) turns out to close the gap this
tailwind was covering, the right move is to reduce or retire it here with that evidence, not to fold the change
into `u` or `h`.
