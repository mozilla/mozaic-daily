# `data-official/2026-09/adjustments/` — display-layer adjustment specs (`h`, `t`, `u`)

## This directory is LIVE BY PRESENCE

`load_adjustments()` does `glob("*.json")` and **sums every spec it finds**. There is no date gate and no
enable flag at the display layer, so adding or removing a file here changes the published numbers
immediately, with no code change to review. All three ramps run from the seam **2026-09-09** to the
**2026-12-15** anchor (98 days); Dec-15 is exactly the anchor, no model rerun.

| spec | code | desktop @ Dec-15 | mobile @ Dec-15 | rationale |
|---|---|--:|--:|---|
| `headwind.json` | `h` | **−1,017,277**, `clamp_at_anchor` (flat after Dec-15) | 0 | `../headwinds/` |
| `tailwind.json` | `t` | 0 | **+299,000** | `../tailwind/` |
| `tou_mobile_headwind.json` | `u` | 0 | **−27,162** | `../tou_mobile_headwind/` |
| **net** | | **−1,017,277** | **+271,838** | |

Changes this cycle, newest first (every one a planning decision recorded in the spec's `notes`):

- **`h` 2026-09-15: −1,167,277 → −1,017,277** (+150,000) at the c-suite's request, so published desktop
  Dec-15 lands at 49,332,443 = August +629,000. Recorded as an `h` edit, not a separate code, on
  Brendan's instruction.
- **`h` 2026-09-10: −1,089,347 → −1,167,277** with the seam refresh: the +77,930 the new seam's raw model
  added at Dec-15 was taken back out so all-in desktop stayed exactly August +479,000.
- **`h` 2026-09-08: −726,000 → −1,089,347**, the combinatorics `h_for_plus479k` counterfactual.
- **`h` 2026-09-04: −1,315,000 → −726,000**, the Dec-15 value of Brad's Win10 model curve (file + sha1 in
  `../headwinds/`), with `clamp_at_anchor: true` so it is flat after Dec-15 like his file. Applying the
  curve's *shape* shifted to the seam was tried and reverted the same day (lost 166,711 of headwind).
- **`u` split out of `headwind.json` 2026-09-04**: the mobile −27,162 leg is a different source
  (terms-of-use risk) from the Win10 curve and is sized independently; `headwind.json` now carries
  `mobile_dau: 0`.
- **`t` carried forward unchanged** from August (+299,000; ramp start moved with the seam).

**Present vs Archived.** Everything here is tracked JSON and stays on disk. The October cycle received
copies of all three specs at the 2026-10-05 roll-forward, marked PROVISIONAL in `notes` until re-gated.
