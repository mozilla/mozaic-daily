# `adjustments/` — combinatorics CANDIDATES for the 2027 Glean-history draft (none applied)

Registered 2026-10-12 per Brendan's decision 8 ("register the excluded terms as candidates, after the comparisons"
— moved up because the lock closed the build). Both are display-layer `step` specs **marked `candidate_only: true`**
and are **not** summed onto any published curve: `REPORT.html` lists them in a "candidates, not applied" table and
the headline stays `glean_equivalent`.

| file | code | what | size at 2027-12-15 (build 5) |
|---|---|---|---|
| `duplicate_id_inflation.json` | `d` | pre-Fx-148 duplicate Glean ids still inflating published MAU (0.709% Windows, 0.253% Linux of published MAU, frozen at 2026-09-08) | MAU +950,000 |
| `linux_glean_only_temporary.json` | `x` | Linux Glean-only surges (bot-like) that published Glean counts and the headline excludes (1.92% of Linux DAU, 8.3% of Linux MAU in Sep 2026) | DAU +76,000; MAU +1,375,000 |

Both carry a `desktop_mau` field. The display-layer renderer handles `desktop_dau` / `mobile_dau` only today; wiring MAU into
`render_adjustment` / `apply_net_adjustment_to_series` is the first task of the combinatorics step for this draft. Sizes are
pinned to build 5 and must be re-derived if the build changes (the `sizing.basis` field says how).
