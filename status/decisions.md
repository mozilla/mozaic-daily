# Decisions — mozaic-daily

Rows D-001 to D-009 were reconstructed on 2026-09-28 from `data-official/2026-09/_index.md` and the git log, not recorded live. Append only; never renumber.

| id | date | stage | decision | reason |
|---|---|---|---|---|
| D-001 | 2026-09-04 | Inputs | Register the mobile terms-of-use headwind as its own code `u` rather than a leg of `h` | Different source from the Win10 curve; must be sized independently |
| D-002 | 2026-09-04 | Inputs | Ship Japan bot traffic `j` at the MIDDLE scenario and India excess `i` on the PROPORTIONAL path | Planning choices; no inflection observed for either |
| D-003 | 2026-09-08 | Calibration | Set the Win10 headwind `h` anchor to −1,089,347 | Makes all-in desktop Dec-15 exactly August +479,000 |
| D-004 | 2026-09-09 | Mobile paid | `p` reads the marketing paid-DAU file as a level; retire the lift-plus-anchor round-trip | The round-trip cancelled exactly and only added a hand-copied float |
| D-005 | 2026-09-09 | Inputs | Re-export `j` at the PEAK plateau (43,813) and `i` with the data edge moved to 2026-09-06 | Fleet stopped growing; newer data |
| D-006 | 2026-09-10 | Seam | Move the seam to 2026-09-09 and re-anchor `h` to −1,167,277 | Use every landed day; keep published desktop at August +479,000 |
| D-007 | 2026-09-10 | Inputs | Ingest the existing-users Launch at Login tailwind `e` but withhold it | Reason not recorded in the repo |
| D-008 | 2026-09-15 | Leadership | Ease `h` to −1,017,277 and move `p` to the workbook's Med paid scenario | Leadership request; planning decision, not a measurement |
| D-009 | 2026-09-17 | Export | Desktop daily export uses the `exact` post-anchor rule | The file re-smooths to the published curve through Dec-31 |
| D-010 | 2026-10-06 | Mobile paid | Raise the split producer's tail-overlap tolerance to 1% for October via a new `--overlap-tolerance` flag (default unchanged) and rebuild | The 0.515% gap is attrition of old paid clients in the live tables, not SQL drift; September carried the same join at 0.35% |
| D-011 | 2026-10-06 | Inputs | Apply the existing-users Launch at Login tailwind `e` in the October desktop build (delete `withheld`) | Brendan's instruction: build with everything available as the iterative starting point |
| D-012 | 2026-10-06 | Cycle | First October build carries September's model locks, every September overlay curve, the Med paid curve and the h/t/u anchors unchanged; quick update | Fresh data first, adjustments iterated afterwards; possibly a reference build for a colleague |
| D-013 | 2026-10-06 | Repo | Declare the repository shared (`Repo mode: shared` in CLAUDE.md) | Other people now depend on it; commits on cycle branches, no PRs without an explicit ask |
| D-014 | 2026-10-06 | Effects | Build the October desktop adjustment combinatorics now (31 runs); hold the mobile `p` on/off pair | Isolate `e`'s realized effect; mobile waits on the paid-curve decision |
| D-015 | 2026-08-02 | Mobile paid (backfilled 2026-10-06) | Adopt the measured paid/organic split `p` for mobile and retire the marketing-lift overlay `m` | `m` absorbed 58% of any curve change; paid acquisition has no start date, so an anchor was an accounting choice |
| D-016 | 2026-08-03 | Mobile paid (backfilled 2026-10-06) | Add the mobile calibration tailwind `t`, +299,000 at Dec-15, as its own display-layer code | Carry the remainder of the 322,714 the m-to-p swap removed; no parameter config recovered it |
| D-017 | 2026-10-06 | Mobile paid | Re-pull the paid curve from GMIO feed `_20261002` (point estimate), wire it into `organic.json` and rerun mobile; the Med-curve build becomes the revert target | The Med curve sat 115K/day below measured paid at the October seam and made the paid 28d-MA fall for four weeks after the seam; the new curve matches the measured 28-day mean at the seam to within 200 DAU (resolves P-1) |
